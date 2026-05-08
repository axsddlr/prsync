#!/usr/bin/env python3

import os
import sys
import argparse
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Set, Tuple, Optional
from dataclasses import dataclass
import time
import logging
from queue import Queue
from threading import Lock
import re
import atexit
import shlex
import shutil
import tempfile
import traceback
import traceback

SSH_TIMEOUT = 300  # 5 minutes for SSH commands
RSYNC_TIMEOUT = 3600  # 1 hour for rsync transfers
RETRY_BACKOFF = [2, 4, 8]  # seconds between retries


@dataclass
class RemoteTarget:
    user: Optional[str]
    host: str
    path: str
    control_path: Optional[str] = None

    @classmethod
    def parse(cls, target: str) -> Optional["RemoteTarget"]:
        """Parse a target string like 'user@host:/path' or 'host:/path'"""
        match = re.match(r"^(?:([^@]+)@)?([^:]+):(.+)$", target)
        if match:
            user, host, path = match.groups()
            return cls(user=user, host=host, path=path)
        return None

    def __str__(self) -> str:
        """Convert back to string format"""
        if self.user:
            return f"{self.user}@{self.host}:{self.path}"
        return f"{self.host}:{self.path}"

    def ssh_base_args(self) -> List[str]:
        """Return base SSH arguments including user and control path if available"""
        args = ["ssh"]
        if self.control_path:
            args.extend(["-o", f"ControlPath={self.control_path}"])
        if self.user:
            args.extend(["-l", self.user])
        return args

    def setup_ssh_multiplexing(self):
        """Setup SSH connection multiplexing"""
        temp_dir = tempfile.mkdtemp(prefix="rsync_ssh_")
        self.control_path = os.path.join(temp_dir, "control_%h_%p_%r")

        ssh_cmd = self.ssh_base_args()
        ssh_cmd.extend(
            [
                "-nNf",
                "-o",
                "ControlMaster=yes",
                "-o",
                f"ControlPath={self.control_path}",
                "-o",
                "ControlPersist=yes",
                self.host,
            ]
        )

        try:
            subprocess.run(ssh_cmd, check=True, timeout=SSH_TIMEOUT)
            return True
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logging.error(f"Failed to setup SSH multiplexing: {e}")
            self.control_path = None
            shutil.rmtree(temp_dir, ignore_errors=True)
            return False

    def cleanup_ssh_multiplexing(self):
        """Cleanup SSH multiplexing connection"""
        if self.control_path:
            ssh_cmd = self.ssh_base_args()
            ssh_cmd.extend(
                ["-O", "exit", self.host]
            )

            try:
                subprocess.run(ssh_cmd, check=True, timeout=SSH_TIMEOUT)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                pass

            if os.path.exists(os.path.dirname(self.control_path)):
                try:
                    shutil.rmtree(os.path.dirname(self.control_path))
                except OSError:
                    pass


@dataclass
class RsyncJob:
    source_files: List[Path]
    source_base: Path
    target: str
    rsync_args: List[str]
    job_id: int
    control_path: Optional[str] = None


class ParallelRsync:
    def __init__(
        self,
        source_dir: str,
        target: str,
        parallel_jobs: int = 4,
        bucket_size_mb: int = 1000,
        rsync_args: Optional[List[str]] = None,
        retry_count: int = 3,
    ):
        self.source = source_dir
        self.target = target
        self.remote_source = RemoteTarget.parse(source_dir)
        self.remote_target = RemoteTarget.parse(target)
        self.is_remote_source = bool(self.remote_source)
        self.is_remote_target = bool(self.remote_target)

        if self.is_remote_source and self.is_remote_target:
            raise ValueError(
                "Remote-to-remote transfers are not supported. "
                "Source and target cannot both be remote hosts."
            )
        self.parallel_jobs = parallel_jobs
        self.bucket_size_mb = bucket_size_mb
        self.rsync_args = rsync_args if rsync_args else []
        self.retry_count = retry_count

        # Setup SSH multiplexing for remote source
        if self.is_remote_source:
            if not self.remote_source.setup_ssh_multiplexing():
                logging.warning(
                    "SSH multiplexing setup failed for source, "
                    "falling back to non-multiplexed SSH"
                )
            else:
                atexit.register(self.remote_source.cleanup_ssh_multiplexing)

        # Setup SSH multiplexing for remote target
        if self.is_remote_target:
            if not self.remote_target.setup_ssh_multiplexing():
                logging.warning(
                    "SSH multiplexing setup failed for target, "
                    "falling back to non-multiplexed SSH"
                )
            else:
                atexit.register(self.remote_target.cleanup_ssh_multiplexing)

        self.current_bucket: List[Tuple[Path, int]] = []
        self.current_bucket_size = 0
        self.buckets: List[List[Path]] = []

        self.logger = logging.getLogger(__name__)
        self.progress_lock = Lock()
        self.total_files = 0
        self.completed_files = 0
        self.failed_transfers = Queue()
        self._remote_target_manifest: Set[str] = set()
        self._active_processes: List[subprocess.Popen] = []
        atexit.register(self._terminate_all)

    def _get_remote_file_list(self, remote: RemoteTarget) -> List[Tuple[str, int]]:
        """Get list of files and sizes from remote host"""
        ssh_cmd = remote.ssh_base_args()
        
        # Use find to get file paths and sizes
        find_cmd = f"find {shlex.quote(remote.path)} -type f -printf '%s %P\\n'"
        ssh_cmd.extend([remote.host, find_cmd])
        
        try:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True,
                                     check=True, timeout=SSH_TIMEOUT)
            files = []
            for line in result.stdout.splitlines():
                size, path = line.strip().split(" ", 1)
                files.append((path, int(size)))
            return files
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            stderr_hint = getattr(e, 'stderr', '')
            if stderr_hint and 'invalid predicate' in stderr_hint:
                self.logger.error(
                    "Failed to get remote file list: remote host does not support "
                    "GNU find (required for -printf). Use a GNU/Linux remote host."
                )
            else:
                self.logger.error(f"Failed to get remote file list: {e}")
            raise

    def scan_and_distribute(self):
        """Scan directory and distribute files into buckets based on size"""
        if self.is_remote_source:
            self.logger.info(f"Scanning remote directory: {self.remote_source}")
            files = self._get_remote_file_list(self.remote_source)
            source_base = self.remote_source.path
        else:
            self.logger.info(f"Scanning local directory: {self.source}")
            source_base = Path(self.source).resolve()
            if not source_base.exists():
                raise ValueError(f"Source directory does not exist: {source_base}")
            
            files = []
            for root, _, filenames in os.walk(source_base, onerror=lambda err: self.logger.error(
                f"Error walking directory: {err}"
            )):
                for filename in filenames:
                    filepath = Path(root) / filename
                    try:
                        files.append((str(filepath.relative_to(source_base)), filepath.stat().st_size))
                    except OSError as e:
                        self.logger.error(f"Error accessing file {filepath}: {e}")

        # Sort files by size descending for better bucket packing
        files.sort(key=lambda f: f[1], reverse=True)

        # Sort files by size descending so large files claim their own bucket first
        files.sort(key=lambda f: f[1], reverse=True)

        for filepath, file_size in files:
            self.total_files += 1
            # Files larger than bucket size get their own bucket
            if file_size >= (self.bucket_size_mb * 1024 * 1024):
                if self.current_bucket:
                    self.buckets.append([path for path, _ in self.current_bucket])
                    self.current_bucket = []
                    self.current_bucket_size = 0
                self.buckets.append([filepath])
                continue
            self.current_bucket.append((filepath, file_size))
            self.current_bucket_size += file_size

            if self.current_bucket_size >= (self.bucket_size_mb * 1024 * 1024):
                self.buckets.append([path for path, _ in self.current_bucket])
                self.current_bucket = []
                self.current_bucket_size = 0

        if self.current_bucket:
            self.buckets.append([path for path, _ in self.current_bucket])

        self.logger.info(
            f"Found {self.total_files} files in {len(self.buckets)} buckets"
        )

    def _build_remote_target_manifest(self):
        """Build a set of all file paths on the remote target for O(1) existence checks"""
        base = self.remote_target.path.rstrip('/')
        ssh_cmd = self.remote_target.ssh_base_args()
        find_cmd = f"find {shlex.quote(base)} -type f"
        ssh_cmd.extend([self.remote_target.host, find_cmd])

        try:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True,
                                     check=True, timeout=SSH_TIMEOUT)
            self._remote_target_manifest = set(result.stdout.splitlines())
            self.logger.info(
                f"Remote target manifest: {len(self._remote_target_manifest)} files"
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            self.logger.error(f"Failed to build remote target manifest: {e}")
            raise

    def _terminate_all(self):
        """Terminate all active rsync subprocesses"""
        procs = list(self._active_processes)
        for proc in procs:
            try:
                proc.terminate()
            except OSError:
                pass
        for proc in procs:
            try:
                proc.wait(timeout=5)
            except (subprocess.TimeoutExpired, OSError):
                try:
                    proc.kill()
                except OSError:
                    pass
        self._active_processes.clear()

    def execute_rsync(self, job: RsyncJob) -> bool:
        """Execute rsync for a given bucket of files"""
        files_to_sync = []
        
        # Check each file if it needs to be synced
        for source_file in job.source_files:
            relative_path = str(source_file)
            should_sync = True
            if self.is_remote_target:
                base = self.remote_target.path.rstrip('/')
                target_path = f"{base}/{relative_path}"
                should_sync = target_path not in self._remote_target_manifest
            else:
                target_file = Path(self.target) / relative_path
                should_sync = not target_file.exists()
            
            if should_sync:
                files_to_sync.append(relative_path)
            else:
                with self.progress_lock:
                    self.completed_files += 1
                    progress = (self.completed_files / self.total_files) * 100
                    self.logger.info(
                        f"Skipping existing file: {relative_path} - Progress: {progress:.1f}% ({self.completed_files}/{self.total_files})"
                    )
        
        if not files_to_sync:
            return True

        last_error: Optional[str] = None
        for attempt in range(self.retry_count):
            if attempt > 0:
                delay = RETRY_BACKOFF[min(attempt - 1, len(RETRY_BACKOFF) - 1)]
                self.logger.warning(
                    f"Retrying job {job.job_id} in {delay}s (attempt {attempt + 1}/{self.retry_count})"
                )
                time.sleep(delay)

            tmpfile = tempfile.NamedTemporaryFile(
                mode="w", prefix="rsync_filelist_", suffix=f"_{job.job_id}",
                delete=False
            )
            bucket_file_list = tmpfile.name

            try:
                tmpfile.write("\0".join(files_to_sync))
                tmpfile.close()

                cmd = ["rsync"] + job.rsync_args

                if self.is_remote_source and self.remote_source.control_path:
                    rsh = " ".join(self.remote_source.ssh_base_args())
                    cmd.extend(["--rsh", rsh])

                if self.is_remote_target and self.remote_target.control_path:
                    rsh = " ".join(self.remote_target.ssh_base_args())
                    cmd.extend(["--rsh", rsh])

                source_path = (
                    f"{str(self.remote_source)}/"
                    if self.is_remote_source
                    else f"{str(job.source_base)}/"
                )

                cmd.extend(["--files-from=" + bucket_file_list, "--from0", source_path, job.target])

                self.logger.debug(f"Executing rsync command: {' '.join(cmd)}")

                process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=None)
                self._active_processes.append(process)
                try:
                    process.wait(timeout=RSYNC_TIMEOUT)
                except subprocess.TimeoutExpired:
                    last_error = "timeout"
                    self.logger.error(f"Rsync timed out for job {job.job_id}")
                    process.kill()
                    process.wait()
                    continue
                finally:
                    self._active_processes.remove(process)

                if process.returncode != 0:
                    last_error = f"exit code {process.returncode}"
                    self.logger.error(f"Rsync failed for job {job.job_id} ({last_error})")
                    continue

                with self.progress_lock:
                    self.completed_files += len(files_to_sync)
                    progress = (self.completed_files / self.total_files) * 100
                    self.logger.info(
                        f"Progress: {progress:.1f}% ({self.completed_files}/{self.total_files})"
                    )

                return True

            finally:
                try:
                    os.unlink(bucket_file_list)
                except OSError:
                    pass

        self.failed_transfers.put((job, last_error or "unknown"))
        self.logger.error(f"Job {job.job_id} failed after {self.retry_count} attempts")
        return False

    def run(self):
        """Run the parallel rsync operation"""
        start_time = time.time()
        self.scan_and_distribute()

        if self.is_remote_target:
            self._build_remote_target_manifest()

        jobs = []
        for i, bucket in enumerate(self.buckets):
            job = RsyncJob(
                source_files=[Path(f) for f in bucket],
                source_base=Path(self.remote_source.path if self.is_remote_source else self.source),
                target=str(self.remote_target)
                if self.is_remote_target
                else str(self.target),
                rsync_args=self.rsync_args,
                job_id=i,
                control_path=self.remote_target.control_path
                if self.is_remote_target
                else None,
            )
            jobs.append(job)

        executor = ThreadPoolExecutor(max_workers=self.parallel_jobs)
        try:
            results = list(executor.map(self.execute_rsync, jobs))
        except KeyboardInterrupt:
            self.logger.warning("Interrupted, terminating active transfers...")
            self._terminate_all()
            raise
        finally:
            executor.shutdown(wait=True)

        success_count = sum(1 for r in results if r)
        failed_count = len(results) - success_count
        elapsed_time = time.time() - start_time

        self.logger.info(f"\nTransfer completed in {elapsed_time:.1f} seconds")
        self.logger.info(f"Successfully transferred: {success_count} buckets")
        if failed_count > 0:
            self.logger.error(f"Failed transfers: {failed_count} buckets")
            while not self.failed_transfers.empty():
                job, error = self.failed_transfers.get()
                self.logger.error(f"Job {job.job_id} failed with error: {error}")


def setup_logging(level: int = logging.INFO):
    logging.basicConfig(
        level=level, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Parallel rsync tool with SSH multiplexing"
    )
    parser.add_argument("source", help="Source directory")
    parser.add_argument(
        "target", help="Target directory (local or remote, e.g., user@host:/path)"
    )
    parser.add_argument(
        "-j", "--jobs", type=int, default=4, help="Number of parallel jobs (default: 4)"
    )
    parser.add_argument(
        "-s",
        "--bucket-size",
        type=int,
        default=1000,
        help="Bucket size in MB (default: 1000)",
    )
    parser.add_argument(
        "--rsync-args",
        default="-avz --progress",
        help="Additional rsync arguments (default: -avz --progress)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_const", const=logging.DEBUG,
        dest="log_level", default=logging.INFO,
        help="Enable verbose (debug) logging output"
    )
    parser.add_argument(
        "-q", "--quiet", action="store_const", const=logging.WARNING,
        dest="log_level",
        help="Suppress informational output"
    )
    parser.add_argument(
        "-n", "--dry-run", action="store_true",
        help="Perform a trial run with no changes made"
    )
    parser.add_argument(
        "--retry", type=int, default=3,
        help="Number of retry attempts for failed bucket transfers (default: 3)"
    )

    args = parser.parse_args()

    setup_logging(level=args.log_level)

    rsync_args = shlex.split(args.rsync_args)
    if args.dry_run:
        rsync_args.append("-n")

    try:
        parallel_rsync = ParallelRsync(
            source_dir=args.source,
            target=args.target,
            parallel_jobs=args.jobs,
            bucket_size_mb=args.bucket_size,
            rsync_args=rsync_args,
            retry_count=args.retry,
        )

        parallel_rsync.run()

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error: {e}")
        logging.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
