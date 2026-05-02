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
import tempfile


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

    def setup_ssh_multiplexing(self):
        """Setup SSH connection multiplexing"""
        temp_dir = tempfile.mkdtemp(prefix="rsync_ssh_")
        self.control_path = os.path.join(temp_dir, "control_%h_%p_%r")

        ssh_cmd = ["ssh"]
        if self.user:
            ssh_cmd.extend(["-l", self.user])

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
            subprocess.run(ssh_cmd, check=True)
            return True
        except subprocess.CalledProcessError as e:
            logging.error(f"Failed to setup SSH multiplexing: {e}")
            return False

    def cleanup_ssh_multiplexing(self):
        """Cleanup SSH multiplexing connection"""
        if self.control_path:
            ssh_cmd = ["ssh"]
            if self.user:
                ssh_cmd.extend(["-l", self.user])

            ssh_cmd.extend(
                ["-O", "exit", "-o", f"ControlPath={self.control_path}", self.host]
            )

            try:
                subprocess.run(ssh_cmd, check=True)
            except subprocess.CalledProcessError:
                pass

            if os.path.exists(os.path.dirname(self.control_path)):
                try:
                    os.rmdir(os.path.dirname(self.control_path))
                except OSError:
                    pass


@dataclass
class RsyncJob:
    source_files: List[Path]
    source_base: Path
    target: str
    rsync_args: List[str]
    job_id: int
    is_remote_target: bool
    control_path: Optional[str] = None


class ParallelRsync:
    def __init__(
        self,
        source_dir: str,
        target: str,
        parallel_jobs: int = 4,
        bucket_size_mb: int = 1000,
        rsync_args: List[str] = None,
    ):
        self.source = source_dir
        self.target = target
        self.remote_source = RemoteTarget.parse(source_dir)
        self.remote_target = RemoteTarget.parse(target)
        self.is_remote_source = bool(self.remote_source)
        self.is_remote_target = bool(self.remote_target)
        self.parallel_jobs = parallel_jobs
        self.bucket_size_mb = bucket_size_mb
        self.rsync_args = rsync_args if rsync_args else []

        # Setup SSH multiplexing for remote source
        if self.is_remote_source:
            if not self.remote_source.setup_ssh_multiplexing():
                raise ValueError("Failed to setup SSH connection multiplexing for source")
            atexit.register(self.remote_source.cleanup_ssh_multiplexing)

        # Setup SSH multiplexing for remote target
        if self.is_remote_target:
            if not self.remote_target.setup_ssh_multiplexing():
                raise ValueError("Failed to setup SSH connection multiplexing")
            atexit.register(self.remote_target.cleanup_ssh_multiplexing)

        self.current_bucket: List[Tuple[Path, int]] = []
        self.current_bucket_size = 0
        self.buckets: List[List[Path]] = []

        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        self.progress_lock = Lock()
        self.total_files = 0
        self.completed_files = 0
        self.failed_transfers = Queue()
        self._remote_target_manifest: Set[str] = set()

    def _get_remote_file_list(self, remote: RemoteTarget) -> List[Tuple[str, int]]:
        """Get list of files and sizes from remote host"""
        ssh_cmd = ["ssh"]
        if remote.control_path:
            ssh_cmd.extend(["-o", f"ControlPath={remote.control_path}"])
        if remote.user:
            ssh_cmd.extend(["-l", remote.user])
        
        # Use find to get file paths and sizes
        find_cmd = f"find {shlex.quote(remote.path)} -type f -printf '%s %P\\n'"
        ssh_cmd.extend([remote.host, find_cmd])
        
        try:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True, check=True)
            files = []
            for line in result.stdout.splitlines():
                size, path = line.strip().split(" ", 1)
                files.append((path, int(size)))
            return files
        except subprocess.CalledProcessError as e:
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
            for root, _, filenames in os.walk(source_base):
                for filename in filenames:
                    filepath = Path(root) / filename
                    try:
                        files.append((str(filepath.relative_to(source_base)), filepath.stat().st_size))
                    except OSError as e:
                        self.logger.error(f"Error accessing file {filepath}: {e}")

        for filepath, file_size in files:
            self.total_files += 1
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
        ssh_cmd = ["ssh"]
        if self.remote_target.control_path:
            ssh_cmd.extend(["-o", f"ControlPath={self.remote_target.control_path}"])
        if self.remote_target.user:
            ssh_cmd.extend(["-l", self.remote_target.user])
        find_cmd = f"find {shlex.quote(base)} -type f"
        ssh_cmd.extend([self.remote_target.host, find_cmd])

        try:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True, check=True)
            self._remote_target_manifest = set(result.stdout.splitlines())
            self.logger.info(
                f"Remote target manifest: {len(self._remote_target_manifest)} files"
            )
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to build remote target manifest: {e}")
            raise

    def execute_rsync(self, job: RsyncJob) -> bool:
        """Execute rsync for a given bucket of files"""
        bucket_file_list = f".rsync_filelist_{job.job_id}"
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
            
        try:
            with open(bucket_file_list, "w") as f:
                f.write("\n".join(files_to_sync))

            cmd = ["rsync"] + job.rsync_args

            # Configure SSH for remote source
            if self.is_remote_source and self.remote_source.control_path:
                ssh_cmd = f"ssh -o ControlPath={self.remote_source.control_path}"
                cmd.extend(["-e", ssh_cmd])

            # Configure SSH for remote target
            if self.is_remote_target and self.remote_target.control_path:
                ssh_cmd = f"ssh -o ControlPath={self.remote_target.control_path}"
                cmd.extend(["-e", ssh_cmd])

            source_path = (
                f"{str(self.remote_source)}/"
                if self.is_remote_source
                else f"{str(job.source_base)}/"
            )

            cmd.extend(
                [
                    "--files-from=" + bucket_file_list,
                    source_path,
                    job.target,
                ]
            )

            self.logger.debug(f"Executing rsync command: {' '.join(cmd)}")

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )

            stdout, stderr = process.communicate()

            if process.returncode != 0:
                self.logger.error(f"Rsync failed for job {job.job_id}")
                self.logger.error(f"stderr: {stderr}")
                self.failed_transfers.put((job, stderr))
                return False

            with self.progress_lock:
                self.completed_files += len(files_to_sync)
                progress = (self.completed_files / self.total_files) * 100
                self.logger.info(
                    f"Progress: {progress:.1f}% ({self.completed_files}/{self.total_files})"
                )

            return True

        finally:
            try:
                os.remove(bucket_file_list)
            except OSError:
                pass

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
                is_remote_target=self.is_remote_target,
                control_path=self.remote_target.control_path
                if self.is_remote_target
                else None,
            )
            jobs.append(job)

        with ThreadPoolExecutor(max_workers=self.parallel_jobs) as executor:
            results = list(executor.map(self.execute_rsync, jobs))

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

    args = parser.parse_args()

    try:
        parallel_rsync = ParallelRsync(
            source_dir=args.source,
            target=args.target,
            parallel_jobs=args.jobs,
            bucket_size_mb=args.bucket_size,
            rsync_args=args.rsync_args.split(),
        )

        parallel_rsync.run()

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
