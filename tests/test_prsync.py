import os
import sys
import subprocess
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock, call

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from prsync import RemoteTarget, ParallelRsync, RsyncJob


class TestRemoteTarget:
    def test_parse_local_path_returns_none(self):
        assert RemoteTarget.parse("/local/path") is None

    def test_parse_remote_with_user(self):
        target = RemoteTarget.parse("user@host:/remote/path")
        assert target is not None
        assert target.user == "user"
        assert target.host == "host"
        assert target.path == "/remote/path"

    def test_parse_remote_without_user(self):
        target = RemoteTarget.parse("host:/remote/path")
        assert target is not None
        assert target.user is None
        assert target.host == "host"
        assert target.path == "/remote/path"

    def test_str_with_user(self):
        target = RemoteTarget(user="user", host="host", path="/remote/path")
        assert str(target) == "user@host:/remote/path"

    def test_str_without_user(self):
        target = RemoteTarget(user=None, host="host", path="/remote/path")
        assert str(target) == "host:/remote/path"

    def test_ssh_base_args_without_control_path(self):
        target = RemoteTarget(user="user", host="host", path="/p")
        assert target.ssh_base_args() == ["ssh", "-l", "user"]

    def test_ssh_base_args_with_control_path(self):
        target = RemoteTarget(user="u", host="h", path="/p", control_path="/tmp/cp")
        assert target.ssh_base_args() == ["ssh", "-o", "ControlPath=/tmp/cp", "-l", "u"]

    def test_setup_ssh_multiplexing_success(self):
        target = RemoteTarget(user="u", host="h", path="/p")
        with patch("prsync.subprocess.run") as mock_run:
            assert target.setup_ssh_multiplexing() is True
            assert target.control_path is not None
            mock_run.assert_called_once()

    def test_setup_ssh_multiplexing_failure(self):
        target = RemoteTarget(user="u", host="h", path="/p")
        with patch("prsync.subprocess.run", side_effect=subprocess.CalledProcessError(1, "ssh")):
            assert target.setup_ssh_multiplexing() is False
            assert target.control_path is None


class TestParallelRsync:
    def test_remote_to_remote_raises_value_error(self):
        with pytest.raises(ValueError, match="Remote-to-remote transfers are not supported"):
            ParallelRsync(
                source_dir="user@remote1:/src",
                target="user@remote2:/dst",
            )

    def test_local_source_bucket_distribution(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "src"
            src.mkdir()
            (src / "small.txt").write_text("x" * 100)
            (src / "large.txt").write_text("x" * 5_000_000)

            rsync = ParallelRsync(
                source_dir=str(src),
                target=str(Path(tmpdir) / "dst"),
                bucket_size_mb=1,
            )
            rsync.scan_and_distribute()

            assert rsync.total_files == 2

    def test_parse_remote_source(self):
        rsync = ParallelRsync(
            source_dir="user@host:/src",
            target="/local/dst",
        )
        assert rsync.is_remote_source is True
        assert rsync.is_remote_target is False
        assert rsync.remote_source.user == "user"
        assert rsync.remote_source.host == "host"

    def test_parse_remote_target(self):
        rsync = ParallelRsync(
            source_dir="/local/src",
            target="host:/remote/dst",
        )
        assert rsync.is_remote_source is False
        assert rsync.is_remote_target is True
        assert rsync.remote_target.host == "host"

    def test_non_existent_source_directory_raises_error(self):
        with pytest.raises(ValueError, match="Source directory does not exist"):
            rsync = ParallelRsync(source_dir="/nonexistent/path", target="/tmp/dst")
            rsync.scan_and_distribute()

    def test_retry_count_default(self):
        rsync = ParallelRsync(source_dir="/tmp/src", target="/tmp/dst")
        assert rsync.retry_count == 3

    def test_retry_count_custom(self):
        rsync = ParallelRsync(source_dir="/tmp/src", target="/tmp/dst", retry_count=5)
        assert rsync.retry_count == 5

    def test_execute_rsync_retries_on_failure(self):
        rsync = ParallelRsync(
            source_dir="/tmp/src",
            target="/tmp/dst",
            retry_count=2,
        )
        rsync.total_files = 1

        job = RsyncJob(
            source_files=[Path("test.txt")],
            source_base=Path("/tmp/src"),
            target="/tmp/dst",
            rsync_args=[],
            job_id=0,
        )

        with patch("prsync.subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.returncode = 1
            mock_popen.return_value = proc

            result = rsync.execute_rsync(job)

            assert result is False
            assert mock_popen.call_count == 2

    def test_execute_rsync_succeeds_on_first_try(self):
        rsync = ParallelRsync(
            source_dir="/tmp/src",
            target="/tmp/dst",
        )
        rsync.total_files = 1

        job = RsyncJob(
            source_files=[Path("test.txt")],
            source_base=Path("/tmp/src"),
            target="/tmp/dst",
            rsync_args=[],
            job_id=0,
        )

        with patch("prsync.subprocess.Popen") as mock_popen:
            proc = MagicMock()
            proc.returncode = 0
            mock_popen.return_value = proc

            result = rsync.execute_rsync(job)

            assert result is True
            assert mock_popen.call_count == 1

    def test_terminate_all_clears_process_list(self):
        rsync = ParallelRsync(source_dir="/tmp/src", target="/tmp/dst")
        mock_proc = MagicMock()
        rsync._active_processes.append(mock_proc)
        rsync._terminate_all()
        assert len(rsync._active_processes) == 0
        mock_proc.terminate.assert_called_once()

    def test_run_with_local_source(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = Path(tmpdir) / "src"
            src.mkdir()
            (src / "f1.txt").write_text("data")

            rsync = ParallelRsync(
                source_dir=str(src),
                target=str(Path(tmpdir) / "dst"),
                parallel_jobs=1,
            )

            with patch("prsync.subprocess.Popen") as mock_popen:
                proc = MagicMock()
                proc.returncode = 0
                mock_popen.return_value = proc

                rsync.run()
                assert mock_popen.call_count == 1

    def test_dry_run_appends_n_flag(self):
        from prsync import main as prsync_main

        with patch("prsync.ParallelRsync") as mock_rsync:
            with patch("prsync.setup_logging"):
                with patch("sys.argv", ["prsync", "/src", "/dst", "-n"]):
                    try:
                        prsync_main()
                    except SystemExit:
                        pass

            args, kwargs = mock_rsync.call_args
            assert "-n" in kwargs["rsync_args"]
