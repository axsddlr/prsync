# prsync

Parallel rsync tool that splits file transfers across multiple concurrent rsync processes with SSH connection multiplexing. Maximizes bandwidth utilization for large file sets.

## What It Does

`prsync` scans a source directory, groups files into size-balanced buckets, and transfers each bucket via a separate rsync process running in parallel. For remote targets, it sets up SSH connection multiplexing so all concurrent rsync processes share a single authenticated SSH connection.

## Requirements

- Python 3.6+
- rsync installed on both source and target systems
- SSH access to remote systems (for remote transfers)

## Installation

```bash
curl -O https://raw.githubusercontent.com/axsddlr/prsync/main/prsync.py
chmod +x prsync.py
```

Or run directly:

```bash
python prsync.py SOURCE TARGET [OPTIONS]
```

## Usage

```bash
python prsync.py /local/path user@remote:/remote/path -j 8
```

### Options

| Flag | Description | Default |
|------|-------------|---------|
| `-j, --jobs N` | Number of parallel rsync processes | `4` |
| `-s, --bucket-size MB` | Target size per bucket in MB | `1000` |
| `--rsync-args "ARGS"` | Additional rsync arguments (passed through `shlex.split`) | `-avz --progress` |
| `-v, --verbose` | Enable debug logging | |
| `-q, --quiet` | Suppress informational output | |
| `-n, --dry-run` | Trial run with no changes made | |

### Examples

Local to remote with 8 parallel jobs:

```bash
python prsync.py /local/path user@remote:/remote/path -j 8
```

Large files, fewer jobs, bigger buckets:

```bash
python prsync.py /media/video nas:/backup -j 2 -s 5000
```

Custom rsync flags:

```bash
python prsync.py ./src root@server:/dest -j 4 --rsync-args="-avzP --compress-level=9"
```

Dry run to preview without transferring:

```bash
python prsync.py /local /backup -n
```

## How It Works

1. **Scan** — Walks the source directory (or queries the remote host via SSH) and catalogs all files with sizes.
2. **Bucket** — Distributes files into balanced buckets based on total size (default ~1000 MB each). Files that exceed the bucket size occupy their own bucket.
3. **SSH multiplex** — For remote transfers, establishes a single master SSH connection (`ControlMaster=yes`) reused by all parallel rsync processes. Falls back to regular SSH if multiplexing setup fails.
4. **Transfer** — Spawns up to `-j` concurrent rsync processes, each handling one bucket via `--files-from` with null-separated file lists. Skips files that already exist at the target.

## Development

`prsync` is a single-file Python script with no external dependencies beyond the standard library.

```bash
git clone https://github.com/axsddlr/prsync.git
cd prsync
python prsync.py --help
```

## License

MIT — see [LICENSE](LICENSE).
