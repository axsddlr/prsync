# Parallel Rsync Tool

A Python-based utility that parallelizes rsync operations by splitting files into buckets and running multiple rsync processes simultaneously. This tool is designed to maximize bandwidth utilization when transferring large numbers of files.

## Features

- **Parallel Transfer**: Run multiple rsync processes simultaneously
- **Smart File Bucketing**: Group files into balanced buckets based on size
- **SSH Connection Multiplexing**: Single SSH authentication for multiple transfers
- **Progress Tracking**: Real-time progress reporting for all transfers
- **Configurable**: Adjustable parallel jobs, bucket sizes, and rsync arguments
- **Error Handling**: Comprehensive error reporting and failed transfer tracking

## Requirements

- Python 3.6 or higher
- rsync installed on both source and target systems
- SSH access to remote system (for remote transfers)
- Standard Python libraries (all included in standard distribution)

## Installation

1. Download the script:

```bash
wget https://raw.githubusercontent.com/axsddlr/parallel-rsync/main/prsync.py
```

2. Make it executable:

```bash
chmod +x prsync.py
```

## Usage

### Basic Usage

```bash
python prsync.py SOURCE TARGET [OPTIONS]
```

### Command Line Options

```
-j, --jobs INTEGER        Number of parallel jobs (default: 4)
-s, --bucket-size INTEGER Bucket size in MB (default: 1000)
--rsync-args STRING      Additional rsync arguments (default: -avz --progress)
```

### Examples

1. Local to Remote Transfer:

```bash
python prsync.py /local/path user@remote:/remote/path -j 8
```

2. With Custom Rsync Arguments:

```bash
python prsync.py /source/path root@remote-server-ip:/target/path -j 4 --rsync-args="-avzP --compress-level=9 --partial-dir=.rsync-partial"
```

3. Large Files Transfer:

```bash
python prsync.py /media/large_files nas:/backup -j 2 -s 5000
```

## How It Works

1. **File Discovery**: The script scans the source directory and catalogs all files.

2. **Bucket Creation**: Files are grouped into buckets based on the specified bucket size:
   - Default bucket size is 1000MB
   - Files are distributed to maintain roughly equal bucket sizes
   - Very large files may occupy their own bucket

3. **SSH Multiplexing**: For remote transfers:
   - Establishes a single master SSH connection
   - All rsync processes use the same connection
   - Eliminates multiple password prompts
   - Automatically cleans up SSH connections on completion

4. **Parallel Execution**:
   - Creates separate rsync process for each bucket
   - Default of 4 simultaneous transfers
   - Monitors and reports progress of all transfers
   - Handles failures gracefully

## Performance Considerations

- **Number of Jobs**: Start with `-j 4` and adjust based on:
  - Network bandwidth
  - Disk I/O capacity
  - CPU resources
  - Memory availability

- **Bucket Size**: Consider adjusting `-s` (bucket size) based on:
  - Total data size
  - Number of files
  - Average file size
  - Available memory

## Limitations

- Source and target paths must be accessible from the system running the script
- Does not currently support remote-to-remote transfers
- Large numbers of small files may impact performance
- Memory usage increases with the number of files

## Troubleshooting

### Common Issues

1. **SSH Authentication Failures**:
   - Ensure SSH key-based authentication is set up
   - Check SSH permissions on remote system
   - Verify SSH configuration file settings

2. **Performance Issues**:
   - Reduce number of parallel jobs
   - Increase bucket size for large files
   - Decrease bucket size for many small files
   - Check network and disk I/O utilization

3. **Memory Usage**:
   - Decrease bucket size
   - Reduce number of parallel jobs
   - Consider splitting very large transfers into multiple runs

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Future Improvements

- Support for remote source locations
- Remote-to-remote transfer support
- Bandwidth limiting per transfer
- Resume interrupted transfers
- Transfer verification option
- Real-time transfer speed reporting
- Advanced file filtering options

## Author

axsddlr - Initial work

## Acknowledgments

- Inspired by the need for faster transfers of large file sets
- Based on the robust and reliable rsync utility
