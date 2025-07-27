# YFS: Vertical File System

A Vertical file system implementation in Go that stores everything in three files: index, free blocks, and block storage.

## Architecture

YFS consists of three main files:
- **index.yfs**: Protocol Buffer file containing the directory tree and file metadata
- **free.yfs**: Binary file listing available block IDs for reuse
- **blocks.glob**: Binary file containing the actual file data in fixed-size blocks

## Project Structure

```
yfs/
├── binutils/
│   └── yfs_binutils.go    # Utility functions for YFS
│   └── go.mod             # Go module configuration
├── lib/
│   └── go.mod             # Go module configuration
│   ├── yfs.proto          # Protocol buffer definitions
│   ├── yfs.pb.go          # Generated protobuf code
│   └── yfs.go             # Main YFS implementation
├── go.work                # Go workspace configuration
├── build.sh               # Build script for binutils
├── prepare.sh             # Script to prepare the environment
└── README.md

```

## Build and Run

### 1. Clone the Repository

```bash
git clone https://github.com/sammwyy/yfs.git
cd yfs
```

### 2. Generate Protocol Buffer Code

Save the protobuf definition as `yfs/yfs.proto`, then run:

```bash
cd lib
protoc --go_out=. --go_opt=paths=source_relative yfs.proto
cd ..
```

This will generate `yfs.pb.go` with the required structs.

### 3. Build and Run

```bash
chmod +x ./build.sh

# Build the binutils
./build.sh

# Interactive YFS shell bin
./dist/yfs -dir ./workspace
```

## Key Features

### Block Management
- **Fixed-size blocks**: Default 128 bytes (configurable)
- **Block chaining**: Files can span multiple blocks using next-block pointers
- **Free block reuse**: Deleted file blocks are tracked and reused
- **Dynamic allocation**: New blocks created as needed

### File Operations
- **WriteFile**: Create or update files with automatic block management
- **ReadFile**: Read complete file contents from block chains
- **DeleteFile**: Remove files and mark blocks as free
- **CopyFile**: Duplicate files with independent block allocation
- **MoveFile**: Rename/move files (only updates index)

### Directory Operations
- **Ls**: List files and directories in a specific path
- **LsAll**: Get complete directory tree structure
- **Automatic directory creation**: Parent directories created as needed

### System Information
- **GetStats**: File system statistics (blocks used/free, version, etc.)
- **GetBlockSize**: Current block size configuration

## Block Storage Format

### Blocks File Header
```
[0-1]   Block size (uint16, little-endian)
```

### Block Structure
```
[0-N]       Block data (N = block_size bytes)
[N-N+3]     Next block ID (uint32, little-endian, 0 = end of file)
```

### Block Offset Calculation
```
offset = 2 + ((block_size + 4) * (block_id - 1))
```

## Protocol Buffer Schema

The index file uses Protocol Buffers for efficient serialization:

- **FileSystemHeader**: Root container with version and directory tree
- **DirectoryEntry**: Directory with files and subdirectories maps
- **FileEntry_pb**: File metadata with first block ID and size
- **FileMetadata**: Common metadata (name, modification time)

## Usage Example

```go
// Create new YFS instance
fs, err := yfs.NewYFS("/path/to/yfs/directory")

// Write a file
err = fs.WriteFile("test.txt", []byte("Hello, YFS!"))

// Read a file
data, err := fs.ReadFile("test.txt")

// List directory
entries, err := fs.Ls("/")

// Get complete tree
tree, err := fs.LsAll()

// File operations
err = fs.CopyFile("test.txt", "backup.txt")
err = fs.MoveFile("backup.txt", "archive/backup.txt")
err = fs.DeleteFile("test.txt")

// System info
stats, err := fs.GetStats()
```

## Performance Characteristics

- **Random access**: O(1) block access by ID
- **File read/write**: O(blocks_used) for file operations
- **Directory lookup**: O(1) hash map access in Protocol Buffers
- **Space efficiency**: Minimal overhead with block reuse
- **Scalability**: Supports files up to 2^32 blocks (~280TB with 128-byte blocks)

## Error Handling

The library provides comprehensive error handling for:
- File not found errors
- Invalid path errors
- Block allocation failures
- I/O errors
- Protocol buffer serialization errors

## Thread Safety

**Note**: This implementation is not thread-safe. For concurrent access, implement appropriate locking mechanisms around YFS operations.

## Limitations

- **Single-threaded**: No built-in concurrency support
- **Memory usage**: Directory tree loaded entirely in memory
- **Platform dependent**: File paths use OS-specific separators
- **No compression**: Files stored as-is without compression
- **No encryption**: No built-in security features

## Future Enhancements

Potential improvements could include:
- Compression support
- Encryption/security features
- Concurrent access with locking
- Memory-mapped file access
- Block-level checksums
- Directory streaming for large file systems
- Custom block sizes per file type

## License

MIT License