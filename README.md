# YFS: Vertical File System

A vertical file system implementation in Go, built for fast, append-friendly, and scalable random access over a block-based storage abstraction.

---

## ⚙️ Architecture

YFS consists of three core files:

* **`root.yfs`**: Protocol Buffer file containing the directory tree and file metadata (inodes)
* **`bitmap.yfs`**: Binary bitmap that tracks free and used blocks efficiently
* **`blocks.glob`**: Raw binary data, storing all fixed-size blocks (data and metadata blocks)

---

## 📁 Project Structure

```
yfs/
├── binutils/
│   ├── yfs_binutils.go    # Utility functions for YFS
│   └── go.mod             # Go module configuration
├── lib/
│   ├── go.mod             # Go module configuration
│   ├── yfs.proto          # Protocol buffer definitions
│   ├── yfs.pb.go          # Generated protobuf code
│   └── yfs.go             # Main YFS implementation
├── go.work                # Go workspace configuration
├── build.sh               # Build script for binutils
├── prepare.sh             # Script to prepare the environment
└── README.md
```

---

## 🚀 Build and Run

### 1. Clone the Repository

```bash
git clone https://github.com/sammwyy/yfs.git
cd yfs
```

### 2. Generate Protocol Buffer Code

```bash
chmod +x ./build_proto.sh
./build_proto.sh
```

### 3. Build and Launch

```bash
chmod +x ./build.sh
./build.sh

# Start interactive YFS shell
./dist/yfs -dir ./workspace
```

---

## 🔑 Key Features

### ✅ Block Management

* **Fixed-size blocks** (default 128 bytes, configurable)
* **Indexed block chaining**: Files reference a first *block index*, which lists all blocks (including ranges)
* **Next-block pointers** inside block indexes allow chaining large files
* **4-byte footer per block** for pointing to next index block
* **Free block tracking** via a fast bitmap in `bitmap.yfs`
* **Dynamic allocation** with intelligent bitmap traversal

### ✅ File Operations

* **WriteFile**: Automatically allocates blocks and updates index chain
* **ReadFile**: Efficient sequential reads using index block + data blocks
* **DeleteFile**: Frees all data and index blocks using bitmap
* **CopyFile**: Creates new file with duplicated block chain
* **MoveFile**: Updates metadata without touching underlying data

### ✅ Directory Operations

* **Ls / LsAll**: List contents of a path or full tree
* **CreateDirectory**: Automatically creates parent directories
* **DirectoryEntry** now uses lightweight pointers (name + block ID)

### ✅ System Info

* **GetStats**: View stats like block usage, file count, etc.
* **GetBlockSize**: Query the configured block size

---

## 🧱 Block Storage Format

### Global Header (in `blocks.glob`)

```
[0-1]  Block size (uint16, little-endian)
```

### Data Block Format

```
[0 - N-5]     Raw data
[N-4 - N-1]   Next block ID (uint32, 0 if none)
```

### Index Block Format

```
- Contains a list of:
   - block IDs (uint32)
   - or block ranges (start:end)

- Ends with:
   - [N-4 - N-1] → next index block ID
```

### Offset Calculation

```go
offset = 2 + (block_id * (block_size + 4)) // +4 for footer
```

---

## 🧬 Protocol Buffer Schema Highlights

YFS uses Protobuf to define its file/directory metadata (`root.yfs`):

* **FileSystemHeader**: Includes version, block size, and root directory pointer
* **DirectoryEntry**: Contains directory metadata and list of `FilePointer`s
* **FileEntry_pb**: Stores file metadata, total size, and pointer to first index block
* **FilePointer / DirectoryPointer**: Efficiently references files/directories by name + block ID

---

## 📌 Example Usage (Go)

```go
fs, _ := yfs.NewYFS("/path/to/yfs")

fs.WriteFile("hello.txt", []byte("Hello, world!"))
data, _ := fs.ReadFile("hello.txt")

fs.CreateDirectory("docs")
fs.MoveFile("hello.txt", "docs/hello.txt")

tree, _ := fs.LsAll()
stats, _ := fs.GetStats()
```

---

## ⚡ Performance

| Operation        | Complexity                  |
| ---------------- | --------------------------- |
| Block access     | O(1) (by ID)                |
| File read/write  | O(blocks\_used)             |
| Directory lookup | O(1) (map in-memory)        |
| Allocation       | O(1) avg (bitmap-optimized) |
| File rename/move | O(1)                        |

* **Append-efficient**, supports large files via block chains
* **Minimal overhead**: only 4 bytes per block for chaining
* **Scalable**: Up to \~280 TB with 128-byte blocks

---

## 🛑 Limitations

* **Full tree loaded into memory**
* **No compression or encryption (yet)**

---

## 🌱 Future Enhancements

* ✅ Streamed access to large files and directories
* ✅ Optional block-level checksums
* 🔒 Encryption at block level
* 📦 Compression for large data
* 🔁 Journaling for write safety

---

## 📜 License

MIT License