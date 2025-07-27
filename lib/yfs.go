package yfs

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
)

const (
	DefaultBlockSize = 128
	HeaderSize       = 2
	NextBlockSize    = 4
	NullBlockID      = 0
)

// YFS represents the file system
type YFS struct {
	indexPath  string
	freePath   string
	blocksPath string
	blockSize  uint32
	header     *FileSystemHeader
}

// FileEntry represents a file or directory in the system
type FileEntry struct {
	Name         string
	IsDirectory  bool
	Size         int64
	ModTime      time.Time
	FirstBlockID uint32
	Children     map[string]*FileEntry
}

// NewYFS creates a new YFS instance from a directory containing the three files
func NewYFS(dir string) (*YFS, error) {
	return NewYFSFromPaths(
		filepath.Join(dir, "index.yfs"),
		filepath.Join(dir, "free.yfs"),
		filepath.Join(dir, "blocks.glob"),
	)
}

// NewYFSFromPaths creates a new YFS instance from individual file paths
func NewYFSFromPaths(indexPath, freePath, blocksPath string) (*YFS, error) {
	yfs := &YFS{
		indexPath:  indexPath,
		freePath:   freePath,
		blocksPath: blocksPath,
		blockSize:  DefaultBlockSize,
	}

	// Initialize or load the file system
	if err := yfs.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize YFS: %v", err)
	}

	return yfs, nil
}

// initialize sets up the YFS instance, creating files if they don't exist
func (yfs *YFS) initialize() error {
	// Check if index file exists
	if _, err := os.Stat(yfs.indexPath); os.IsNotExist(err) {
		// Create new file system
		return yfs.createFileSystem()
	}

	// Load existing file system
	return yfs.loadFileSystem()
}

// createFileSystem creates a new empty file system
func (yfs *YFS) createFileSystem() error {
	// Create header with default settings
	yfs.header = &FileSystemHeader{
		Version:   1,
		BlockSize: DefaultBlockSize,
		Root: &DirectoryEntry{
			Metadata: &FileMetadata{
				Name:    "/",
				ModTime: time.Now().Unix(),
			},
			Files:       make(map[string]*FileEntryPb),
			Directories: make(map[string]*DirectoryEntry),
		},
	}

	// Create empty files
	if err := yfs.saveIndex(); err != nil {
		return err
	}

	if err := yfs.createEmptyFile(yfs.freePath); err != nil {
		return err
	}

	if err := yfs.createEmptyBlocksFile(); err != nil {
		return err
	}

	return nil
}

// createEmptyFile creates an empty file
func (yfs *YFS) createEmptyFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	return file.Close()
}

// createEmptyBlocksFile creates the blocks file with header
func (yfs *YFS) createEmptyBlocksFile() error {
	file, err := os.Create(yfs.blocksPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header (block size as int16)
	header := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint16(header, uint16(yfs.blockSize))
	_, err = file.Write(header)
	return err
}

// loadFileSystem loads an existing file system
func (yfs *YFS) loadFileSystem() error {
	// Read index file
	data, err := os.ReadFile(yfs.indexPath)
	if err != nil {
		return fmt.Errorf("failed to read index file: %v", err)
	}

	yfs.header = &FileSystemHeader{}
	if err := proto.Unmarshal(data, yfs.header); err != nil {
		return fmt.Errorf("failed to unmarshal index: %v", err)
	}

	yfs.blockSize = yfs.header.BlockSize
	return nil
}

// saveIndex saves the index to disk
func (yfs *YFS) saveIndex() error {
	data, err := proto.Marshal(yfs.header)
	if err != nil {
		return fmt.Errorf("failed to marshal index: %v", err)
	}

	return os.WriteFile(yfs.indexPath, data, 0644)
}

// calculateBlockOffset calculates the offset for a given block ID
func (yfs *YFS) calculateBlockOffset(blockID uint32) int64 {
	if blockID == NullBlockID {
		return -1
	}
	return int64(HeaderSize) + int64(yfs.blockSize+NextBlockSize)*int64(blockID-1)
}

// allocateBlock allocates a new block, reusing free blocks if available
func (yfs *YFS) allocateBlock() (uint32, error) {
	// Try to get a free block first
	if freeBlockID, err := yfs.getFreeBlock(); err == nil && freeBlockID != NullBlockID {
		return freeBlockID, nil
	}

	// No free blocks available, create a new one
	return yfs.createNewBlock()
}

// getFreeBlock gets a block from the free blocks list
func (yfs *YFS) getFreeBlock() (uint32, error) {
	file, err := os.OpenFile(yfs.freePath, os.O_RDWR, 0644)
	if err != nil {
		return NullBlockID, err
	}
	defer file.Close()

	// Check file size
	stat, err := file.Stat()
	if err != nil {
		return NullBlockID, err
	}

	if stat.Size() < 4 {
		return NullBlockID, fmt.Errorf("no free blocks available")
	}

	// Read the last block ID
	_, err = file.Seek(-4, io.SeekEnd)
	if err != nil {
		return NullBlockID, err
	}

	blockIDBytes := make([]byte, 4)
	_, err = file.Read(blockIDBytes)
	if err != nil {
		return NullBlockID, err
	}

	blockID := binary.LittleEndian.Uint32(blockIDBytes)

	// Remove the block ID from the file by truncating
	newSize := stat.Size() - 4
	err = file.Truncate(newSize)
	if err != nil {
		return NullBlockID, err
	}

	return blockID, nil
}

// createNewBlock creates a new block at the end of the blocks file
func (yfs *YFS) createNewBlock() (uint32, error) {
	file, err := os.OpenFile(yfs.blocksPath, os.O_RDWR, 0644)
	if err != nil {
		return NullBlockID, err
	}
	defer file.Close()

	// Get current file size to determine the new block ID
	stat, err := file.Stat()
	if err != nil {
		return NullBlockID, err
	}

	// Calculate block ID (1-based)
	currentBlocks := (stat.Size() - HeaderSize) / int64(yfs.blockSize+NextBlockSize)
	newBlockID := uint32(currentBlocks + 1)

	// Seek to end and write empty block
	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return NullBlockID, err
	}

	// Write empty block data + next block ID (0 = null)
	blockData := make([]byte, yfs.blockSize+NextBlockSize)
	binary.LittleEndian.PutUint32(blockData[yfs.blockSize:], NullBlockID)

	_, err = file.Write(blockData)
	if err != nil {
		return NullBlockID, err
	}

	return newBlockID, nil
}

// freeBlock marks a block as free
func (yfs *YFS) freeBlock(blockID uint32) error {
	if blockID == NullBlockID {
		return nil
	}

	file, err := os.OpenFile(yfs.freePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Append block ID to free blocks file
	blockIDBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockIDBytes, blockID)

	_, err = file.Write(blockIDBytes)
	return err
}

// writeBlock writes data to a specific block
func (yfs *YFS) writeBlock(blockID uint32, data []byte, nextBlockID uint32) error {
	file, err := os.OpenFile(yfs.blocksPath, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	offset := yfs.calculateBlockOffset(blockID)
	if offset < 0 {
		return fmt.Errorf("invalid block ID: %d", blockID)
	}

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}

	// Prepare block data (pad if necessary)
	blockData := make([]byte, yfs.blockSize)
	copy(blockData, data)

	// Write block data
	_, err = file.Write(blockData)
	if err != nil {
		return err
	}

	// Write next block ID
	nextBlockBytes := make([]byte, NextBlockSize)
	binary.LittleEndian.PutUint32(nextBlockBytes, nextBlockID)
	_, err = file.Write(nextBlockBytes)

	return err
}

// readBlock reads data from a specific block
func (yfs *YFS) readBlock(blockID uint32) ([]byte, uint32, error) {
	file, err := os.Open(yfs.blocksPath)
	if err != nil {
		return nil, NullBlockID, err
	}
	defer file.Close()

	offset := yfs.calculateBlockOffset(blockID)
	if offset < 0 {
		return nil, NullBlockID, fmt.Errorf("invalid block ID: %d", blockID)
	}

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, NullBlockID, err
	}

	// Read block data
	blockData := make([]byte, yfs.blockSize)
	_, err = file.Read(blockData)
	if err != nil {
		return nil, NullBlockID, err
	}

	// Read next block ID
	nextBlockBytes := make([]byte, NextBlockSize)
	_, err = file.Read(nextBlockBytes)
	if err != nil {
		return nil, NullBlockID, err
	}

	nextBlockID := binary.LittleEndian.Uint32(nextBlockBytes)
	return blockData, nextBlockID, nil
}

// writeFileToBlocks writes file data across multiple blocks
func (yfs *YFS) writeFileToBlocks(data []byte, existingFirstBlockID uint32) (uint32, error) {
	if len(data) == 0 {
		// Free existing blocks if any
		if existingFirstBlockID != NullBlockID {
			yfs.freeBlockChain(existingFirstBlockID)
		}
		return NullBlockID, nil
	}

	var firstBlockID uint32
	var currentBlockID uint32
	// var previousBlockID uint32

	// Try to reuse existing blocks
	existingBlockID := existingFirstBlockID
	existingBlocks := make([]uint32, 0)

	// Collect existing block chain
	for existingBlockID != NullBlockID {
		existingBlocks = append(existingBlocks, existingBlockID)
		_, nextBlockID, err := yfs.readBlock(existingBlockID)
		if err != nil {
			break
		}
		existingBlockID = nextBlockID
	}

	offset := 0
	blockIndex := 0

	for offset < len(data) {
		// Determine how much data to write to this block
		remainingData := len(data) - offset
		blockDataSize := int(yfs.blockSize)
		if remainingData < blockDataSize {
			blockDataSize = remainingData
		}

		blockData := data[offset : offset+blockDataSize]

		// Reuse existing block or allocate new one
		if blockIndex < len(existingBlocks) {
			currentBlockID = existingBlocks[blockIndex]
		} else {
			var err error
			currentBlockID, err = yfs.allocateBlock()
			if err != nil {
				return NullBlockID, err
			}
		}

		if firstBlockID == NullBlockID {
			firstBlockID = currentBlockID
		}

		// Determine next block ID
		var nextBlockID uint32 = NullBlockID
		if offset+blockDataSize < len(data) {
			// More data to write, will need another block
			if blockIndex+1 < len(existingBlocks) {
				nextBlockID = existingBlocks[blockIndex+1]
			} else {
				nextBlockID, _ = yfs.allocateBlock()
			}
		}

		// Write the block
		err := yfs.writeBlock(currentBlockID, blockData, nextBlockID)
		if err != nil {
			return NullBlockID, err
		}

		// previousBlockID = currentBlockID
		offset += blockDataSize
		blockIndex++
	}

	// Free any unused existing blocks
	for i := blockIndex; i < len(existingBlocks); i++ {
		yfs.freeBlock(existingBlocks[i])
	}

	return firstBlockID, nil
}

// readFileFromBlocks reads file data from multiple blocks
func (yfs *YFS) readFileFromBlocks(firstBlockID uint32) ([]byte, error) {
	if firstBlockID == NullBlockID {
		return []byte{}, nil
	}

	var result []byte
	currentBlockID := firstBlockID

	for currentBlockID != NullBlockID {
		blockData, nextBlockID, err := yfs.readBlock(currentBlockID)
		if err != nil {
			return nil, err
		}

		// For the last block, we might not need all the data
		if nextBlockID == NullBlockID {
			// Find the actual end of data (remove trailing zeros)
			actualSize := len(blockData)
			for actualSize > 0 && blockData[actualSize-1] == 0 {
				actualSize--
			}
			blockData = blockData[:actualSize]
		}

		result = append(result, blockData...)
		currentBlockID = nextBlockID
	}

	return result, nil
}

// freeBlockChain frees an entire chain of blocks
func (yfs *YFS) freeBlockChain(firstBlockID uint32) error {
	currentBlockID := firstBlockID

	for currentBlockID != NullBlockID {
		_, nextBlockID, err := yfs.readBlock(currentBlockID)
		if err != nil {
			return err
		}

		if err := yfs.freeBlock(currentBlockID); err != nil {
			return err
		}

		currentBlockID = nextBlockID
	}

	return nil
}

// findEntry finds a file or directory entry by path
func (yfs *YFS) findEntry(path string) (*DirectoryEntry, *FileEntryPb, bool, error) {
	path = strings.Trim(path, "/")
	if path == "" {
		return yfs.header.Root, nil, true, nil
	}

	parts := strings.Split(path, "/")
	currentDir := yfs.header.Root

	// Navigate to the parent directory
	for i, part := range parts[:len(parts)-1] {
		if subDir, exists := currentDir.Directories[part]; exists {
			currentDir = subDir
		} else {
			return nil, nil, false, fmt.Errorf("directory not found: %s", strings.Join(parts[:i+1], "/"))
		}
	}

	finalName := parts[len(parts)-1]

	// Check if it's a directory
	if subDir, exists := currentDir.Directories[finalName]; exists {
		return subDir, nil, true, nil
	}

	// Check if it's a file
	if file, exists := currentDir.Files[finalName]; exists {
		return currentDir, file, false, nil
	}

	return currentDir, nil, false, nil
}

// WriteFile creates or updates a file
func (yfs *YFS) WriteFile(path string, data []byte) error {
	_, file, isDir, err := yfs.findEntry(path)
	if err != nil {
		return err
	}

	if isDir && file == nil {
		return fmt.Errorf("path is a directory: %s", path)
	}

	pathParts := strings.Split(strings.Trim(path, "/"), "/")
	fileName := pathParts[len(pathParts)-1]

	// Create parent directories if they don't exist
	parentPath := strings.Join(pathParts[:len(pathParts)-1], "/")
	parentDir, _, _, err := yfs.findEntry(parentPath)
	if err != nil {
		// Create parent directories
		if err := yfs.createDirectoryChain(parentPath); err != nil {
			return err
		}
		parentDir, _, _, _ = yfs.findEntry(parentPath)
	}

	var existingFirstBlockID uint32
	if file != nil {
		existingFirstBlockID = file.FirstBlockId
	}

	// Write data to blocks
	firstBlockID, err := yfs.writeFileToBlocks(data, existingFirstBlockID)
	if err != nil {
		return err
	}

	// Update or create file entry
	if file == nil {
		file = &FileEntryPb{
			Metadata: &FileMetadata{
				Name:    fileName,
				ModTime: time.Now().Unix(),
			},
			FirstBlockId: firstBlockID,
			Size:         int64(len(data)),
		}
		parentDir.Files[fileName] = file
	} else {
		file.FirstBlockId = firstBlockID
		file.Size = int64(len(data))
		file.Metadata.ModTime = time.Now().Unix()
	}

	return yfs.saveIndex()
}

// ReadFile reads a file's contents
func (yfs *YFS) ReadFile(path string) ([]byte, error) {
	_, file, isDir, err := yfs.findEntry(path)
	if err != nil {
		return nil, err
	}

	if isDir || file == nil {
		return nil, fmt.Errorf("file not found: %s", path)
	}

	return yfs.readFileFromBlocks(file.FirstBlockId)
}

// DeleteFile deletes a file
func (yfs *YFS) DeleteFile(path string) error {
	parentDir, file, isDir, err := yfs.findEntry(path)
	if err != nil {
		return err
	}

	if isDir || file == nil {
		return fmt.Errorf("file not found: %s", path)
	}

	// Free blocks
	if err := yfs.freeBlockChain(file.FirstBlockId); err != nil {
		return err
	}

	// Remove from index
	pathParts := strings.Split(strings.Trim(path, "/"), "/")
	fileName := pathParts[len(pathParts)-1]
	delete(parentDir.Files, fileName)

	return yfs.saveIndex()
}

// CopyFile copies a file
func (yfs *YFS) CopyFile(srcPath, dstPath string) error {
	data, err := yfs.ReadFile(srcPath)
	if err != nil {
		return err
	}

	return yfs.WriteFile(dstPath, data)
}

// MoveFile moves/renames a file
func (yfs *YFS) MoveFile(srcPath, dstPath string) error {
	if err := yfs.CopyFile(srcPath, dstPath); err != nil {
		return err
	}

	return yfs.DeleteFile(srcPath)
}

// createDirectoryChain creates a chain of directories
func (yfs *YFS) createDirectoryChain(path string) error {
	if path == "" {
		return nil
	}

	parts := strings.Split(path, "/")
	currentDir := yfs.header.Root

	for _, part := range parts {
		if part == "" {
			continue
		}

		if _, exists := currentDir.Directories[part]; !exists {
			currentDir.Directories[part] = &DirectoryEntry{
				Metadata: &FileMetadata{
					Name:    part,
					ModTime: time.Now().Unix(),
				},
				Files:       make(map[string]*FileEntryPb),
				Directories: make(map[string]*DirectoryEntry),
			}
		}
		currentDir = currentDir.Directories[part]
	}

	return nil
}

// Ls lists files and directories in a path
func (yfs *YFS) Ls(path string) ([]FileEntry, error) {
	dir, _, isDir, err := yfs.findEntry(path)
	if err != nil {
		return nil, err
	}

	if !isDir {
		return nil, fmt.Errorf("path is not a directory: %s", path)
	}

	var entries []FileEntry

	// Add directories
	for name, subDir := range dir.Directories {
		entries = append(entries, FileEntry{
			Name:        name,
			IsDirectory: true,
			ModTime:     time.Unix(subDir.Metadata.ModTime, 0),
		})
	}

	// Add files
	for name, file := range dir.Files {
		entries = append(entries, FileEntry{
			Name:         name,
			IsDirectory:  false,
			Size:         file.Size,
			ModTime:      time.Unix(file.Metadata.ModTime, 0),
			FirstBlockID: file.FirstBlockId,
		})
	}

	return entries, nil
}

// LsAll returns the complete directory tree
func (yfs *YFS) LsAll() (*FileEntry, error) {
	return yfs.buildDirectoryTree(yfs.header.Root, "/"), nil
}

// buildDirectoryTree recursively builds the directory tree
func (yfs *YFS) buildDirectoryTree(dir *DirectoryEntry, path string) *FileEntry {
	entry := &FileEntry{
		Name:        filepath.Base(path),
		IsDirectory: true,
		ModTime:     time.Unix(dir.Metadata.ModTime, 0),
		Children:    make(map[string]*FileEntry),
	}

	// Add subdirectories
	for name, subDir := range dir.Directories {
		childPath := filepath.Join(path, name)
		entry.Children[name] = yfs.buildDirectoryTree(subDir, childPath)
	}

	// Add files
	for name, file := range dir.Files {
		entry.Children[name] = &FileEntry{
			Name:         name,
			IsDirectory:  false,
			Size:         file.Size,
			ModTime:      time.Unix(file.Metadata.ModTime, 0),
			FirstBlockID: file.FirstBlockId,
		}
	}

	return entry
}

// GetBlockSize returns the current block size
func (yfs *YFS) GetBlockSize() uint32 {
	return yfs.blockSize
}

// GetStats returns file system statistics
func (yfs *YFS) GetStats() (map[string]interface{}, error) {
	// Count total blocks
	blocksFile, err := os.Open(yfs.blocksPath)
	if err != nil {
		return nil, err
	}
	defer blocksFile.Close()

	blocksStat, err := blocksFile.Stat()
	if err != nil {
		return nil, err
	}

	totalBlocks := (blocksStat.Size() - HeaderSize) / int64(yfs.blockSize+NextBlockSize)

	// Count free blocks
	freeFile, err := os.Open(yfs.freePath)
	if err != nil {
		return nil, err
	}
	defer freeFile.Close()

	freeStat, err := freeFile.Stat()
	if err != nil {
		return nil, err
	}

	freeBlocks := freeStat.Size() / 4

	stats := map[string]interface{}{
		"version":      yfs.header.Version,
		"block_size":   yfs.blockSize,
		"total_blocks": totalBlocks,
		"free_blocks":  freeBlocks,
		"used_blocks":  totalBlocks - freeBlocks,
	}

	return stats, nil
}
