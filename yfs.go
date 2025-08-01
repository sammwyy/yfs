package yfs

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

const (
	DefaultBlockSize  = 4096
	HeaderSize        = 4 // Block size as uint32
	NullBlockID       = 0
	MaxBlocksPerIndex = 1000 // Maximum block IDs per index block
	BitmapCacheSize   = 1024 // Number of bitmap bytes to cache
)

// YFS represents the refactored file system
type YFS struct {
	rootPath        string
	bitmapPath      string
	blocksPath      string
	blockSize       uint32
	header          *FileSystemHeader
	bitmap          *BlockBitmap
	mutex           sync.RWMutex
	checksumEnabled bool
}

// BlockBitmap manages free/used blocks efficiently
type BlockBitmap struct {
	data        []byte
	totalBlocks uint64
	searchPos   uint64 // Last search position for optimization
	mutex       sync.RWMutex
	dirty       bool // Whether bitmap needs to be saved
}

// FileInfo represents file information for external use
type FileInfo struct {
	Name        string
	IsDirectory bool
	Size        int64
	ModTime     time.Time
	CreateTime  time.Time
	BlockCount  uint32
}

// New creates a new YFS instance from a directory
func New(dir string) (*YFS, error) {
	return NewFromPaths(
		filepath.Join(dir, "root.yfs"),
		filepath.Join(dir, "bitmap.yfs"),
		filepath.Join(dir, "blocks.glob"),
	)
}

// NewFromPaths creates a new YFS instance from individual file paths
func NewFromPaths(rootPath, bitmapPath, blocksPath string) (*YFS, error) {
	yfs := &YFS{
		rootPath:        rootPath,
		bitmapPath:      bitmapPath,
		blocksPath:      blocksPath,
		blockSize:       DefaultBlockSize,
		checksumEnabled: true,
	}

	if err := yfs.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize YFS: %w", err)
	}

	return yfs, nil
}

// initialize sets up the YFS instance
func (yfs *YFS) initialize() error {
	yfs.mutex.Lock()
	defer yfs.mutex.Unlock()

	// Check if root file exists
	if _, err := os.Stat(yfs.rootPath); os.IsNotExist(err) {
		return yfs.createFileSystem()
	}

	return yfs.loadFileSystem()
}

// createFileSystem creates a new empty file system
func (yfs *YFS) createFileSystem() error {
	// Create header with default settings
	yfs.header = &FileSystemHeader{
		Version:   2,
		BlockSize: DefaultBlockSize,
		Root: &DirectoryEntry{
			Metadata: &FileMetadata{
				Name:       "/",
				ModTime:    time.Now().Unix(),
				CreateTime: time.Now().Unix(),
			},
			Files:       make(map[string]*FileEntry),
			Directories: make(map[string]*DirectoryEntry),
		},
		TotalBlocks:     0,
		ChecksumEnabled: 1,
	}

	// Initialize bitmap
	yfs.bitmap = &BlockBitmap{
		data:        make([]byte, 1024), // Start with 8192 blocks capacity
		totalBlocks: 8192,
		searchPos:   0,
		dirty:       true,
	}

	// Create files
	if err := yfs.saveRoot(); err != nil {
		return err
	}

	if err := yfs.saveBitmap(); err != nil {
		return err
	}

	if err := yfs.createEmptyBlocksFile(); err != nil {
		return err
	}

	return nil
}

// loadFileSystem loads an existing file system
func (yfs *YFS) loadFileSystem() error {
	// Load root
	data, err := os.ReadFile(yfs.rootPath)
	if err != nil {
		return fmt.Errorf("failed to read root file: %w", err)
	}

	yfs.header = &FileSystemHeader{}
	if err := proto.Unmarshal(data, yfs.header); err != nil {
		return fmt.Errorf("failed to unmarshal root: %w", err)
	}

	yfs.blockSize = yfs.header.BlockSize
	yfs.checksumEnabled = yfs.header.ChecksumEnabled > 0

	// Load bitmap
	if err := yfs.loadBitmap(); err != nil {
		return err
	}

	return nil
}

// loadBitmap loads the block bitmap from disk
func (yfs *YFS) loadBitmap() error {
	data, err := os.ReadFile(yfs.bitmapPath)
	if err != nil {
		return fmt.Errorf("failed to read bitmap file: %w", err)
	}

	if len(data) < 8 {
		return fmt.Errorf("invalid bitmap file format")
	}

	totalBlocks := binary.LittleEndian.Uint64(data[:8])
	bitmapData := data[8:]

	yfs.bitmap = &BlockBitmap{
		data:        bitmapData,
		totalBlocks: totalBlocks,
		searchPos:   0,
	}

	return nil
}

// saveBitmap saves the block bitmap to disk
func (yfs *YFS) saveBitmap() error {
	yfs.bitmap.mutex.Lock()
	defer yfs.bitmap.mutex.Unlock()

	if !yfs.bitmap.dirty {
		return nil
	}

	file, err := os.Create(yfs.bitmapPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write total blocks count
	header := make([]byte, 8)
	binary.LittleEndian.PutUint64(header, yfs.bitmap.totalBlocks)
	if _, err := file.Write(header); err != nil {
		return err
	}

	// Write bitmap data
	if _, err := file.Write(yfs.bitmap.data); err != nil {
		return err
	}

	yfs.bitmap.dirty = false
	return nil
}

// saveRoot saves the root directory to disk
func (yfs *YFS) saveRoot() error {
	if yfs.checksumEnabled {
		yfs.updateMetadataChecksum(yfs.header.Root.Metadata)
	}

	data, err := proto.Marshal(yfs.header)
	if err != nil {
		return fmt.Errorf("failed to marshal root: %w", err)
	}

	return os.WriteFile(yfs.rootPath, data, 0644)
}

// createEmptyBlocksFile creates the blocks file with header
func (yfs *YFS) createEmptyBlocksFile() error {
	file, err := os.Create(yfs.blocksPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write header (block size as uint32)
	header := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(header, yfs.blockSize)
	_, err = file.Write(header)
	return err
}

// updateMetadataChecksum updates the CRC32 checksum for metadata
func (yfs *YFS) updateMetadataChecksum(metadata *FileMetadata) {
	if !yfs.checksumEnabled {
		return
	}

	// Create a string representation for checksum calculation
	data := fmt.Sprintf("%s%d%d%d", metadata.Name, metadata.ModTime,
		metadata.CreateTime, metadata.Permissions)
	metadata.Crc32 = crc32.ChecksumIEEE([]byte(data))
}

// verifyMetadataChecksum verifies the CRC32 checksum for metadata
func (yfs *YFS) verifyMetadataChecksum(metadata *FileMetadata) bool {
	if !yfs.checksumEnabled {
		return true
	}

	data := fmt.Sprintf("%s%d%d%d", metadata.Name, metadata.ModTime,
		metadata.CreateTime, metadata.Permissions)
	expected := crc32.ChecksumIEEE([]byte(data))
	return metadata.Crc32 == expected
}

// calculateBlockOffset calculates the offset for a given block ID
func (yfs *YFS) calculateBlockOffset(blockID uint32) int64 {
	if blockID == NullBlockID {
		return -1
	}
	return int64(HeaderSize) + int64(yfs.blockSize)*int64(blockID-1)
}

// allocateBlocks allocates multiple contiguous blocks using intelligent bitmap search
func (yfs *YFS) allocateBlocks(count uint32) ([]uint32, error) {
	yfs.bitmap.mutex.Lock()
	defer yfs.bitmap.mutex.Unlock()

	if count == 0 {
		return nil, nil
	}

	var allocatedBlocks []uint32

	// Try to find contiguous blocks starting from last search position
	startPos := yfs.bitmap.searchPos
	for i := uint64(0); i < yfs.bitmap.totalBlocks; i++ {
		pos := (startPos + i) % yfs.bitmap.totalBlocks

		if yfs.isBlockFree(pos) {
			// Check if we can allocate 'count' contiguous blocks from here
			canAllocate := true
			for j := uint32(0); j < count; j++ {
				if pos+uint64(j) >= yfs.bitmap.totalBlocks || !yfs.isBlockFree(pos+uint64(j)) {
					canAllocate = false
					break
				}
			}

			if canAllocate {
				// Allocate the blocks
				for j := uint32(0); j < count; j++ {
					blockID := uint32(pos + uint64(j) + 1) // Block IDs are 1-based
					yfs.markBlockUsed(pos + uint64(j))
					allocatedBlocks = append(allocatedBlocks, blockID)
				}
				yfs.bitmap.searchPos = pos + uint64(count)
				yfs.bitmap.dirty = true
				return allocatedBlocks, nil
			}
		}
	}

	// If we couldn't find contiguous blocks, try to allocate individual blocks
	if count > 1 {
		return yfs.allocateBlocksIndividual(count)
	}

	return nil, fmt.Errorf("no free blocks available")
}

// allocateBlocksIndividual allocates blocks individually when contiguous allocation fails
func (yfs *YFS) allocateBlocksIndividual(count uint32) ([]uint32, error) {
	var allocatedBlocks []uint32

	for allocated := uint32(0); allocated < count; allocated++ {
		for i := uint64(0); i < yfs.bitmap.totalBlocks; i++ {
			pos := (yfs.bitmap.searchPos + i) % yfs.bitmap.totalBlocks

			if yfs.isBlockFree(pos) {
				blockID := uint32(pos + 1) // Block IDs are 1-based
				yfs.markBlockUsed(pos)
				allocatedBlocks = append(allocatedBlocks, blockID)
				yfs.bitmap.searchPos = pos + 1
				break
			}
		}
	}

	if len(allocatedBlocks) != int(count) {
		// Free any allocated blocks and return error
		for _, blockID := range allocatedBlocks {
			yfs.markBlockFree(uint64(blockID - 1))
		}
		return nil, fmt.Errorf("could not allocate %d blocks", count)
	}

	yfs.bitmap.dirty = true
	return allocatedBlocks, nil
}

// isBlockFree checks if a block is free in the bitmap
func (yfs *YFS) isBlockFree(blockPos uint64) bool {
	byteIndex := blockPos / 8
	bitIndex := blockPos % 8

	if byteIndex >= uint64(len(yfs.bitmap.data)) {
		return false
	}

	return (yfs.bitmap.data[byteIndex] & (1 << bitIndex)) == 0
}

// markBlockUsed marks a block as used in the bitmap
func (yfs *YFS) markBlockUsed(blockPos uint64) {
	byteIndex := blockPos / 8
	bitIndex := blockPos % 8

	// Expand bitmap if necessary
	for byteIndex >= uint64(len(yfs.bitmap.data)) {
		yfs.bitmap.data = append(yfs.bitmap.data, make([]byte, 1024)...)
		yfs.bitmap.totalBlocks += 8192
	}

	yfs.bitmap.data[byteIndex] |= (1 << bitIndex)
}

// markBlockFree marks a block as free in the bitmap
func (yfs *YFS) markBlockFree(blockPos uint64) {
	byteIndex := blockPos / 8
	bitIndex := blockPos % 8

	if byteIndex < uint64(len(yfs.bitmap.data)) {
		yfs.bitmap.data[byteIndex] &^= (1 << bitIndex)
		yfs.bitmap.dirty = true
	}
}

// freeBlocks frees multiple blocks in the bitmap
func (yfs *YFS) freeBlocks(blockIDs []uint32) error {
	yfs.bitmap.mutex.Lock()
	defer yfs.bitmap.mutex.Unlock()

	for _, blockID := range blockIDs {
		if blockID != NullBlockID {
			yfs.markBlockFree(uint64(blockID - 1)) // Convert to 0-based
		}
	}

	yfs.bitmap.dirty = true
	return nil
}

// writeBlock writes data to a specific block
func (yfs *YFS) writeBlock(blockID uint32, data []byte) error {
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

	// Prepare block data (pad or truncate to block size)
	blockData := make([]byte, yfs.blockSize)

	if len(data) > int(yfs.blockSize)-4 {
		return fmt.Errorf("data exceeds block size limit: %d bytes, max: %d bytes", len(data), yfs.blockSize-4)
	}

	// Write length as first 4 bytes
	binary.LittleEndian.PutUint32(blockData[0:4], uint32(len(data)))

	// Copy actual data after the length header
	copy(blockData[4:], data)

	_, err = file.Write(blockData)
	return err
}

// readBlock reads data from a specific block
func (yfs *YFS) readBlock(blockID uint32) ([]byte, error) {
	file, err := os.Open(yfs.blocksPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	offset := yfs.calculateBlockOffset(blockID)
	if offset < 0 {
		return nil, fmt.Errorf("invalid block ID: %d", blockID)
	}

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	blockData := make([]byte, yfs.blockSize)
	_, err = file.Read(blockData)
	if err != nil {
		return nil, err
	}

	// REad the actual data length from the first 4 bytes
	dataLength := binary.LittleEndian.Uint32(blockData[0:4])

	// Validate length
	if dataLength > uint32(len(blockData)-4) {
		return nil, fmt.Errorf("invalid data length: %d bytes, max: %d bytes", dataLength, len(blockData)-4)
	}

	return blockData[4 : 4+dataLength], nil
}

// writeIndexBlock writes an index block to disk
func (yfs *YFS) writeIndexBlock(blockID uint32, indexBlock *IndexBlock) error {
	if yfs.checksumEnabled {
		// Calculate checksum for index block
		data := fmt.Sprintf("%v%v%d", indexBlock.BlockIds, indexBlock.Extents,
			indexBlock.NextIndexBlockId)
		indexBlock.Crc32 = crc32.ChecksumIEEE([]byte(data))
	}

	data, err := proto.Marshal(indexBlock)
	if err != nil {
		return fmt.Errorf("failed to marshal index block: %w", err)
	}

	return yfs.writeBlock(blockID, data)
}

// readIndexBlock reads an index block from disk
func (yfs *YFS) readIndexBlock(blockID uint32) (*IndexBlock, error) {
	data, err := yfs.readBlock(blockID)
	if err != nil {
		return nil, err
	}

	indexBlock := &IndexBlock{}
	if err := proto.Unmarshal(data, indexBlock); err != nil {
		return nil, fmt.Errorf("failed to unmarshal index block: %w", err)
	}

	// Verify checksum if enabled
	if yfs.checksumEnabled && indexBlock.Crc32 != 0 {
		checkData := fmt.Sprintf("%v%v%d", indexBlock.BlockIds, indexBlock.Extents,
			indexBlock.NextIndexBlockId)
		expected := crc32.ChecksumIEEE([]byte(checkData))
		if indexBlock.Crc32 != expected {
			return nil, fmt.Errorf("index block checksum mismatch")
		}
	}

	return indexBlock, nil
}

// writeFileToBlocks writes file data using the new index block system
func (yfs *YFS) writeFileToBlocks(data []byte, existingFirstIndexBlockID uint32) (uint32, error) {
	if len(data) == 0 {
		// Free existing blocks if any
		if existingFirstIndexBlockID != NullBlockID {
			yfs.freeFileBlocks(existingFirstIndexBlockID)
		}
		return NullBlockID, nil
	}

	// Calculate how many data blocks we need
	blocksNeeded := (len(data) + int(yfs.blockSize) - 1) / int(yfs.blockSize)

	// Allocate data blocks
	dataBlocks, err := yfs.allocateBlocks(uint32(blocksNeeded))
	if err != nil {
		return NullBlockID, err
	}

	// Write data to blocks
	for i, blockID := range dataBlocks {
		start := i * int(yfs.blockSize)
		end := start + int(yfs.blockSize)
		if end > len(data) {
			end = len(data)
		}

		if err := yfs.writeBlock(blockID, data[start:end]); err != nil {
			yfs.freeBlocks(dataBlocks)
			return NullBlockID, err
		}
	}

	// Create index blocks to point to data blocks
	firstIndexBlockID, err := yfs.createIndexBlocks(dataBlocks)
	if err != nil {
		yfs.freeBlocks(dataBlocks)
		return NullBlockID, err
	}

	// Free old blocks if they existed
	if existingFirstIndexBlockID != NullBlockID {
		yfs.freeFileBlocks(existingFirstIndexBlockID)
	}

	return firstIndexBlockID, nil
}

// createIndexBlocks creates index blocks for a list of data blocks
func (yfs *YFS) createIndexBlocks(dataBlocks []uint32) (uint32, error) {
	if len(dataBlocks) == 0 {
		return NullBlockID, nil
	}

	var indexBlocks []uint32

	// Create index blocks
	for i := 0; i < len(dataBlocks); i += MaxBlocksPerIndex {
		end := i + MaxBlocksPerIndex
		if end > len(dataBlocks) {
			end = len(dataBlocks)
		}

		// Allocate block for this index
		indexBlockIDs, err := yfs.allocateBlocks(1)
		if err != nil {
			yfs.freeBlocks(indexBlocks)
			return NullBlockID, err
		}
		indexBlockID := indexBlockIDs[0]
		indexBlocks = append(indexBlocks, indexBlockID)

		// Create index block content
		blockIDs := dataBlocks[i:end]
		indexBlock := &IndexBlock{
			BlockIds: blockIDs,
			DataSize: uint32(len(blockIDs) * int(yfs.blockSize)),
		}

		// Link to next index block if there are more
		if end < len(dataBlocks) {
			// We'll set this after creating the next index block
		}

		if err := yfs.writeIndexBlock(indexBlockID, indexBlock); err != nil {
			yfs.freeBlocks(indexBlocks)
			return NullBlockID, err
		}
	}

	// Link index blocks together
	for i := 0; i < len(indexBlocks)-1; i++ {
		indexBlock, err := yfs.readIndexBlock(indexBlocks[i])
		if err != nil {
			return NullBlockID, err
		}
		indexBlock.NextIndexBlockId = indexBlocks[i+1]
		if err := yfs.writeIndexBlock(indexBlocks[i], indexBlock); err != nil {
			return NullBlockID, err
		}
	}

	return indexBlocks[0], nil
}

// readFileFromBlocks reads file data using the index block system
func (yfs *YFS) readFileFromBlocks(firstIndexBlockID uint32, fileSize int64) ([]byte, error) {
	if firstIndexBlockID == NullBlockID || fileSize == 0 {
		return []byte{}, nil
	}

	var result []byte
	currentIndexBlockID := firstIndexBlockID
	bytesRead := int64(0)

	for currentIndexBlockID != NullBlockID && bytesRead < fileSize {
		indexBlock, err := yfs.readIndexBlock(currentIndexBlockID)
		if err != nil {
			return nil, err
		}

		// Read data from blocks referenced by this index block
		for _, blockID := range indexBlock.BlockIds {
			if bytesRead >= fileSize {
				break
			}

			blockData, err := yfs.readBlock(blockID)
			if err != nil {
				return nil, err
			}

			// Calculate how much data to take from this block
			remainingBytes := fileSize - bytesRead
			bytesToTake := int64(len(blockData))
			if bytesToTake > remainingBytes {
				bytesToTake = remainingBytes
			}

			result = append(result, blockData[:bytesToTake]...)
			bytesRead += bytesToTake
		}

		currentIndexBlockID = indexBlock.NextIndexBlockId
	}

	return result, nil
}

// freeFileBlocks frees all blocks associated with a file (index and data blocks)
func (yfs *YFS) freeFileBlocks(firstIndexBlockID uint32) error {
	currentIndexBlockID := firstIndexBlockID

	for currentIndexBlockID != NullBlockID {
		indexBlock, err := yfs.readIndexBlock(currentIndexBlockID)
		if err != nil {
			return err
		}

		// Free data blocks
		if err := yfs.freeBlocks(indexBlock.BlockIds); err != nil {
			return err
		}

		// Free extent blocks
		for _, extent := range indexBlock.Extents {
			var blockIDs []uint32
			for i := uint32(0); i < extent.BlockCount; i++ {
				blockIDs = append(blockIDs, extent.StartBlockId+i)
			}
			if err := yfs.freeBlocks(blockIDs); err != nil {
				return err
			}
		}

		nextIndexBlockID := indexBlock.NextIndexBlockId

		// Free the index block itself
		if err := yfs.freeBlocks([]uint32{currentIndexBlockID}); err != nil {
			return err
		}

		currentIndexBlockID = nextIndexBlockID
	}

	return nil
}

// findEntryUnsafe finds a file or directory entry by path (without locks)
// This should only be called when the caller already holds the appropriate lock
func (yfs *YFS) findEntryUnsafe(path string) (*DirectoryEntry, *FileEntry, bool, error) {
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

// findEntry finds a file or directory entry by path
func (yfs *YFS) findEntry(path string) (*DirectoryEntry, *FileEntry, bool, error) {
	yfs.mutex.RLock()
	defer yfs.mutex.RUnlock()
	return yfs.findEntryUnsafe(path)
}

// WriteFile creates or updates a file
func (yfs *YFS) WriteFile(path string, data []byte) error {
	yfs.mutex.Lock()
	defer yfs.mutex.Unlock()

	_, file, isDir, err := yfs.findEntryUnsafe(path)
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
	parentDir, _, _, err := yfs.findEntryUnsafe(parentPath)
	if err != nil {
		if err := yfs.createDirectoryChain(parentPath); err != nil {
			return err
		}
		parentDir, _, _, _ = yfs.findEntryUnsafe(parentPath)
	}

	var existingFirstIndexBlockID uint32
	if file != nil {
		existingFirstIndexBlockID = file.FirstIndexBlockId
	}

	// Write data to blocks
	firstIndexBlockID, err := yfs.writeFileToBlocks(data, existingFirstIndexBlockID)
	if err != nil {
		return err
	}

	now := time.Now().Unix()

	// Update or create file entry
	if file == nil {
		file = &FileEntry{
			Metadata: &FileMetadata{
				Name:       fileName,
				ModTime:    now,
				CreateTime: now,
			},
			FirstIndexBlockId: firstIndexBlockID,
			Size:              int64(len(data)),
		}

		if parentDir.Files == nil {
			parentDir.Files = make(map[string]*FileEntry)
		}

		parentDir.Files[fileName] = file
	} else {
		file.FirstIndexBlockId = firstIndexBlockID
		file.Size = int64(len(data))
		file.Metadata.ModTime = now
	}

	// Update checksums
	yfs.updateMetadataChecksum(file.Metadata)

	// Save changes
	if err := yfs.saveRoot(); err != nil {
		return err
	}

	return yfs.saveBitmap()
}

// ReadFile reads a file's contents
func (yfs *YFS) ReadFile(path string) ([]byte, error) {
	yfs.mutex.RLock()
	defer yfs.mutex.RUnlock()

	_, file, isDir, err := yfs.findEntry(path)
	if err != nil {
		return nil, err
	}

	if isDir || file == nil {
		return nil, fmt.Errorf("file not found: %s", path)
	}

	// Verify metadata checksum
	if !yfs.verifyMetadataChecksum(file.Metadata) {
		return nil, fmt.Errorf("metadata checksum verification failed for file: %s", path)
	}

	return yfs.readFileFromBlocks(file.FirstIndexBlockId, file.Size)
}

// DeleteFile deletes a file
func (yfs *YFS) DeleteFile(path string) error {
	yfs.mutex.Lock()
	defer yfs.mutex.Unlock()

	parentDir, file, isDir, err := yfs.findEntryUnsafe(path)
	if err != nil {
		return err
	}

	if isDir || file == nil {
		return fmt.Errorf("file not found: %s", path)
	}

	// Free all blocks associated with the file
	if err := yfs.freeFileBlocks(file.FirstIndexBlockId); err != nil {
		return err
	}

	// Remove from index
	pathParts := strings.Split(strings.Trim(path, "/"), "/")
	fileName := pathParts[len(pathParts)-1]
	delete(parentDir.Files, fileName)

	// Save changes
	if err := yfs.saveRoot(); err != nil {
		return err
	}

	return yfs.saveBitmap()
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
			now := time.Now().Unix()
			newDir := &DirectoryEntry{
				Metadata: &FileMetadata{
					Name:       part,
					ModTime:    now,
					CreateTime: now,
				},
				Files:       make(map[string]*FileEntry),
				Directories: make(map[string]*DirectoryEntry),
			}
			yfs.updateMetadataChecksum(newDir.Metadata)
			currentDir.Directories[part] = newDir
		}
		currentDir = currentDir.Directories[part]
	}

	return nil
}

// CreateDirectory creates a new directory
func (yfs *YFS) CreateDirectory(path string) error {
	yfs.mutex.Lock()
	defer yfs.mutex.Unlock()

	_, _, isDir, err := yfs.findEntryUnsafe(path)
	if err == nil && isDir {
		return fmt.Errorf("directory already exists: %s", path)
	}

	if err := yfs.createDirectoryChain(path); err != nil {
		return err
	}

	return yfs.saveRoot()
}

// DeleteDirectory deletes an empty directory
func (yfs *YFS) DeleteDirectory(path string) error {
	yfs.mutex.Lock()
	defer yfs.mutex.Unlock()

	dir, _, isDir, err := yfs.findEntryUnsafe(path)
	if err != nil {
		return err
	}

	if !isDir {
		return fmt.Errorf("path is not a directory: %s", path)
	}

	if len(dir.Files) > 0 || len(dir.Directories) > 0 {
		return fmt.Errorf("directory not empty: %s", path)
	}

	// Find parent directory and remove this directory
	pathParts := strings.Split(strings.Trim(path, "/"), "/")
	if len(pathParts) == 1 && pathParts[0] == "" {
		return fmt.Errorf("cannot delete root directory")
	}

	parentPath := strings.Join(pathParts[:len(pathParts)-1], "/")
	parentDir, _, _, err := yfs.findEntry(parentPath)
	if err != nil {
		return err
	}

	dirName := pathParts[len(pathParts)-1]
	delete(parentDir.Directories, dirName)

	return yfs.saveRoot()
}

// Ls lists files and directories in a path
func (yfs *YFS) Ls(path string) ([]FileInfo, error) {
	yfs.mutex.RLock()
	defer yfs.mutex.RUnlock()

	dir, _, isDir, err := yfs.findEntryUnsafe(path)
	if err != nil {
		return nil, err
	}

	if !isDir {
		return nil, fmt.Errorf("path is not a directory: %s", path)
	}

	var entries []FileInfo

	// Add directories
	for name, subDir := range dir.Directories {
		entries = append(entries, FileInfo{
			Name:        name,
			IsDirectory: true,
			ModTime:     time.Unix(subDir.Metadata.ModTime, 0),
			CreateTime:  time.Unix(subDir.Metadata.CreateTime, 0),
		})
	}

	// Add files
	for name, file := range dir.Files {
		entries = append(entries, FileInfo{
			Name:        name,
			IsDirectory: false,
			Size:        file.Size,
			ModTime:     time.Unix(file.Metadata.ModTime, 0),
			CreateTime:  time.Unix(file.Metadata.CreateTime, 0),
			BlockCount:  file.DataBlockCount,
		})
	}

	return entries, nil
}

// LsAll returns the complete directory tree
func (yfs *YFS) LsAll() (*FileInfo, error) {
	yfs.mutex.RLock()
	defer yfs.mutex.RUnlock()

	return yfs.buildDirectoryTree(yfs.header.Root, "/"), nil
}

// buildDirectoryTree recursively builds the directory tree
func (yfs *YFS) buildDirectoryTree(dir *DirectoryEntry, path string) *FileInfo {
	entry := &FileInfo{
		Name:        filepath.Base(path),
		IsDirectory: true,
		ModTime:     time.Unix(dir.Metadata.ModTime, 0),
		CreateTime:  time.Unix(dir.Metadata.CreateTime, 0),
	}

	return entry
}

// GetFileInfo returns information about a specific file or directory
func (yfs *YFS) GetFileInfo(path string) (*FileInfo, error) {
	yfs.mutex.RLock()
	defer yfs.mutex.RUnlock()

	dir, file, isDir, err := yfs.findEntryUnsafe(path)
	if err != nil {
		return nil, err
	}

	if isDir {
		return &FileInfo{
			Name:        filepath.Base(path),
			IsDirectory: true,
			ModTime:     time.Unix(dir.Metadata.ModTime, 0),
			CreateTime:  time.Unix(dir.Metadata.CreateTime, 0),
		}, nil
	}

	if file == nil {
		return nil, fmt.Errorf("file not found: %s", path)
	}

	return &FileInfo{
		Name:        file.Metadata.Name,
		IsDirectory: false,
		Size:        file.Size,
		ModTime:     time.Unix(file.Metadata.ModTime, 0),
		CreateTime:  time.Unix(file.Metadata.CreateTime, 0),
		BlockCount:  file.DataBlockCount,
	}, nil
}

// GetBlockSize returns the current block size
func (yfs *YFS) GetBlockSize() uint32 {
	return yfs.blockSize
}

// GetStats returns file system statistics
func (yfs *YFS) GetStats() (map[string]interface{}, error) {
	yfs.mutex.RLock()
	defer yfs.mutex.RUnlock()

	// Count used blocks in bitmap
	usedBlocks := uint64(0)
	for i := uint64(0); i < yfs.bitmap.totalBlocks; i++ {
		if !yfs.isBlockFree(i) {
			usedBlocks++
		}
	}

	// Get blocks file size
	blocksFile, err := os.Open(yfs.blocksPath)
	if err != nil {
		return nil, err
	}
	defer blocksFile.Close()

	blocksStat, err := blocksFile.Stat()
	if err != nil {
		return nil, err
	}

	allocatedBlocks := (blocksStat.Size() - int64(HeaderSize)) / int64(yfs.blockSize)

	stats := map[string]interface{}{
		"version":           yfs.header.Version,
		"block_size":        yfs.blockSize,
		"total_blocks":      yfs.bitmap.totalBlocks,
		"allocated_blocks":  allocatedBlocks,
		"used_blocks":       usedBlocks,
		"free_blocks":       yfs.bitmap.totalBlocks - usedBlocks,
		"checksum_enabled":  yfs.checksumEnabled,
		"bitmap_search_pos": yfs.bitmap.searchPos,
		"blocks_file_size":  blocksStat.Size(),
	}

	return stats, nil
}

// Defragment performs file system defragmentation (basic implementation)
func (yfs *YFS) Defragment() error {
	yfs.mutex.Lock()
	defer yfs.mutex.Unlock()

	// This is a simplified defragmentation that resets the bitmap search position
	// A full implementation would reorganize blocks to be more contiguous
	yfs.bitmap.searchPos = 0
	yfs.bitmap.dirty = true

	return yfs.saveBitmap()
}

// Sync ensures all pending changes are written to disk
func (yfs *YFS) Sync() error {
	yfs.mutex.Lock()
	defer yfs.mutex.Unlock()

	if err := yfs.saveRoot(); err != nil {
		return err
	}

	return yfs.saveBitmap()
}

// Close closes the file system and ensures all changes are saved
func (yfs *YFS) Close() error {
	return yfs.Sync()
}

// VerifyIntegrity performs basic integrity checks on the file system
func (yfs *YFS) VerifyIntegrity() error {
	yfs.mutex.RLock()
	defer yfs.mutex.RUnlock()

	// Verify root directory metadata
	if !yfs.verifyMetadataChecksum(yfs.header.Root.Metadata) {
		return fmt.Errorf("root directory metadata checksum verification failed")
	}

	// Recursively verify all file and directory metadata
	return yfs.verifyDirectoryIntegrity(yfs.header.Root, "/")
}

// verifyDirectoryIntegrity recursively verifies directory integrity
func (yfs *YFS) verifyDirectoryIntegrity(dir *DirectoryEntry, path string) error {
	// Verify directory metadata
	if !yfs.verifyMetadataChecksum(dir.Metadata) {
		return fmt.Errorf("directory metadata checksum verification failed: %s", path)
	}

	// Verify files in this directory
	for name, file := range dir.Files {
		if !yfs.verifyMetadataChecksum(file.Metadata) {
			return fmt.Errorf("file metadata checksum verification failed: %s/%s", path, name)
		}

		// Verify index blocks for this file
		if err := yfs.verifyFileIndexBlocks(file.FirstIndexBlockId); err != nil {
			return fmt.Errorf("file index block verification failed for %s/%s: %w", path, name, err)
		}
	}

	// Recursively verify subdirectories
	for name, subDir := range dir.Directories {
		subPath := filepath.Join(path, name)
		if err := yfs.verifyDirectoryIntegrity(subDir, subPath); err != nil {
			return err
		}
	}

	return nil
}

// verifyFileIndexBlocks verifies the integrity of a file's index blocks
func (yfs *YFS) verifyFileIndexBlocks(firstIndexBlockID uint32) error {
	if firstIndexBlockID == NullBlockID {
		return nil
	}

	currentIndexBlockID := firstIndexBlockID
	visitedBlocks := make(map[uint32]bool)

	for currentIndexBlockID != NullBlockID {
		// Check for circular references
		if visitedBlocks[currentIndexBlockID] {
			return fmt.Errorf("circular reference detected in index blocks")
		}
		visitedBlocks[currentIndexBlockID] = true

		// Read and verify index block
		indexBlock, err := yfs.readIndexBlock(currentIndexBlockID)
		if err != nil {
			return fmt.Errorf("failed to read index block %d: %w", currentIndexBlockID, err)
		}

		// Verify that referenced data blocks are within valid range
		for _, blockID := range indexBlock.BlockIds {
			if blockID == NullBlockID || blockID > uint32(yfs.bitmap.totalBlocks) {
				return fmt.Errorf("invalid data block ID %d in index block %d", blockID, currentIndexBlockID)
			}
		}

		// Verify extents
		for _, extent := range indexBlock.Extents {
			if extent.StartBlockId == NullBlockID ||
				extent.StartBlockId+extent.BlockCount > uint32(yfs.bitmap.totalBlocks) {
				return fmt.Errorf("invalid extent in index block %d: start=%d, count=%d",
					currentIndexBlockID, extent.StartBlockId, extent.BlockCount)
			}
		}

		currentIndexBlockID = indexBlock.NextIndexBlockId
	}

	return nil
}
