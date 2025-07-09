package file

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Manager struct {
	mu        sync.Mutex
	directory string
	blockSize int32
	openFiles map[string]*os.File
}

func (m *Manager) BlockSize() int32 {
	return m.blockSize
}

// NewManager creates a new file manager for a given database directory.
// It creates the directory if it does not already exist.
// It also removes any temporary files that may have been leftover from
// previous database sessions.
func NewManager(directory string, blockSize int32) (*Manager, error) {
	// Create the directory if the database is new.
	err := os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		return nil, err
	}

	// Remove any leftover temporary tables.
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "temp") {
			err := os.Remove(filepath.Join(directory, entry.Name()))
			if err != nil {
				return nil, err
			}
		}
	}

	return &Manager{
		directory: directory,
		blockSize: blockSize,
		openFiles: make(map[string]*os.File),
	}, nil
}

// Read reads the contents of a disk block into a page.
// It is safe for concurrent use.
func (m *Manager) Read(block *Block, page *Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := m.getOpenFile(block.Filename())
	if err != nil {
		return err
	}

	if _, err := f.ReadAt(page.Buf(), int64(block.Number()*m.blockSize)); err != nil {
		return err
	}

	return nil
}

// Write writes the contents of a page to a disk block.
// It is safe for concurrent use.
func (m *Manager) Write(block *Block, page *Page) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, err := m.getOpenFile(block.Filename())
	if err != nil {
		return err
	}

	if _, err := f.WriteAt(page.Buf(), int64(block.Number()*m.blockSize)); err != nil {
		return err
	}

	return nil
}

// Append appends a new block to the end of the specified file.
// It calculates the new block number based on the current file size,
// extends the file by writing a block of zeros at that position, and
// returns a Block identifier for the new block.
// This method is safe for concurrent use.
func (m *Manager) Append(filename string) (*Block, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	size, err := m.Size(filename)
	if err != nil {
		return nil, err
	}

	block := NewBlock(filename, size)
	b := make([]byte, m.blockSize)

	f, err := m.getOpenFile(filename)
	if err != nil {
		return nil, err
	}

	if _, err := f.WriteAt(b, int64(block.Number()*m.blockSize)); err != nil {
		return nil, err
	}

	return block, nil
}

// Size returns the number of blocks in the specified file.
func (m *Manager) Size(filename string) (int32, error) {
	f, err := m.getOpenFile(filename)
	if err != nil {
		return 0, err
	}

	info, err := f.Stat()
	if err != nil {
		return 0, err
	}

	return int32(info.Size()) / m.blockSize, nil
}

// getOpenFile retrieves or creates a file handle for the specified filename.
// It first checks a cache of open files. If a handle is not found, it opens
// the file from the disk and adds the new handle to the cache.
func (m *Manager) getOpenFile(filename string) (*os.File, error) {
	if f, ok := m.openFiles[filename]; ok {
		return f, nil
	}

	path := filepath.Join(m.directory, filename)

	// The file is opened with flags os.O_RDWR|os.O_CREATE|os.O_SYNC.
	// The O_SYNC flag is critical for durability, as it ensures that every
	// write operation is immediately flushed to the disk, which is essential
	// for database recovery algorithms.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_SYNC, 0666)
	if err != nil {
		return nil, err
	}

	m.openFiles[filename] = f
	return f, nil
}
