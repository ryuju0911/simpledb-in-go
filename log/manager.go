package log

import (
	"sync"

	"simpledb/file"
)

type Manager struct {
	mu           sync.Mutex
	fileManager  *file.Manager
	logFile      string
	logPage      *file.Page
	currentBlock *file.Block
	latestLSN    int32
	lastSavedLSN int32
}

// NewManager creates a new log manager for a given log file.
// If the log file does not exist, it creates a new one with a single, empty block.
// If the log file exists, it reads the last block of the file into its internal
// log page. This setup allows new log records to be appended to the end of the
// existing log.
func NewManager(fileManager *file.Manager, logFile string) (*Manager, error) {
	logPage := file.NewPage(fileManager.BlockSize())
	logSize, err := fileManager.Size(logFile)
	if err != nil {
		return nil, err
	}

	var currentBlock *file.Block
	if logSize == 0 {
		currentBlock, err = appendNewBlock(fileManager, logFile, logPage)
		if err != nil {
			return nil, err
		}
	} else {
		currentBlock = file.NewBlock(logFile, logSize-1) // block number is 0-indexed
		err = fileManager.Read(currentBlock, logPage)
		if err != nil {
			return nil, err
		}
	}

	return &Manager{
		mu:           sync.Mutex{},
		fileManager:  fileManager,
		logFile:      logFile,
		logPage:      logPage,
		currentBlock: currentBlock,
		latestLSN:    0,
		lastSavedLSN: 0,
	}, nil
}

// Flush ensures that all log records with LSN values less than or equal to the
// specified LSN have been written to disk.
func (m *Manager) Flush(lsn int32) error {
	if lsn >= m.lastSavedLSN {
		return m.flush()
	}
	return nil
}

// Iterator returns a log iterator starting from the most recent log record.
// It ensures all current logs are flushed to disk before creating the iterator.
func (m *Manager) Iterator() (*Iterator, error) {
	err := m.flush()
	if err != nil {
		return nil, err
	}

	return NewIterator(m.fileManager, m.currentBlock)
}

// Append adds a new log record to the log file and returns its assigned LSN.
// It handles block switching if the log record doesn't fit in the current block
// and ensures proper synchronization for concurrent access.
func (m *Manager) Append(log []byte) (int32, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	boundary, err := m.logPage.ReadInt32At(0)
	if err != nil {
		return 0, err
	}

	needBytes := int32(len(log)) + 4
	if boundary-needBytes < 4 {
		// It doesn't fit, so move to the next block.
		err := m.flush()
		if err != nil {
			return 0, err
		}

		m.currentBlock, err = appendNewBlock(m.fileManager, m.logFile, m.logPage)
		if err != nil {
			return 0, err
		}

		boundary, err = m.logPage.ReadInt32At(0)
		if err != nil {
			return 0, err
		}
	}

	logPos := boundary - needBytes
	err = m.logPage.WriteBytesAt(logPos, log)
	if err != nil {
		return 0, err
	}

	err = m.logPage.WriteInt32At(0, logPos)
	if err != nil {
		return 0, err
	}

	m.latestLSN += 1

	return m.latestLSN, nil
}

func (m *Manager) flush() error {
	err := m.fileManager.Write(m.currentBlock, m.logPage)
	if err != nil {
		return err
	}
	m.lastSavedLSN = m.latestLSN
	return nil
}

func appendNewBlock(fileManager *file.Manager, logFile string, logPage *file.Page) (*file.Block, error) {
	block, err := fileManager.Append(logFile)
	if err != nil {
		return nil, err
	}

	err = logPage.WriteInt32At(0, fileManager.BlockSize())
	if err != nil {
		return nil, err
	}

	err = fileManager.Write(block, logPage)
	if err != nil {
		return nil, err
	}

	return block, nil
}
