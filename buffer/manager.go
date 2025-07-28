package buffer

import (
	"errors"
	"sync"
	"time"

	"simpledb/file"
	"simpledb/log"
)

type Manager struct {
	mu         sync.Mutex
	bufferPool []*Buffer
	available  int32
	cond       *sync.Cond // used to wait for a buffer to become available.
}

func NewManager(fileManager *file.Manager, logManager *log.Manager, numBufs int32) *Manager {
	m := &Manager{
		bufferPool: make([]*Buffer, numBufs),
		available:  numBufs,
	}
	m.cond = sync.NewCond(&m.mu)

	for i := range numBufs {
		m.bufferPool[i] = NewBuffer(fileManager, logManager)
	}

	return m
}

// Available returns the number of available (unpinned) buffers.
func (m *Manager) Available() int32 {
	return m.available
}

// FlushAll flushes all dirty buffers modified by the specified transaction.
func (m *Manager) FlushAll(txNum int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, buffer := range m.bufferPool {
		if buffer.ModifyingTx() == txNum {
			if err := buffer.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Manager) Unpin(buf *Buffer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	buf.unpin()
	if !buf.IsPinned() {
		m.available++
		// Wake up any waiting goroutines (in Pin) since a buffer is now free.
		m.cond.Broadcast()
	}
}

const maxWaitTime = 10 * time.Second

// ErrBufferTimeout is returned when a client's request for a buffer times out.
var ErrBufferTimeout = errors.New("buffer manager: request timed out")

// Pin pins a buffer for the specified block. The method blocks if no buffers
// are available, waiting up to a timeout period.
func (m *Manager) Pin(block *file.Block) (*Buffer, error) {
	m.mu.Lock()

	buf := m.tryToPin(block)
	if buf != nil {
		m.mu.Unlock()
		return buf, nil
	}

	// If tryToPin returns nil, no buffer is available. We must wait.
	done := make(chan struct{})
	go func() {
		// Wait for a signal from Unpin. `cond.Wait()` atomically unlocks the
		// mutex and waits, then re-locks it before returning.
		m.cond.Wait()
		close(done)
	}()

	select {
	case <-time.After(maxWaitTime):
		return nil, ErrBufferTimeout
	case <-done:
		// After waking up, try again to get a buffer.
		buf = m.tryToPin(block)
		m.mu.Unlock()
		return buf, nil
	}
}

// tryToPin attempts to pin a buffer for the specified block.
// It first looks for an existing buffer holding that block. If not found,
// it tries to find an unpinned buffer to use.
// This method must be called with the mutex lock already held.
func (m *Manager) tryToPin(block *file.Block) *Buffer {
	// First, try to find a buffer already assigned to this block.
	buf := m.findExistingBuffer(block)

	if buf == nil {
		// If no existing buffer, try to find a free one to replace.
		buf = m.chooseUnpinnedBuffer()
		if buf == nil {
			return nil // No buffers available (all are pinned).
		}
		// Assign the free buffer to the new block.
		buf.assignToBlock(block)
	}

	// If the chosen buffer was not pinned, it is now becoming pinned.
	if !buf.IsPinned() {
		m.available--
	}
	buf.pin()
	return buf
}

// findExistingBuffer searches the buffer pool for a buffer
// already allocated to the specified block.
// This method must be called with the mutex lock already held.
func (m *Manager) findExistingBuffer(block *file.Block) *Buffer {
	for _, buf := range m.bufferPool {
		b := buf.Block()
		if b != nil && b.Equals(block) {
			return buf
		}
	}
	return nil
}

// chooseUnpinnedBuffer finds an unpinned buffer in the pool.
// This simple version takes the first one it finds. A real system would
// use a replacement strategy (e.g., LRU).
// This method must be called with the mutex lock already held.
func (m *Manager) chooseUnpinnedBuffer() *Buffer {
	for _, buf := range m.bufferPool {
		if !buf.IsPinned() {
			return buf
		}
	}
	return nil
}
