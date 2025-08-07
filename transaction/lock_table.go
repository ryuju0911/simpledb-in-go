package transaction

import (
	"errors"
	"sync"
	"time"

	"simpledb/file"
)

type LockTable struct {
	mu    sync.Mutex
	locks map[*file.Block]int32
	cond  *sync.Cond // used to wait for a block to become available.
}

func NewLockTable() *LockTable {
	lt := &LockTable{
		locks: make(map[*file.Block]int32),
	}
	lt.cond = sync.NewCond(&lt.mu)
	return lt
}

// maxWait defines the maximum time to wait for a lock.
const maxWaitTime = 10 * time.Second

// ErrLockAbort is returned when a lock request times out.
var ErrLockTimeout = errors.New("lock request aborted due to timeout")

// SLock grants a shared (read) lock on the specified block.
// It will wait for at most maxWait for the lock.
func (lt *LockTable) SLock(block *file.Block) error {
	lt.mu.Lock()

	if !lt.hasXLock(block) {
		lt.locks[block]++
		lt.mu.Unlock()
		return nil
	}

	done := make(chan struct{})
	go func() {
		lt.cond.Wait()
		close(done)
	}()

	timer := time.NewTimer(maxWaitTime)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			return ErrLockTimeout
		case <-done:
			if lt.hasXLock(block) {
				continue
			}

			lt.locks[block]++
			lt.mu.Unlock()
			return nil
		}
	}
}

// XLock grants an exclusive (write) lock on the specified block.
// It will wait for at most maxWait for the lock.
func (lt *LockTable) XLock(block *file.Block) error {
	lt.mu.Lock()

	// Concurrency manager always obtains an slock on the block before requesting the
	// xlock, and so a value higher than 1 indicates that some other transaction also has a
	// lock on this block.
	if !lt.hasOtherSLocks(block) {
		lt.locks[block] = -1
		lt.mu.Unlock()
		return nil
	}

	done := make(chan struct{})
	go func() {
		lt.cond.Wait()
		close(done)
	}()

	timer := time.NewTimer(maxWaitTime)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			return ErrLockTimeout
		case <-done:
			if lt.hasOtherSLocks(block) {
				continue
			}

			lt.locks[block] = -1
			lt.mu.Unlock()
			return nil
		}
	}
}

// Unlock releases a lock on the specified block.
// If this is the last lock on the block, it notifies other goroutines
// that may be waiting for a lock.
func (lt *LockTable) Unlock(block *file.Block) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	if lt.getLockValue(block) > 1 {
		// There are other shared locks, so just decrement the count.
		lt.locks[block]--
	} else {
		// This is the last shared lock or an exclusive lock.
		delete(lt.locks, block)
		lt.cond.Broadcast()
	}
}

// hasXlock checks if the block has an exclusive lock.
func (lt *LockTable) hasXLock(block *file.Block) bool {
	return lt.locks[block] < 0
}

// hasOtherSLocks checks if the block has more than one shared lock.
func (lt *LockTable) hasOtherSLocks(block *file.Block) bool {
	return lt.locks[block] > 1
}

// getLockVal returns the lock value for a block.
// 0 means no lock, >0 means shared lock count, <0 means exclusive lock.
func (lt *LockTable) getLockValue(block *file.Block) int32 {
	return lt.locks[block]
}
