package buffer

import (
	"errors"
	"testing"
	"time"

	"simpledb/file"
	"simpledb/log"
)

// setup creates a temporary directory and initializes file and log managers for testing.
func setup(t *testing.T) (*file.Manager, *log.Manager) {
	t.Helper()
	dir := t.TempDir()
	const blockSize = 400
	const logFile = "testlogfile"

	fm, err := file.NewManager(dir, blockSize)
	if err != nil {
		t.Fatalf("failed to create file manager: %v", err)
	}

	lm, err := log.NewManager(fm, logFile)
	if err != nil {
		t.Fatalf("failed to create log manager: %v", err)
	}

	return fm, lm
}

func TestManager_FlushAll(t *testing.T) {
	fm, lm := setup(t)
	const numBufs = 3
	bm := NewManager(fm, lm, numBufs)

	const txNum1 = 10
	const txNum2 = 20

	// 1. Prepare blocks and initial data.
	blk1 := file.NewBlock("testfile", 0)
	blk2 := file.NewBlock("testfile", 1)
	blk3 := file.NewBlock("testfile", 2)

	// Write initial data to blk2 to verify it's not overwritten by the flush of tx1.
	pInitial := file.NewPage(fm.BlockSize())
	pInitial.WriteStringAt(0, "initial data")
	if err := fm.Write(blk2, pInitial); err != nil {
		t.Fatalf("failed to write initial data to blk2: %v", err)
	}

	// 2. Pin and modify three buffers, assigning them to two different transactions.
	// Buffer 1 for txNum1
	buf1, err := bm.Pin(blk1)
	if err != nil {
		t.Fatalf("failed to pin block 1: %v", err)
	}
	buf1.Contents().WriteStringAt(10, "data for tx1-a")
	buf1.SetModified(txNum1, 1) // lsn=1

	// Buffer 2 for txNum2
	buf2, err := bm.Pin(blk2)
	if err != nil {
		t.Fatalf("failed to pin block 2: %v", err)
	}
	buf2.Contents().WriteStringAt(20, "data for tx2")
	buf2.SetModified(txNum2, 2) // lsn=2

	// Buffer 3 for txNum1
	buf3, err := bm.Pin(blk3)
	if err != nil {
		t.Fatalf("failed to pin block 3: %v", err)
	}
	buf3.Contents().WriteStringAt(30, "data for tx1-b")
	buf3.SetModified(txNum1, 3) // lsn=3

	// 3. Call FlushAll for txNum1. This should flush buf1 and buf3, but not buf2.
	if err := bm.FlushAll(txNum1); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}

	// 4. Verify the buffer states.
	if buf1.ModifyingTx() != -1 {
		t.Errorf("buffer 1 should be clean after flush, but is modified by tx %d", buf1.ModifyingTx())
	}
	if buf3.ModifyingTx() != -1 {
		t.Errorf("buffer 3 should be clean after flush, but is modified by tx %d", buf3.ModifyingTx())
	}
	if buf2.ModifyingTx() != txNum2 {
		t.Errorf("buffer 2 should still be dirty by tx %d, but got %d", txNum2, buf2.ModifyingTx())
	}

	// 5. Verify disk contents.
	pCheck := file.NewPage(fm.BlockSize())

	// Check blk1 (flushed) and blk3 (flushed)
	if err := fm.Read(blk1, pCheck); err != nil {
		t.Fatalf("failed to read block 1 for verification: %v", err)
	}
	if s, _ := pCheck.ReadStringAt(10); s != "data for tx1-a" {
		t.Errorf("disk content for block 1 is wrong: got %q, want %q", s, "data for tx1-a")
	}
	if err := fm.Read(blk3, pCheck); err != nil {
		t.Fatalf("failed to read block 3 for verification: %v", err)
	}
	if s, _ := pCheck.ReadStringAt(30); s != "data for tx1-b" {
		t.Errorf("disk content for block 3 is wrong: got %q, want %q", s, "data for tx1-b")
	}

	// Check blk2 (not flushed): should contain the initial data, not the modified data.
	if err := fm.Read(blk2, pCheck); err != nil {
		t.Fatalf("failed to read block 2 for verification: %v", err)
	}
	if s, _ := pCheck.ReadStringAt(0); s != "initial data" {
		t.Errorf("disk content for block 2 should be initial data, but got %q", s)
	}
	if s, _ := pCheck.ReadStringAt(20); s != "" {
		t.Errorf("disk content for block 2 should not contain tx2 data, but it does: %q", s)
	}
}

func TestManager_Pin(t *testing.T) {
	t.Run("Pin new blocks when buffers are available", func(t *testing.T) {
		fm, lm := setup(t)
		const numBufs = 1
		bm := NewManager(fm, lm, numBufs)

		blk := file.NewBlock("testfile", 1)

		buf, err := bm.Pin(blk)
		if err != nil {
			t.Fatalf("Failed to pin blk: %v", err)
		}
		if buf.Block() != blk {
			t.Errorf("Pinned buffer has wrong block, got %v, want %v", buf.Block(), blk)
		}
		if bm.Available() != numBufs-1 {
			t.Errorf("Available buffers should be %d, got %d", numBufs-1, bm.Available())
		}
	})

	t.Run("Pin an already pinned block", func(t *testing.T) {
		fm, lm := setup(t)
		const numBufs = 1
		bm := NewManager(fm, lm, numBufs)

		blk := file.NewBlock("testfile", 1)

		buf, err := bm.Pin(blk)
		if err != nil {
			t.Fatalf("Failed to pin blk: %v", err)
		}

		// We should get the same buffer instance back.
		// Note: Comparing pointers here is intentional.
		bufAgain, err := bm.Pin(blk) // Pin it once more to check pin count > 1
		if err != nil {
			t.Fatalf("Failed to re-pin blk: %v", err)
		}

		if bufAgain != buf {
			t.Errorf("Re-pinning a block should return the same buffer instance")
		}
		if bm.Available() != numBufs-1 {
			t.Errorf("Available buffers should still be %d after re-pin, got %d", numBufs-1, bm.Available())
		}
	})

	t.Run("Pin waits and succeeds when another client unpins a buffer", func(t *testing.T) {
		fm, lm := setup(t)
		const numBufs = 1
		bm := NewManager(fm, lm, numBufs)

		blk1 := file.NewBlock("testfile", 1)
		blk2 := file.NewBlock("testfile", 2)

		// Client A: Pin the only available buffer.
		buf1, err := bm.Pin(blk1)
		if err != nil {
			t.Fatalf("Client A failed to pin blk1: %v", err)
		}

		// Use a channel to get the result from the waiting client.
		type pinResult struct {
			buf *Buffer
			err error
		}
		pinResultChan := make(chan pinResult, 1)

		// Client B: Try to pin another block. This will wait.
		go func() {
			buf, err := bm.Pin(blk2)
			pinResultChan <- pinResult{buf, err}
		}()

		// Give the goroutine a moment to block on the condition variable.
		time.Sleep(20 * time.Millisecond)

		// Client A: Unpin the buffer, which should wake up Client B.
		bm.Unpin(buf1)

		// Wait for Client B to finish and check its result.
		select {
		case result := <-pinResultChan:
			if result.err != nil {
				t.Fatalf("Client B should have succeeded but failed: %v", result.err)
			}
			if !result.buf.Block().Equals(blk2) {
				t.Errorf("Client B pinned the wrong block, got %v, want %v", result.buf.Block(), blk2)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Test timed out waiting for Client B to acquire the buffer")
		}
	})

	t.Run("Pin fails with timeout when all buffers are pinned", func(t *testing.T) {
		fm, lm := setup(t)
		const numBufs = 1
		bm := NewManager(fm, lm, numBufs)

		blk1 := file.NewBlock("testfile", 1)
		blk2 := file.NewBlock("testfile", 2)

		_, err := bm.Pin(blk1)
		if err != nil {
			t.Fatalf("Failed to pin blk1: %v", err)
		}

		if bm.Available() != 0 {
			t.Errorf("Available buffers should be 0, got %d", bm.Available())
		}

		_, err = bm.Pin(blk2)
		if !errors.Is(err, ErrBufferTimeout) {
			t.Errorf("Expected error to be ErrBufferTimeout, but got %v", err)
		}
	})
}
