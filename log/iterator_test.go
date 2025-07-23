package log

import (
	"bytes"
	"testing"
)

func TestIterator_Next(t *testing.T) {
	t.Run("iterates within a single block", func(t *testing.T) {
		const blockSize = 400
		_, logManager, _ := setup(t, blockSize)

		// Append a few logs that will fit in one block
		logs := [][]byte{
			[]byte("log one"),
			[]byte("log two"),
			[]byte("log three"),
		}
		for _, log := range logs {
			if _, err := logManager.Append(log); err != nil {
				t.Fatalf("failed to append log: %v", err)
			}
		}

		// Get an iterator. This also flushes the logs to disk.
		iterator, err := logManager.Iterator()
		if err != nil {
			t.Fatalf("failed to create iterator: %v", err)
		}

		// Iterate backwards and check each log
		for i := len(logs) - 1; i >= 0; i-- {
			if !iterator.HasNext() {
				t.Fatalf("HasNext() returned false unexpectedly for log %d", i)
			}
			retrievedLog, err := iterator.Next()
			if err != nil {
				t.Fatalf("Next() returned an error for log %d: %v", i, err)
			}
			if !bytes.Equal(retrievedLog, logs[i]) {
				t.Errorf("retrieved log mismatch. got %q, want %q", retrievedLog, logs[i])
			}
		}

		// After retrieving all logs, HasNext should be false
		if iterator.HasNext() {
			t.Error("HasNext() returned true after all logs were read")
		}
	})

	t.Run("iterates across multiple blocks", func(t *testing.T) {
		// Use a small block size to easily trigger an overflow
		const blockSize = 100
		_, logManager, _ := setup(t, blockSize)

		// Log 1 almost fills the first block (block 0)
		// Block size 100. Boundary int takes 4 bytes.
		// Log length int takes 4 bytes. So, 100 - 4 - 4 = 92 bytes available for log data.
		log1 := make([]byte, 80)
		log1[0] = 'A' // Mark it for identification

		// Log 2 will cause an overflow into a new block (block 1)
		log2 := make([]byte, 30)
		log2[0] = 'B'

		// Log 3 will be in the same block as log 2 (block 1)
		log3 := make([]byte, 25)
		log3[0] = 'C'

		logs := [][]byte{log1, log2, log3}
		for _, log := range logs {
			if _, err := logManager.Append(log); err != nil {
				t.Fatalf("failed to append log: %v", err)
			}
		}

		// Get an iterator. It will start at the end of the last block (block 1).
		iterator, err := logManager.Iterator()
		if err != nil {
			t.Fatalf("failed to create iterator: %v", err)
		}

		// Iterate backwards and check each log
		for i := len(logs) - 1; i >= 0; i-- {
			if !iterator.HasNext() {
				t.Fatalf("HasNext() returned false unexpectedly for log %d", i)
			}
			retrievedLog, err := iterator.Next()
			if err != nil {
				t.Fatalf("Next() returned an error for log %d: %v", i, err)
			}
			if !bytes.Equal(retrievedLog, logs[i]) {
				t.Errorf("retrieved log mismatch for log %d. got %q, want %q", i, retrievedLog, logs[i])
			}
		}

		// After retrieving all logs, HasNext should be false
		if iterator.HasNext() {
			t.Error("HasNext() returned true after all logs were read")
		}
	})
}
