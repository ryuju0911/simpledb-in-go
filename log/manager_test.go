package log

import (
	"testing"

	"simpledb/file"
)

func TestNewManager(t *testing.T) {
	const blockSize = 400
	const logFile = "testlogfile"

	t.Run("with a new log file", func(t *testing.T) {
		fileManager, _, _ := setup(t, blockSize)

		logManager, err := NewManager(fileManager, logFile)
		if err != nil {
			t.Fatalf("NewManager() with new log file failed: %v", err)
		}
		if logManager == nil {
			t.Fatal("NewManager() returned a nil manager")
		}

		// 1. Check if a new block was created
		size, err := fileManager.Size(logFile)
		if err != nil {
			t.Fatalf("fileManager.Size() failed: %v", err)
		}
		if size != 1 {
			t.Errorf("expected log file to have size 1 block, got %d", size)
		}

		// 2. Check if the current block is correct
		if logManager.currentBlock.Number() != 0 {
			t.Errorf("expected current block number to be 0, got %d", logManager.currentBlock.Number())
		}

		// 3. Check if the log page was initialized correctly
		// The boundary should be set to the block size.
		boundary, err := logManager.logPage.ReadInt32At(0)
		if err != nil {
			t.Fatalf("failed to read boundary from log page: %v", err)
		}
		if boundary != blockSize {
			t.Errorf("expected initial boundary to be %d, got %d", blockSize, boundary)
		}
	})

	t.Run("with an existing log file", func(t *testing.T) {
		fileManager, _, _ := setup(t, blockSize)

		// Manually create a log file with one block and some data
		// to simulate a previous run of the database.
		page := file.NewPage(blockSize)
		// Set the boundary to a custom value to verify it's read correctly.
		const boundaryOffset = 120
		page.WriteInt32At(0, boundaryOffset)
		page.WriteStringAt(boundaryOffset, "some old log data")

		// Write this page to the first block of the log file
		block := file.NewBlock(logFile, 0)
		if err := fileManager.Write(block, page); err != nil {
			t.Fatalf("failed to write initial log block: %v", err)
		}

		// Now, create the log manager. It should load this existing state.
		logManager, err := NewManager(fileManager, logFile)
		if err != nil {
			t.Fatalf("NewManager() with existing log file failed: %v", err)
		}

		// Check if the current block is the last one (block 0).
		if logManager.currentBlock.Number() != 0 {
			t.Errorf("expected current block number to be 0, got %d", logManager.currentBlock.Number())
		}

		// Check if the log page was loaded correctly from the file.
		boundary, err := logManager.logPage.ReadInt32At(0)
		if err != nil {
			t.Fatalf("failed to read boundary from log page: %v", err)
		}
		if boundary != boundaryOffset {
			t.Errorf("expected loaded boundary to be %d, got %d", boundaryOffset, boundary)
		}
	})
}

func TestLogManager_Append(t *testing.T) {
	t.Run("Append multiple logs", func(t *testing.T) {
		const blockSize = 400
		_, logManager, _ := setup(t, blockSize)

		// Append a few log records
		logs := [][]byte{
			[]byte("log record 1"),
			[]byte("another log"),
			[]byte("a third log entry"),
		}
		var lsns []int32
		for _, log := range logs {
			lsn, err := logManager.Append(log)
			if err != nil {
				t.Fatalf("failed to append log: %v", err)
			}
			lsns = append(lsns, lsn)
		}

		// Verify that LSNs are assigned sequentially
		for i := 1; i < len(lsns); i++ {
			if lsns[i] != lsns[i-1]+1 {
				t.Errorf("LSNs are not sequential: got %v", lsns)
			}
		}

		// Check that the latest LSN is correct
		if logManager.latestLSN != int32(len(logs)) {
			t.Errorf("latestLSN is incorrect: got %d, want %d", logManager.latestLSN, len(logs))
		}
	})

	t.Run("Append log causing block overflow", func(t *testing.T) {
		const blockSize = 100
		fileManager, logManager, logFile := setup(t, blockSize)

		// Create a log record that will fill most of the first block
		// (100 - 4 for boundary int - 4 for length int = 92)
		log1 := make([]byte, 80)
		lsn1, err := logManager.Append(log1)
		if err != nil {
			t.Fatalf("failed to append log1: %v", err)
		}
		if lsn1 != 1 {
			t.Errorf("expected lsn 1, got %d", lsn1)
		}

		// This log won't fit, triggering a new block
		log2 := make([]byte, 20)
		lsn2, err := logManager.Append(log2)
		if err != nil {
			t.Fatalf("failed to append log2: %v", err)
		}
		if lsn2 != 2 {
			t.Errorf("expected lsn 2, got %d", lsn2)
		}

		// Check that the log file size increased to 2 blocks
		size, err := fileManager.Size(logFile)
		if err != nil {
			t.Fatalf("failed to get log file size: %v", err)
		}
		if size != 2 {
			t.Errorf("log file should have 2 blocks after overflow, but has %d", size)
		}
	})
}

func TestLogManager_Flush(t *testing.T) {
	t.Run("Flush forces write to disk", func(t *testing.T) {
		blockSize := int32(400)
		_, lm, _ := setup(t, blockSize)

		if lm.lastSavedLSN != 0 {
			t.Fatalf("initial lastSavedLSN should be 0, got %d", lm.lastSavedLSN)
		}

		// Append a few logs
		lsn1, _ := lm.Append([]byte("log record 1"))
		lsn2, _ := lm.Append([]byte("another log"))

		if lm.latestLSN != 2 {
			t.Fatalf("latestLSN should be 2, got %d", lm.latestLSN)
		}

		// Before flush, lastSavedLSN is still the initial value
		// because flush is only called when a block fills up.
		if lm.lastSavedLSN != 0 {
			t.Errorf("lastSavedLSN should be 0 before flush, got %d", lm.lastSavedLSN)
		}

		// Flush logs up to the first LSN. This implementation flushes everything.
		err := lm.Flush(lsn1)
		if err != nil {
			t.Fatalf("failed to flush logs: %v", err)
		}

		// The flush operation should update lastSavedLSN to latestLSN
		if lm.lastSavedLSN != lm.latestLSN {
			t.Errorf("lastSavedLSN was not updated correctly: got %d, want %d", lm.lastSavedLSN, lm.latestLSN)
		}

		// Flushing an old LSN should do nothing
		err = lm.Flush(lsn1)
		if err != nil {
			t.Fatalf("failed to flush old LSN: %v", err)
		}

		// Appending another log
		_, _ = lm.Append([]byte("a third log"))

		// Flush again with the latest LSN
		err = lm.Flush(lsn2)
		if err != nil {
			t.Fatalf("failed to flush all logs: %v", err)
		}

		if lm.lastSavedLSN != 3 {
			t.Errorf("all logs not flushed: got %d, want %d", lm.lastSavedLSN, 3)
		}
	})
}

func TestLogManager_Iterator(t *testing.T) {
	t.Run("Iterate through logs", func(t *testing.T) {
		const blockSize = 400
		_, logManager, _ := setup(t, blockSize)

		// Append a few logs
		logs := [][]byte{
			[]byte("log record 1"),
			[]byte("another log"),
			[]byte("a third log entry"),
		}
		for _, log := range logs {
			_, err := logManager.Append(log)
			if err != nil {
				t.Fatalf("failed to append log: %v", err)
			}
		}

		_, err := logManager.Iterator()
		if err != nil {
			t.Fatalf("failed to get log iterator: %v", err)
		}

		// Check that all current logs were flushed to disk
		if logManager.lastSavedLSN != logManager.latestLSN {
			t.Errorf("lastSavedLSN was not updated correctly: got %d, want %d", logManager.lastSavedLSN, logManager.latestLSN)
		}
	})
}
