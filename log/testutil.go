package log

import (
	"testing"

	"simpledb/file"
)

func setup(t *testing.T, blockSize int32) (*file.Manager, *Manager, string) {
	t.Helper()
	dir := t.TempDir()
	fm, err := file.NewManager(dir, blockSize)
	if err != nil {
		t.Fatalf("failed to create file manager: %v", err)
	}
	logFile := "testlogfile"
	lm, err := NewManager(fm, logFile)
	if err != nil {
		t.Fatalf("failed to create log manager: %v", err)
	}
	return fm, lm, logFile
}
