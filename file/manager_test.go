package file

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestNewManager(t *testing.T) {
	t.Run("Creates directory and cleans up temp files", func(t *testing.T) {
		dir := t.TempDir()
		const blockSize = 400

		// Create some files to test cleanup logic
		tempFilePath := filepath.Join(dir, "tempfile1")
		if _, err := os.Create(tempFilePath); err != nil {
			t.Fatalf("Failed to create temp file for test: %v", err)
		}

		permanentFilePath := filepath.Join(dir, "permanentfile.db")
		if _, err := os.Create(permanentFilePath); err != nil {
			t.Fatalf("Failed to create permanent file for test: %v", err)
		}

		// Create the manager
		manager, err := NewManager(dir, blockSize)
		if err != nil {
			t.Fatalf("NewManager() failed: %v", err)
		}

		if manager == nil {
			t.Fatal("NewManager() returned a nil manager")
		}

		// Check if temp file was removed
		if _, err := os.Stat(tempFilePath); !os.IsNotExist(err) {
			t.Errorf("Temp file %q was not removed", tempFilePath)
		}

		// Check if permanent file still exists
		if _, err := os.Stat(permanentFilePath); os.IsNotExist(err) {
			t.Errorf("Permanent file %q was unexpectedly removed", permanentFilePath)
		}
	})

	t.Run("Handles existing directory", func(t *testing.T) {
		dir := t.TempDir()
		const blockSize = 400

		// Call NewManager once to create the directory
		if _, err := NewManager(dir, blockSize); err != nil {
			t.Fatalf("First call to NewManager() failed: %v", err)
		}

		// Call it again on the same directory
		_, err := NewManager(dir, blockSize)
		if err != nil {
			t.Fatalf("Second call to NewManager() on existing directory failed: %v", err)
		}
	})

	t.Run("Fails on invalid directory path", func(t *testing.T) {
		// Create a file to use as an invalid directory path
		tmpfile, err := os.CreateTemp("", "testfile")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tmpfile.Name()) // clean up
		tmpfile.Close()

		// Attempt to create a manager with a path that is a file, not a directory.
		// os.MkdirAll will return an error here on some OSes (e.g. Linux "ENOTDIR").
		_, err = NewManager(tmpfile.Name(), 400)
		if err == nil {
			t.Errorf("NewManager() should have failed for path %q but it did not", tmpfile.Name())
		}
	})
}

func TestManager_ReadWrite(t *testing.T) {
	directory := t.TempDir()
	const blockSize = 400
	manager, err := NewManager(directory, blockSize)
	if err != nil {
		t.Fatalf("Failed to create file manager: %v", err)
	}

	block := NewBlock("testfile", 2) // Use block number 2 to test offset calculation
	p1 := NewPage(manager.blockSize)

	// Populate the page with some data.
	p1.WriteStringAt(88, "hello world")
	p1.WriteInt32At(20, 12345)

	// Write the page to a block in the file.
	if err := manager.Write(block, p1); err != nil {
		t.Fatalf("Write() failed: %v", err)
	}

	// Create a new page and read the same block into it.
	p2 := NewPage(manager.blockSize)
	if err := manager.Read(block, p2); err != nil {
		t.Fatalf("Read() failed: %v", err)
	}

	// Verify that the contents of the two pages are identical.
	if !bytes.Equal(p1.Buf(), p2.Buf()) {
		t.Errorf("Page buffers do not match after read/write cycle")
	}
}

func TestManager_Append(t *testing.T) {
	directory := t.TempDir()
	const blockSize = 400
	const filename = "testappendfile"
	manager, err := NewManager(directory, blockSize)
	if err != nil {
		t.Fatalf("Failed to create file manager: %v", err)
	}

	// Append first block
	block1, err := manager.Append(filename)
	if err != nil {
		t.Fatalf("Append() failed for the first block: %v", err)
	}

	if block1.Number() != 0 {
		t.Errorf("block1.Number() = %d, want 0", block1.Number())
	}

	// Append second block
	block2, err := manager.Append(filename)
	if err != nil {
		t.Fatalf("Append() failed for the second block: %v", err)
	}

	if block2.Number() != 1 {
		t.Errorf("block2.Number() = %d, want 1", block2.Number())
	}
}

func TestManager_Size(t *testing.T) {
	directory := t.TempDir()
	const blockSize = 400
	const filename = "testsizefile"
	manager, err := NewManager(directory, blockSize)
	if err != nil {
		t.Fatalf("Failed to create file manager: %v", err)
	}

	// Case 1: Size of a new/empty file should be 0.
	size, err := manager.Size(filename)
	if err != nil {
		t.Fatalf("Size() on new file failed: %v", err)
	}
	if size != 0 {
		t.Errorf("Size() on new file = %d, want 0", size)
	}

	// Case 2: Size after appending one block.
	if _, err := manager.Append(filename); err != nil {
		t.Fatalf("Append() failed: %v", err)
	}
	size, err = manager.Size(filename)
	if err != nil {
		t.Fatalf("Size() after one append failed: %v", err)
	}
	if size != 1 {
		t.Errorf("Size() after one append = %d, want 1", size)
	}

	// Case 3: Size after appending a second block.
	if _, err := manager.Append(filename); err != nil {
		t.Fatalf("Append() failed: %v", err)
	}
	size, err = manager.Size(filename)
	if err != nil {
		t.Fatalf("Size() after two appends failed: %v", err)
	}
	if size != 2 {
		t.Errorf("Size() after two appends = %d, want 2", size)
	}

	// Case 4: Write a partial block and check size.
	p := NewPage(blockSize)
	p.WriteStringAt(0, "partial data")
	// Write to a new block (block 2)
	if err := manager.Write(NewBlock(filename, 2), p); err != nil {
		t.Fatalf("Write() for partial block failed: %v", err)
	}
	// The file now contains 3 blocks (0, 1, 2).
	size, err = manager.Size(filename)
	if err != nil {
		t.Fatalf("Size() after partial write failed: %v", err)
	}
	if size != 3 {
		t.Errorf("Size() after writing to block 2 = %d, want 3", size)
	}
}
