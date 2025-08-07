package transaction

import (
	"testing"

	"simpledb/buffer"
	"simpledb/file"
	"simpledb/log"
)

func TestTransaction(t *testing.T) {
	dir := t.TempDir()

	fm, err := file.NewManager(dir, 400)
	if err != nil {
		t.Fatalf("failed to create file manager: %v", err)
	}

	lm, err := log.NewManager(fm, "testlogfile")
	if err != nil {
		t.Fatalf("failed to create log manager: %v", err)
	}

	bm := buffer.NewManager(fm, lm, 8)

	tx1, err := NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("tx1: failed to create transaction: %v", err)
	}

	block := file.NewBlock("testfile", 1)
	err = tx1.Pin(block)
	if err != nil {
		t.Fatalf("tx1: failed to pin block: %v", err)
	}

	if err := tx1.WriteInt32(block, 80, 1, false); err != nil {
		t.Fatalf("tx1: failed to write int32: %v", err)
	}

	if err := tx1.WriteString(block, 40, "one", false); err != nil {
		t.Fatalf("tx1: failed to write string: %v", err)
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx1: failed to commit: %v", err)
	}

	tx2, err := NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("tx2: failed to create transaction: %v", err)
	}

	if err := tx2.Pin(block); err != nil {
		t.Fatalf("tx2: failed to pin block: %v", err)
	}

	intVal, err := tx2.ReadInt32(block, 80)
	if err != nil {
		t.Fatalf("tx2: failed to read int32: %v", err)
	}

	strVal, err := tx2.ReadString(block, 40)
	if err != nil {
		t.Fatalf("tx2: failed to read string: %v", err)
	}

	if intVal != 1 {
		t.Errorf("Expected intVal to be 1, got %d", intVal)
	}
	if strVal != "one" {
		t.Errorf("Expected strVal to be 'one', got '%s'", strVal)
	}

	if err := tx2.WriteInt32(block, 80, 2, true); err != nil {
		t.Fatalf("tx2: failed to write int32: %v", err)
	}
	if err := tx2.WriteString(block, 40, "one!", true); err != nil {
		t.Fatalf("tx2: failed to write string: %v", err)
	}

	if err := tx2.Commit(); err != nil {
		t.Fatalf("tx2: failed to commit: %v", err)
	}

	tx3, err := NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("tx3: failed to create transaction: %v", err)
	}

	if err := tx3.Pin(block); err != nil {
		t.Fatalf("tx3: failed to pin block: %v", err)
	}

	intVal, err = tx3.ReadInt32(block, 80)
	if err != nil {
		t.Fatalf("tx3: failed to read int32: %v", err)
	}

	strVal, err = tx3.ReadString(block, 40)
	if err != nil {
		t.Fatalf("tx3: failed to read string: %v", err)
	}

	if intVal != 2 {
		t.Errorf("Expected intVal to be 2, got %d", intVal)
	}
	if strVal != "one!" {
		t.Errorf("Expected strVal to be 'one!', got '%s'", strVal)
	}

	if err := tx3.Rollback(); err != nil {
		t.Fatalf("tx3: failed to rollback: %v", err)
	}

	tx4, err := NewTransaction(fm, lm, bm)
	if err != nil {
		t.Fatalf("tx4: failed to create transaction: %v", err)
	}

	if err := tx4.Pin(block); err != nil {
		t.Fatalf("tx4: failed to pin block: %v", err)
	}

	intVal, err = tx4.ReadInt32(block, 80)
	if err != nil {
		t.Fatalf("tx4: failed to read int32: %v", err)
	}

	strVal, err = tx4.ReadString(block, 40)
	if err != nil {
		t.Fatalf("tx4: failed to read string: %v", err)
	}

	if intVal != 2 {
		t.Errorf("Expected intVal to be 2, got %d", intVal)
	}
	if strVal != "one!" {
		t.Errorf("Expected strVal to be 'one!', got '%s'", strVal)
	}

	if err := tx4.Commit(); err != nil {
		t.Fatalf("tx4: failed to commit: %v", err)
	}
}
