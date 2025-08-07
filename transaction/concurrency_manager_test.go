package transaction

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"simpledb/buffer"
	"simpledb/file"
	"simpledb/log"
)

func TestConcurrencyManager(t *testing.T) {
	dir := t.TempDir()

	fm, err := file.NewManager(dir, 400)
	if err != nil {
		t.Fatal(err)
	}

	lm, err := log.NewManager(fm, "testlogfile")
	if err != nil {
		t.Fatal(err)
	}

	bm := buffer.NewManager(fm, lm, 8)

	var eg errgroup.Group
	eg.Go(func() error {
		return clientA(t, fm, lm, bm)
	})
	eg.Go(func() error {
		return clientB(t, fm, lm, bm)
	})
	eg.Go(func() error {
		return clientC(t, fm, lm, bm)
	})

	if err := eg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func clientA(t *testing.T, fm *file.Manager, lm *log.Manager, bm *buffer.Manager) error {
	t.Helper()

	tx, err := NewTransaction(fm, lm, bm)
	if err != nil {
		return fmt.Errorf("clientA: failed to create transaction: %w", err)
	}

	block1 := file.NewBlock("testfile", 1)
	block2 := file.NewBlock("testfile", 2)

	if err := tx.Pin(block1); err != nil {
		return fmt.Errorf("clientA: failed to pin block1: %w", err)
	}

	if err := tx.Pin(block2); err != nil {
		return fmt.Errorf("clientA: failed to pin block2: %w", err)
	}

	if _, err := tx.ReadInt32(block1, 0); err != nil {
		return fmt.Errorf("clientA: failed to read from block1: %w", err)
	}

	time.Sleep(1 * time.Second)

	if _, err := tx.ReadInt32(block2, 0); err != nil {
		return fmt.Errorf("clientA: failed to read from block2: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("clientA: failed to commit: %w", err)
	}
	return nil
}

func clientB(t *testing.T, fm *file.Manager, lm *log.Manager, bm *buffer.Manager) error {
	t.Helper()

	tx, err := NewTransaction(fm, lm, bm)
	if err != nil {
		return fmt.Errorf("clientB: failed to create transaction: %w", err)
	}

	block1 := file.NewBlock("testfile", 1)
	block2 := file.NewBlock("testfile", 2)

	if err := tx.Pin(block1); err != nil {
		return fmt.Errorf("clientB: failed to pin block1: %w", err)
	}

	if err := tx.Pin(block2); err != nil {
		return fmt.Errorf("clientB: failed to pin block2: %w", err)
	}

	if err := tx.WriteInt32(block2, 0, 0, false); err != nil {
		return fmt.Errorf("clientB: failed to write to block2: %w", err)
	}

	time.Sleep(1 * time.Second)

	if _, err := tx.ReadInt32(block1, 0); err != nil {
		return fmt.Errorf("clientB: failed to read from block1: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("clientB: failed to commit: %w", err)
	}
	return nil
}

func clientC(t *testing.T, fm *file.Manager, lm *log.Manager, bm *buffer.Manager) error {
	t.Helper()

	tx, err := NewTransaction(fm, lm, bm)
	if err != nil {
		return fmt.Errorf("clientC: failed to create transaction: %w", err)
	}

	block1 := file.NewBlock("testfile", 1)
	block2 := file.NewBlock("testfile", 2)

	if err := tx.Pin(block1); err != nil {
		return fmt.Errorf("clientC: failed to pin block1: %w", err)
	}

	if err := tx.Pin(block2); err != nil {
		return fmt.Errorf("clientC: failed to pin block2: %w", err)
	}

	if err := tx.WriteInt32(block1, 0, 0, false); err != nil {
		return fmt.Errorf("clientC: failed to write to block1: %w", err)
	}

	time.Sleep(1 * time.Second)

	if _, err := tx.ReadInt32(block2, 0); err != nil {
		return fmt.Errorf("clientC: failed to read from block2: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("clientC: failed to commit: %w", err)
	}
	return nil
}
