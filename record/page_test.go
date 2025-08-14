package record

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"simpledb/buffer"
	"simpledb/file"
	"simpledb/log"
	"simpledb/transaction"
)

func TestPage(t *testing.T) {
	directory := t.TempDir()

	fileManager, err := file.NewManager(directory, 400)
	if err != nil {
		t.Fatal(err)
	}

	logManager, err := log.NewManager(fileManager, "testlogfile")
	if err != nil {
		t.Fatal(err)
	}

	bufferManager := buffer.NewManager(fileManager, logManager, 8)

	tx, err := transaction.NewTransaction(fileManager, logManager, bufferManager)
	if err != nil {
		t.Fatal(err)
	}

	schema := NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)

	layout := NewLayout(schema)
	block, err := tx.Append("testfile")
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Pin(block); err != nil {
		t.Fatal(err)
	}

	page, err := NewPage(tx, block, layout)
	if err := page.Format(); err != nil {
		t.Fatal(err)
	}

	slot, err := page.InsertAfter(-1)
	if err != nil {
		t.Fatal(err)
	}

	for slot >= 0 {
		n := int32(rand.N(50))
		if err := page.WriteInt32(slot, "A", n); err != nil {
			t.Fatal(err)
		}
		if err := page.WriteString(slot, "B", fmt.Sprintf("rec%d", n)); err != nil {
			t.Fatal(err)
		}
		slot, err = page.InsertAfter(slot)
		if err != nil {
			t.Fatal(err)
		}
	}

	slot, err = page.NextAfter(-1)
	if err != nil {
		t.Fatal(err)
	}

	for slot >= 0 {
		a, err := page.ReadInt32(slot, "A")
		if err != nil {
			t.Fatal(err)
		}
		_, err = page.ReadString(slot, "B")
		if err != nil {
			t.Fatal(err)
		}

		if a < 25 {
			if err := page.Delete(slot); err != nil {
				t.Fatal(err)
			}
		}
		slot, err = page.NextAfter(slot)
		if err != nil {
			t.Fatal(err)
		}
	}

	for slot >= 0 {
		_, err := page.ReadInt32(slot, "A")
		if err != nil {
			t.Fatal(err)
		}
		_, err = page.ReadString(slot, "B")
		if err != nil {
			t.Fatal(err)
		}

		slot, err = page.NextAfter(slot)
		if err != nil {
			t.Fatal(err)
		}
	}

	tx.Unpin(block)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
