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

func TestTableScan(t *testing.T) {
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

	ts, err := NewTableScan(tx, "T", layout)
	if err != nil {
		t.Fatal(err)
	}

	if err := ts.BeforeFirst(); err != nil {
		t.Fatal(err)
	}

	for range 50 {
		if err := ts.Insert(); err != nil {
			t.Fatal(err)
		}

		n := int32(rand.N(50))
		if err := ts.WriteInt32("A", n); err != nil {
			t.Fatal(err)
		}
		if err := ts.WriteString("B", fmt.Sprintf("rec%d", n)); err != nil {
			t.Fatal(err)
		}
	}

	if err := ts.BeforeFirst(); err != nil {
		t.Fatal(err)
	}

	for {
		exist, err := ts.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !exist {
			break
		}

		a, err := ts.ReadInt32("A")
		if err != nil {
			t.Fatal(err)
		}
		_, err = ts.ReadString("B")
		if err != nil {
			t.Fatal(err)
		}

		if a < 25 {
			if err := ts.Delete(); err != nil {
				t.Fatal(err)
			}
		}
	}

	if err := ts.BeforeFirst(); err != nil {
		t.Fatal(err)
	}

	for {
		exist, err := ts.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !exist {
			break
		}

		_, err = ts.ReadInt32("A")
		if err != nil {
			t.Fatal(err)
		}
		_, err = ts.ReadString("B")
		if err != nil {
			t.Fatal(err)
		}
	}

	ts.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
