package metadata

import (
	"testing"

	"simpledb/record"
	"simpledb/server"
)

func TestStatManager(t *testing.T) {
	directory := t.TempDir()

	simpleDB := server.NewSimpleDB(directory, 400, 8)
	tx := simpleDB.NewTx()

	tm := NewTableManager(true, tx)

	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	tm.CreateTable("MyTable", schema, tx)

	tableScan, _ := record.NewTableScan(tx, "MyTable", tm.GetLayout("MyTable", tx))
	tableScan.Insert()
	tableScan.WriteInt32("A", 1)
	tableScan.WriteString("B", "test")
	tableScan.Close()

	sm := NewStatManager(tm)
	statInfo := sm.GetStatInfo("MyTable", tm.GetLayout("MyTable", tx), tx)

	if statInfo.BlocksAccessed() != 1 {
		t.Errorf("invalid blocks accessed: got %d, want %d", statInfo.BlocksAccessed(), 1)
	}
	if statInfo.RecordsOutput() != 1 {
		t.Errorf("invalid records output: got %d, want %d", statInfo.RecordsOutput(), 1)
	}
	if statInfo.DistinctValues("A") != 1 {
		t.Errorf("invalid distinct values of A: got %d, want %d", statInfo.DistinctValues("A"), 1)
	}
	if statInfo.DistinctValues("B") != 1 {
		t.Errorf("invalid distinct values of B: got %d, want %d", statInfo.DistinctValues("B"), 1)
	}
}
