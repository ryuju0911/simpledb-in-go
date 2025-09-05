package metadata

import (
	"testing"

	"simpledb/record"
	"simpledb/server"
)

func TestIndexManager(t *testing.T) {
	directory := t.TempDir()

	simpleDB := server.NewSimpleDB(directory, 400, 8)
	tx := simpleDB.NewTx()

	tm := NewTableManager(true, tx)
	sm := NewStatManager(tm)
	im := NewIndexManager(true, tm, sm, tx)

	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	tm.CreateTable("MyTable", schema, tx)

	im.CreateIndex("MyIndex", "MyTable", "A", tx)

	indexInfo := im.GetIndexInfo("MyTable", tx)

	if len(indexInfo) != 1 {
		t.Errorf("invalid index info length: got %d, want %d", len(indexInfo), 1)
	}
	if indexInfo["A"].indexName != "MyIndex" {
		t.Errorf("invalid index name: got %s, want %s", indexInfo["A"].indexName, "MyIndex")
	}
	if indexInfo["A"].fieldName != "A" {
		t.Errorf("invalid field name: got %s, want %s", indexInfo["A"].fieldName, "A")
	}
}
