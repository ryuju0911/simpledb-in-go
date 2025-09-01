package metadata

import (
	"slices"
	"testing"

	"simpledb/record"
	"simpledb/server"
)

func TestTableManager(t *testing.T) {
	directory := t.TempDir()

	simpleDB := server.NewSimpleDB(directory, 400, 8)
	tx := simpleDB.NewTx()

	tm := NewTableManager(true, tx)

	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	tm.CreateTable("MyTable", schema, tx)

	layout := tm.GetLayout("MyTable", tx)
	size := layout.SlotSize()
	if size != 21 {
		t.Errorf("invalid slot size: got %d, want %d", size, 21)
	}

	gotSchema := layout.Schema()
	if !slices.Equal(gotSchema.Fields(), []string{"A", "B"}) {
		t.Errorf("invalid schema: got %v, want %v", gotSchema.Fields(), []string{"A", "B"})
	}
	if gotSchema.FieldLength("B") != 9 {
		t.Errorf("invalid field length: got %d, want %d", gotSchema.FieldLength("B"), 9)
	}
}
