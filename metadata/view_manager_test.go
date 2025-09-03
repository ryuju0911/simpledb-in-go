package metadata

import (
	"testing"

	"simpledb/server"
)

func TestViewManager(t *testing.T) {
	directory := t.TempDir()

	simpleDB := server.NewSimpleDB(directory, 400, 8)
	tx := simpleDB.NewTx()

	tm := NewTableManager(true, tx)
	vm := NewViewManager(true, tm, tx)

	vm.CreateView("MyView", "SELECT * FROM MyTable", tx)

	viewDef := vm.GetViewDef("MyView", tx)
	if viewDef != "SELECT * FROM MyTable" {
		t.Errorf("invalid view definition: got %s, want %s", viewDef, "SELECT * FROM MyTable")
	}
}
