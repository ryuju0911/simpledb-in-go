package metadata

import (
	"simpledb/record"
	"simpledb/transaction"
)

// View definitions are stored as varchar strings.
// The current limit of 100 characters is completely unrealistic,
// as a view definition could be thousands of characters long.
// A better choice would be to implement the ViewDef field as a clob type, such as clob(9999).
const maxViewDef int32 = 100

type ViewManager struct {
	tableManager *TableManager
}

func NewViewManager(isNew bool, tableManager *TableManager, tx *transaction.Transaction) *ViewManager {
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField("viewname", maxName)
		schema.AddStringField("viewdef", maxViewDef)
		tableManager.CreateTable("viewcat", schema, tx)
	}
	return &ViewManager{tableManager: tableManager}
}

func (vm *ViewManager) CreateView(viewName string, viewDef string, tx *transaction.Transaction) {
	layout := vm.tableManager.GetLayout("viewcat", tx)
	tableScan, _ := record.NewTableScan(tx, "viewcat", layout)
	tableScan.Insert()
	tableScan.WriteString("viewname", viewName)
	tableScan.WriteString("viewdef", viewDef)
	tableScan.Close()
}

func (vm *ViewManager) GetViewDef(viewName string, tx *transaction.Transaction) string {
	layout := vm.tableManager.GetLayout("viewcat", tx)
	tableScan, _ := record.NewTableScan(tx, "viewcat", layout)
	for {
		exist, _ := tableScan.Next()
		if !exist {
			break
		}
		name, _ := tableScan.ReadString("viewname")
		if name == viewName {
			viewDef, _ := tableScan.ReadString("viewdef")
			return viewDef
		}
	}
	tableScan.Close()
	return ""
}
