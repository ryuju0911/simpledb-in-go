package metadata

import (
	"simpledb/record"
	"simpledb/transaction"
)

type MetadataManager struct {
	tableManager *TableManager
	viewManager  *ViewManager
	statManager  *StatManager
	indexManager *IndexManager
}

func NewMetadataManager(isNew bool, tx *transaction.Transaction) *MetadataManager {
	tableManager := NewTableManager(isNew, tx)
	viewManager := NewViewManager(isNew, tableManager, tx)
	statManager := NewStatManager(tableManager)
	indexManager := NewIndexManager(isNew, tableManager, statManager, tx)
	return &MetadataManager{tableManager: tableManager, viewManager: viewManager, statManager: statManager, indexManager: indexManager}
}

func (mm *MetadataManager) CreateTable(tableName string, schema *record.Schema, tx *transaction.Transaction) {
	mm.tableManager.CreateTable(tableName, schema, tx)
}

func (mm *MetadataManager) GetLayout(tableName string, tx *transaction.Transaction) *record.Layout {
	return mm.tableManager.GetLayout(tableName, tx)
}

func (mm *MetadataManager) CreateView(viewName string, viewDef string, tx *transaction.Transaction) {
	mm.viewManager.CreateView(viewName, viewDef, tx)
}

func (mm *MetadataManager) GetViewDef(viewName string, tx *transaction.Transaction) string {
	return mm.viewManager.GetViewDef(viewName, tx)
}

func (mm *MetadataManager) CreateIndex(indexName string, tableName string, fieldName string, tx *transaction.Transaction) {
	mm.indexManager.CreateIndex(indexName, tableName, fieldName, tx)
}

func (mm *MetadataManager) GetIndexInfo(tableName string, tx *transaction.Transaction) map[string]*IndexInfo {
	return mm.indexManager.GetIndexInfo(tableName, tx)
}

func (mm *MetadataManager) GetStatInfo(tableName string, layout *record.Layout, tx *transaction.Transaction) StatInfo {
	return mm.statManager.GetStatInfo(tableName, layout, tx)
}
