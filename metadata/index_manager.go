package metadata

import (
	"simpledb/record"
	"simpledb/transaction"
)

type IndexInfo struct {
	indexName   string
	fieldName   string
	tx          *transaction.Transaction
	tableSchema *record.Schema
	indexLayout *record.Layout
	statInfo    StatInfo
}

func NewIndexInfo(indexName string, fieldName string, tableSchema *record.Schema, tx *transaction.Transaction, statInfo StatInfo) *IndexInfo {
	indexInfo := &IndexInfo{
		indexName:   indexName,
		fieldName:   fieldName,
		tx:          tx,
		tableSchema: tableSchema,
		statInfo:    statInfo,
	}
	indexInfo.indexLayout = indexInfo.createIndexLayout()
	return indexInfo
}

// TODO
func (ii *IndexInfo) Opens() {}

// TODO
func (ii *IndexInfo) BlocksAccessed() {}

func (ii *IndexInfo) RecordsOutput() int32 {
	return ii.statInfo.RecordsOutput() / ii.statInfo.DistinctValues(ii.fieldName)
}

func (ii *IndexInfo) DistinctValues(fieldName string) int32 {
	if fieldName == ii.fieldName {
		return 1
	}
	return ii.statInfo.DistinctValues(fieldName)
}

func (ii *IndexInfo) createIndexLayout() *record.Layout {
	schema := record.NewSchema()
	schema.AddIntField("block")
	schema.AddIntField("id")
	if ii.tableSchema.FieldType(ii.fieldName) == record.Integer {
		schema.AddIntField("dataval")
	} else {
		fieldLength := ii.tableSchema.FieldLength(ii.fieldName)
		schema.AddStringField("dataval", fieldLength)
	}
	return record.NewLayout(schema)
}

type IndexManager struct {
	layout       *record.Layout
	tableManager *TableManager
	statManager  *StatManager
}

func NewIndexManager(isNew bool, tableManager *TableManager, statManager *StatManager, tx *transaction.Transaction) *IndexManager {
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField("indexname", maxName)
		schema.AddStringField("tablename", maxName)
		schema.AddStringField("fieldname", maxName)
		tableManager.CreateTable("idxcat", schema, tx)
	}
	layout := tableManager.GetLayout("idxcat", tx)
	return &IndexManager{layout: layout, tableManager: tableManager, statManager: statManager}
}

func (im *IndexManager) CreateIndex(indexName string, tableName string, fieldName string, tx *transaction.Transaction) {
	tableScan, _ := record.NewTableScan(tx, "idxcat", im.layout)
	tableScan.Insert()
	tableScan.WriteString("indexname", indexName)
	tableScan.WriteString("tablename", tableName)
	tableScan.WriteString("fieldname", fieldName)
	tableScan.Close()
}

func (im *IndexManager) GetIndexInfo(tableName string, tx *transaction.Transaction) map[string]*IndexInfo {
	res := make(map[string]*IndexInfo)
	tableScan, _ := record.NewTableScan(tx, "idxcat", im.layout)
	for tableScan.Next() {
		indexName, _ := tableScan.ReadString("tablename")
		if indexName == tableName {
			indexName, _ := tableScan.ReadString("indexname")
			fieldName, _ := tableScan.ReadString("fieldname")
			layout := im.tableManager.GetLayout(tableName, tx)
			statInfo := im.statManager.GetStatInfo(tableName, layout, tx)
			indexInfo := NewIndexInfo(indexName, fieldName, layout.Schema(), tx, statInfo)
			res[fieldName] = indexInfo
		}
	}

	tableScan.Close()
	return res
}
