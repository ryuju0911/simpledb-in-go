package metadata

import (
	"simpledb/record"
	"simpledb/transaction"
)

const maxName int32 = 16

type TableManager struct {
	tcatLayout *record.Layout
	fcatLayout *record.Layout
}

func NewTableManager(isNew bool, tx *transaction.Transaction) *TableManager {
	tcatSchema := record.NewSchema()
	tcatSchema.AddStringField("tblname", maxName)
	tcatSchema.AddIntField("slotsize")
	tcatLayout := record.NewLayout(tcatSchema)

	fcatSchema := record.NewSchema()
	fcatSchema.AddStringField("tblname", maxName)
	fcatSchema.AddStringField("fldname", maxName)
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	fcatLayout := record.NewLayout(fcatSchema)

	tm := &TableManager{tcatLayout: tcatLayout, fcatLayout: fcatLayout}

	if isNew {
		tm.CreateTable("tblcat", tcatSchema, tx)
		tm.CreateTable("fldcat", fcatSchema, tx)
	}

	return tm
}

// CreateTable calculates the record offsets and saves it all in the catalog.
func (tm *TableManager) CreateTable(tableName string, schema *record.Schema, tx *transaction.Transaction) {
	layout := record.NewLayout(schema)

	// Insert one record into tblcat.
	tcat, _ := record.NewTableScan(tx, "tblcat", tm.tcatLayout)

	tcat.Insert()
	tcat.WriteString("tblname", tableName)
	tcat.WriteInt32("slotsize", layout.SlotSize())
	tcat.Close()

	fcat, _ := record.NewTableScan(tx, "fldcat", tm.fcatLayout)
	for _, fieldName := range schema.Fields() {
		fcat.Insert()
		fcat.WriteString("tblname", tableName)
		fcat.WriteString("fldname", fieldName)
		fcat.WriteInt32("type", int32(schema.FieldType(fieldName)))
		fcat.WriteInt32("length", schema.FieldLength(fieldName))
		fcat.WriteInt32("offset", layout.Offset(fieldName))
	}
	fcat.Close()
}

// GetLayout goes to the catalog, extracts the metadata for the specified table,
// and returns a Layout object containing the metadata.
func (tm *TableManager) GetLayout(tableName string, tx *transaction.Transaction) *record.Layout {
	var size int32 = -1
	tcat, _ := record.NewTableScan(tx, "tblcat", tm.tcatLayout)
	for tcat.Next() {
		name, _ := tcat.ReadString("tblname")
		if name == tableName {
			size, _ = tcat.ReadInt32("slotsize")
			break
		}
	}
	tcat.Close()

	schema := record.NewSchema()
	offsets := make(map[string]int32)
	fcat, _ := record.NewTableScan(tx, "fldcat", tm.fcatLayout)
	for fcat.Next() {
		name, _ := fcat.ReadString("tblname")
		if name == tableName {
			fieldName, _ := fcat.ReadString("fldname")
			fieldType, _ := fcat.ReadInt32("type")
			length, _ := fcat.ReadInt32("length")
			offset, _ := fcat.ReadInt32("offset")
			offsets[fieldName] = offset
			schema.AddField(fieldName, record.FieldType(fieldType), length)
		}
	}
	fcat.Close()

	return record.NewLayoutFromMetadata(schema, offsets, size)
}
