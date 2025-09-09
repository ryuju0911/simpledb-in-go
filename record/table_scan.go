package record

import (
	"fmt"

	"simpledb/file"
	"simpledb/transaction"
)

type TableScan struct {
	tx          *transaction.Transaction
	layout      *Layout
	recordPage  *Page
	filename    string
	currentSlot int32
}

func NewTableScan(tx *transaction.Transaction, tableName string, layout *Layout) (*TableScan, error) {
	fileName := fmt.Sprintf("%s.tbl", tableName)
	ts := &TableScan{
		tx:       tx,
		layout:   layout,
		filename: fileName,
	}

	size, err := tx.Size(fileName)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		if err := ts.moveToNewBlock(); err != nil {
			return nil, err
		}
	} else {
		if err := ts.moveToBlock(0); err != nil {
			return nil, err
		}
	}

	return ts, nil
}

func (ts *TableScan) Close() {
	if ts.recordPage != nil {
		ts.tx.Unpin(ts.recordPage.Block())
	}
}

func (ts *TableScan) BeforeFirst() {
	ts.moveToBlock(0)
}

func (ts *TableScan) Next() bool {
	slot, err := ts.recordPage.NextAfter(ts.currentSlot)
	if err != nil {
		return false
	}
	ts.currentSlot = slot

	for slot < 0 {
		lastBlock, err := ts.atLastBlock()
		if err != nil {
			return false
		}
		if lastBlock {
			return false
		}
		if err := ts.moveToBlock(ts.recordPage.Block().Number() + 1); err != nil {
			return false
		}
		slot, err = ts.recordPage.NextAfter(ts.currentSlot)
		if err != nil {
			return false
		}
		ts.currentSlot = slot
	}
	return true
}

func (ts *TableScan) ReadInt32(fieldName string) (int32, error) {
	return ts.recordPage.ReadInt32(ts.currentSlot, fieldName)
}

func (ts *TableScan) ReadString(fieldName string) (string, error) {
	return ts.recordPage.ReadString(ts.currentSlot, fieldName)
}

func (ts *TableScan) ReadValue(fieldName string) (any, error) {
	if ts.layout.Schema().FieldType(fieldName) == Integer {
		return ts.ReadInt32(fieldName)
	}
	return ts.ReadString(fieldName)
}

func (ts *TableScan) HasField(fieldName string) bool {
	return ts.layout.Schema().HasField(fieldName)
}

func (ts *TableScan) WriteInt32(fieldName string, value int32) {
	ts.recordPage.WriteInt32(ts.currentSlot, fieldName, value)
}

func (ts *TableScan) WriteString(fieldName string, value string) {
	ts.recordPage.WriteString(ts.currentSlot, fieldName, value)
}

func (ts *TableScan) WriteValue(fieldName string, value any) {
	if ts.layout.Schema().FieldType(fieldName) == Integer {
		ts.WriteInt32(fieldName, value.(int32))
	} else {
		ts.WriteString(fieldName, value.(string))
	}
}

func (ts *TableScan) Insert() {
	slot, _ := ts.recordPage.InsertAfter(ts.currentSlot)
	ts.currentSlot = slot

	for ts.currentSlot < 0 {
		lastBlock, _ := ts.atLastBlock()
		if lastBlock {
			ts.moveToNewBlock()
		} else {
			ts.moveToBlock(ts.recordPage.Block().Number() + 1)
		}
		ts.currentSlot, _ = ts.recordPage.InsertAfter(ts.currentSlot)
	}
}

func (ts *TableScan) Delete() {
	ts.recordPage.Delete(ts.currentSlot)
}

func (ts *TableScan) MoveToRID(rid *RID) error {
	ts.Close()
	block := file.NewBlock(ts.filename, rid.blockNum)
	recordPage, err := NewPage(ts.tx, block, ts.layout)
	if err != nil {
		return err
	}

	ts.recordPage = recordPage
	ts.currentSlot = rid.slot
	return nil
}

func (ts *TableScan) GetRID() *RID {
	return &RID{
		blockNum: ts.recordPage.Block().Number(),
		slot:     ts.currentSlot,
	}
}

func (ts *TableScan) moveToBlock(blockNum int32) error {
	ts.Close()
	block := file.NewBlock(ts.filename, blockNum)
	recordPage, err := NewPage(ts.tx, block, ts.layout)
	if err != nil {
		return err
	}
	ts.recordPage = recordPage
	ts.currentSlot = -1
	return nil
}

func (ts *TableScan) moveToNewBlock() error {
	ts.Close()
	block, err := ts.tx.Append(ts.filename)
	if err != nil {
		return err
	}

	recordPage, err := NewPage(ts.tx, block, ts.layout)
	if err != nil {
		return err
	}
	ts.recordPage = recordPage

	if err := ts.recordPage.Format(); err != nil {
		return err
	}

	ts.currentSlot = -1
	return nil
}

func (ts *TableScan) atLastBlock() (bool, error) {
	size, err := ts.tx.Size(ts.filename)
	if err != nil {
		return false, err
	}
	return ts.recordPage.Block().Number() == size-1, nil
}

type RID struct {
	blockNum int32
	slot     int32
}

func (r *RID) BlockNumber() int32 {
	return r.blockNum
}

func (r *RID) Slot() int32 {
	return r.slot
}

func (r *RID) Equals(other *RID) bool {
	return r.blockNum == other.blockNum && r.slot == other.slot
}
