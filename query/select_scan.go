package query

import (
	"simpledb/record"
)

type SelectScan struct {
	scan UpdateScan
	pred *Predicate
}

func NewSelectScan(scan UpdateScan, pred *Predicate) *SelectScan {
	return &SelectScan{
		scan: scan,
		pred: pred,
	}
}

func (ss *SelectScan) BeforeFirst() {
	ss.scan.BeforeFirst()
}

func (ss *SelectScan) Next() bool {
	for ss.scan.Next() {
		if ss.pred.IsSatisfied(ss.scan) {
			return true
		}
	}
	return false
}

func (ss *SelectScan) ReadInt32(fieldName string) (int32, error) {
	return ss.scan.ReadInt32(fieldName)
}

func (ss *SelectScan) ReadString(fieldName string) (string, error) {
	return ss.scan.ReadString(fieldName)
}

func (ss *SelectScan) ReadValue(fieldName string) (any, error) {
	return ss.scan.ReadValue(fieldName)
}

func (ss *SelectScan) HasField(fieldName string) bool {
	return ss.scan.HasField(fieldName)
}

func (ss *SelectScan) Close() {
	ss.scan.Close()
}

func (ss *SelectScan) WriteInt32(fieldName string, value int32) {
	ss.scan.WriteInt32(fieldName, value)
}

func (ss *SelectScan) WriteString(fieldName string, value string) {
	ss.scan.WriteString(fieldName, value)
}

func (ss *SelectScan) WriteValue(fieldName string, value any) {
	ss.scan.WriteValue(fieldName, value)
}

func (ss *SelectScan) Insert() {
	ss.scan.Insert()
}

func (ss *SelectScan) Delete() {
	ss.scan.Delete()
}

func (ss *SelectScan) GetRID() *record.RID {
	return ss.scan.GetRID()
}

func (ss *SelectScan) MoveToRID(rid *record.RID) {
	ss.scan.MoveToRID(rid)
}
