package query

import "simpledb/record"

type Scan interface {
	BeforeFirst()
	Next() bool
	ReadInt32(fieldName string) (int32, error)
	ReadString(fieldName string) (string, error)
	ReadValue(fieldName string) (any, error)
	HasField(fieldName string) bool
	Close()
}

type UpdateScan interface {
	Scan
	WriteInt32(fieldName string, value int32)
	WriteString(fieldName string, value string)
	WriteValue(fieldName string, value any)
	Insert()
	Delete()
	GetRID() *record.RID
	MoveToRID(rid *record.RID)
}
