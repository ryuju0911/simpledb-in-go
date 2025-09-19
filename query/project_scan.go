package query

import "errors"

type ProjectScan struct {
	scan   Scan
	fields []string
}

func NewProjectScan(scan Scan, fields []string) *ProjectScan {
	return &ProjectScan{
		scan:   scan,
		fields: fields,
	}
}

func (ps *ProjectScan) BeforeFirst() {
	ps.scan.BeforeFirst()
}

func (ps *ProjectScan) Next() bool {
	return ps.scan.Next()
}

var ErrFieldNotFound = errors.New("field not found")

func (ps *ProjectScan) ReadInt32(fieldName string) (int32, error) {
	if ps.HasField(fieldName) {
		return ps.scan.ReadInt32(fieldName)
	}
	return 0, ErrFieldNotFound
}

func (ps *ProjectScan) ReadString(fieldName string) (string, error) {
	if ps.HasField(fieldName) {
		return ps.scan.ReadString(fieldName)
	}
	return "", ErrFieldNotFound
}

func (ps *ProjectScan) ReadValue(fieldName string) (any, error) {
	if ps.HasField(fieldName) {
		return ps.scan.ReadValue(fieldName)
	}
	return nil, ErrFieldNotFound
}

func (ps *ProjectScan) HasField(fieldName string) bool {
	for _, fld := range ps.fields {
		if fld == fieldName {
			return true
		}
	}
	return false
}

func (ps *ProjectScan) Close() {
	ps.scan.Close()
}
