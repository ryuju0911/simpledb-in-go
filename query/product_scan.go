package query

type ProductScan struct {
	scan1 Scan
	scan2 Scan
}

func NewProductScan(scan1 Scan, scan2 Scan) *ProductScan {
	return &ProductScan{
		scan1: scan1,
		scan2: scan2,
	}
}

func (ps *ProductScan) BeforeFirst() {
	ps.scan1.BeforeFirst()
	ps.scan1.Next()
	ps.scan2.BeforeFirst()
}

func (ps *ProductScan) Next() bool {
	if ps.scan2.Next() {
		return true
	}
	ps.scan2.BeforeFirst()
	return ps.scan2.Next() && ps.scan1.Next()
}

func (ps *ProductScan) ReadInt32(fieldName string) (int32, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.ReadInt32(fieldName)
	}
	return ps.scan2.ReadInt32(fieldName)
}

func (ps *ProductScan) ReadString(fieldName string) (string, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.ReadString(fieldName)
	}
	return ps.scan2.ReadString(fieldName)
}

func (ps *ProductScan) ReadValue(fieldName string) (any, error) {
	if ps.scan1.HasField(fieldName) {
		return ps.scan1.ReadValue(fieldName)
	}
	return ps.scan2.ReadValue(fieldName)
}

func (ps *ProductScan) HasField(fieldName string) bool {
	return ps.scan1.HasField(fieldName) || ps.scan2.HasField(fieldName)
}

func (ps *ProductScan) Close() {
	ps.scan1.Close()
	ps.scan2.Close()
}
