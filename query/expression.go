package query

import "simpledb/record"

type Expression struct {
	constant  any
	fieldName *string
}

func NewExpressionWithValue(value any) Expression {
	return Expression{
		constant: value,
	}
}

func NewExpressionWithFieldName(fieldName string) Expression {
	return Expression{
		fieldName: &fieldName,
	}
}

func (e Expression) IsFieldName() bool {
	return e.fieldName != nil
}

func (e Expression) AsConstant() any {
	return e.constant
}

func (e Expression) AsFieldName() string {
	return *e.fieldName
}

func (e Expression) Evaluate(scan Scan) any {
	if e.constant != nil {
		return e.constant
	}
	val, _ := scan.ReadValue(*e.fieldName)
	return val
}

func (e Expression) AppliesTo(schema *record.Schema) bool {
	if e.constant != nil {
		return true
	}
	return schema.HasField(*e.fieldName)
}
