package query

import "simpledb/record"

type Term struct {
	lhs Expression
	rhs Expression
}

func NewTerm(lhs Expression, rhs Expression) Term {
	return Term{
		lhs: lhs,
		rhs: rhs,
	}
}

func (t Term) IsSatisfied(scan Scan) bool {
	lhsVal := t.lhs.Evaluate(scan)
	rhsVal := t.rhs.Evaluate(scan)
	return lhsVal == rhsVal
}

func (t Term) AppliesTo(schema *record.Schema) bool {
	return t.lhs.AppliesTo(schema) && t.rhs.AppliesTo(schema)
}

// TODO
// func (t Term) ReductionFactor()

func (t Term) EquqtesWithConstant(fieldName string) any {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && !t.rhs.IsFieldName() {
		return t.rhs.AsConstant()
	}
	if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && !t.lhs.IsFieldName() {
		return t.lhs.AsConstant()
	}
	return nil
}

func (t Term) EquatesWithField(fieldName string) string {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && t.rhs.IsFieldName() {
		return t.rhs.AsFieldName()
	}
	if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && t.lhs.IsFieldName() {
		return t.lhs.AsFieldName()
	}
	return ""
}
