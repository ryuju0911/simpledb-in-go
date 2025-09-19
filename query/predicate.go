package query

import "simpledb/record"

type Predicate struct {
	terms []Term
}

func NewPredicate(term Term) *Predicate {
	return &Predicate{
		terms: []Term{term},
	}
}

func (p *Predicate) ConjoinWith(pred *Predicate) {
	p.terms = append(p.terms, pred.terms...)
}

func (p *Predicate) IsSatisfied(scan Scan) bool {
	for _, term := range p.terms {
		if !term.IsSatisfied(scan) {
			return false
		}
	}
	return true
}

// TODO
// func (p *Predicate) ReductionFactor() int32 {}

func (p *Predicate) SelectSubPred(schema *record.Schema) *Predicate {
	res := &Predicate{}
	for _, term := range p.terms {
		if term.AppliesTo(schema) {
			res.terms = append(res.terms, term)
		}
	}
	if len(res.terms) == 0 {
		return nil
	}
	return res
}

func (p *Predicate) JoinSubPred(schema1 *record.Schema, schema2 *record.Schema) *Predicate {
	res := &Predicate{}
	newSchema := record.NewSchema()
	newSchema.AddAll(schema1)
	newSchema.AddAll(schema2)
	for _, term := range p.terms {
		if !term.AppliesTo(schema1) && !term.AppliesTo(schema2) && term.AppliesTo(newSchema) {
			res.terms = append(res.terms, term)
		}
	}
	if len(res.terms) == 0 {
		return nil
	}
	return res
}

func (p *Predicate) EquatesWithConstant(fieldName string) any {
	for _, term := range p.terms {
		if val := term.EquqtesWithConstant(fieldName); val != nil {
			return val
		}
	}
	return nil
}

func (p *Predicate) EquatesWithField(fieldName string) string {
	for _, term := range p.terms {
		if val := term.EquatesWithField(fieldName); val != "" {
			return val
		}
	}
	return ""
}
