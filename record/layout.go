package record

type Layout struct {
	schema   *Schema
	offsets  map[string]int32
	slotSize int32
}

func NewLayout(schema *Schema) *Layout {
	offsets := make(map[string]int32)
	pos := int32(4)
	for _, fieldName := range schema.fields {
		offsets[fieldName] = pos
		pos += lengthInBytes(schema, fieldName)
	}
	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: pos,
	}
}

func (l *Layout) Schema() *Schema {
	return l.schema
}

func (l *Layout) Offset(fieldName string) int32 {
	return l.offsets[fieldName]
}

func (l *Layout) SlotSize() int32 {
	return l.slotSize
}

func lengthInBytes(schema *Schema, fieldName string) int32 {
	fieldType := schema.FieldType(fieldName)
	switch fieldType {
	case Integer:
		return 4
	case Varchar:
		return schema.FieldLength(fieldName) + 4
	default:
		return 0
	}
}
