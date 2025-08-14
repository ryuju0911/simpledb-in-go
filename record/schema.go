package record

type FieldType int32

const (
	Integer FieldType = iota
	Varchar
)

type fieldInfo struct {
	fieldType FieldType
	length    int32
}

type Schema struct {
	fields []string
	info   map[string]fieldInfo
}

func NewSchema() *Schema {
	return &Schema{
		info: make(map[string]fieldInfo),
	}
}

func (s *Schema) AddField(fieldName string, fieldType FieldType, length int32) {
	s.fields = append(s.fields, fieldName)
	s.info[fieldName] = fieldInfo{
		fieldType: fieldType,
		length:    length,
	}
}

func (s *Schema) AddIntField(fieldName string) {
	s.AddField(fieldName, Integer, 0)
}

func (s *Schema) AddStringField(fieldName string, length int32) {
	s.AddField(fieldName, Varchar, length)
}

func (s *Schema) Add(fieldName string, schema *Schema) {
	fieldType := schema.FieldType(fieldName)
	length := schema.FieldLength(fieldName)
	s.AddField(fieldName, fieldType, length)
}

func (s *Schema) HasField(fieldName string) bool {
	_, exist := s.info[fieldName]
	return exist
}

func (s *Schema) FieldType(fieldName string) FieldType {
	return s.info[fieldName].fieldType
}

func (s *Schema) FieldLength(fieldName string) int32 {
	return s.info[fieldName].length
}
