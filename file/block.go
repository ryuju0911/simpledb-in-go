package file

type Block struct {
	filename string
	number   int32
}

func NewBlock(filename string, number int32) *Block {
	return &Block{filename, number}
}

func (b *Block) Filename() string {
	return b.filename
}

func (b *Block) Number() int32 {
	return b.number
}
