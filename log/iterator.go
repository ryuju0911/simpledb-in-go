package log

import (
	"simpledb/file"
)

type Iterator struct{}

func NewIterator(fileManager *file.Manager, block *file.Block) (*Iterator, error) {
	return &Iterator{}, nil
}
