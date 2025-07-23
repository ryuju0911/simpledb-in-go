package log

import (
	"simpledb/file"
)

// Iterator provides a way to read log records from the log file in reverse order.
// It allows clients to iterate over the log records from most recent to oldest.
type Iterator struct {
	fileManager *file.Manager
	block       *file.Block
	page        *file.Page
	currentPos  int32
}

// NewIterator creates a new iterator for the log records in a file, starting
// from a specific block. The iterator is positioned at the most recent log record
// in that block.
func NewIterator(fileManager *file.Manager, block *file.Block) (*Iterator, error) {
	page := file.NewPage(fileManager.BlockSize())

	i := &Iterator{
		fileManager: fileManager,
		block:       block,
		page:        page,
	}

	if err := i.moveToBlock(block); err != nil {
		return nil, err
	}

	return i, nil
}

// HasNext returns true if there are more log records to be read. It checks if
// the iterator has reached the end of the current block and if there are previous
// blocks to move to.
func (i *Iterator) HasNext() bool {
	return i.currentPos < i.fileManager.BlockSize() || i.block.Number() > 0
}

// Next returns the next log record as a byte slice. It reads records from the
// current block. If the end of a block is reached, it automatically moves to the
// previous block to continue iteration. The iteration proceeds from the most
// recent record to the oldest.
func (i *Iterator) Next() ([]byte, error) {
	if i.currentPos == i.fileManager.BlockSize() {
		block := file.NewBlock(i.block.Filename(), i.block.Number()-1)
		if err := i.moveToBlock(block); err != nil {
			return nil, err
		}
		i.block = block
	}

	log, err := i.page.ReadBytesAt(i.currentPos)
	if err != nil {
		return nil, err
	}

	i.currentPos += int32(len(log)) + 4
	return log, nil
}

// moveToBlock loads the contents of a specified block into the iterator's page
// and positions the iterator at the first log record in that block. The log
// records are stored from the end of the block, and the boundary of the used
// space is stored at the beginning of the block.
func (i *Iterator) moveToBlock(block *file.Block) error {
	err := i.fileManager.Read(block, i.page)
	if err != nil {
		return err
	}

	boundary, err := i.page.ReadInt32At(0)
	if err != nil {
		return err
	}
	i.currentPos = boundary

	return nil
}
