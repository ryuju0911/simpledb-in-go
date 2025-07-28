package buffer

import (
	"simpledb/file"
	"simpledb/log"
)

type Buffer struct {
	fileManager *file.Manager
	logManager  *log.Manager
	contents    *file.Page
	block       *file.Block
	pins        int32
	modifiedBy  int32 // transaction number that made the change
	lsn         int32 // LSN of the most recent log record
}

func NewBuffer(fileManager *file.Manager, logManager *log.Manager) *Buffer {
	return &Buffer{
		fileManager: fileManager,
		logManager:  logManager,
		contents:    file.NewPage(fileManager.BlockSize()),
		block:       nil,
		pins:        0,
		modifiedBy:  -1,
		lsn:         -1,
	}
}

func (b *Buffer) Contents() *file.Page {
	return b.contents
}

func (b *Buffer) Block() *file.Block {
	return b.block
}

func (b *Buffer) SetModified(txNum, lsn int32) {
	b.modifiedBy = txNum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) ModifyingTx() int32 {
	return b.modifiedBy
}

func (b *Buffer) assignToBlock(block *file.Block) error {
	// Flush the buffer, so that any modifications to the previous block are preserved.
	if err := b.flush(); err != nil {
		return err
	}

	b.block = block
	b.fileManager.Read(block, b.contents)
	b.pins = 0
	return nil
}

func (b *Buffer) flush() error {
	if b.modifiedBy >= 0 {
		if err := b.logManager.Flush(b.lsn); err != nil {
			return err
		}
		if err := b.fileManager.Write(b.block, b.contents); err != nil {
			return err
		}
		b.modifiedBy = -1
	}
	return nil
}

func (b *Buffer) pin() {
	b.pins++
}

func (b *Buffer) unpin() {
	b.pins--
}
