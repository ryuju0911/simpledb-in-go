package transaction

import (
	"slices"

	"simpledb/buffer"
	"simpledb/file"
)

type BufferList struct {
	buffers       map[*file.Block]*buffer.Buffer
	pins          []*file.Block
	bufferManager *buffer.Manager
}

func NewBufferList(bufferManager *buffer.Manager) *BufferList {
	return &BufferList{
		buffers:       make(map[*file.Block]*buffer.Buffer),
		pins:          make([]*file.Block, 0),
		bufferManager: bufferManager,
	}
}

func (bl *BufferList) GetBuffer(block *file.Block) *buffer.Buffer {
	return bl.buffers[block]
}

func (bl *BufferList) Pin(block *file.Block) error {
	buf, err := bl.bufferManager.Pin(block)
	if err != nil {
		return err
	}

	bl.buffers[block] = buf
	bl.pins = append(bl.pins, block)
	return nil
}

func (bl *BufferList) Unpin(block *file.Block) {
	buf := bl.buffers[block]
	if buf == nil {
		return
	}

	bl.bufferManager.Unpin(buf)
	_ = slices.DeleteFunc(bl.pins, func(b *file.Block) bool {
		return b == block
	})
	if !slices.Contains(bl.pins, block) {
		delete(bl.buffers, block)
	}
}

func (bl *BufferList) UnpinAll() {
	for _, block := range bl.pins {
		buf := bl.buffers[block]
		if buf == nil {
			continue
		}
		bl.bufferManager.Unpin(buf)
	}

	clear(bl.buffers)
	clear(bl.pins)
}
