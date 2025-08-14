package record

import (
	"simpledb/file"
	"simpledb/transaction"
)

type Page struct {
	tx     *transaction.Transaction
	block  *file.Block
	layout *Layout
}

func NewPage(tx *transaction.Transaction, block *file.Block, layout *Layout) (*Page, error) {
	if err := tx.Pin(block); err != nil {
		return nil, err
	}
	return &Page{
		tx:     tx,
		block:  block,
		layout: layout,
	}, nil
}

func (p *Page) ReadInt32(slot int32, fieldName string) (int32, error) {
	pos := p.offest(slot) + p.layout.Offset(fieldName)
	return p.tx.ReadInt32(p.block, pos)
}

func (p *Page) ReadString(slot int32, fieldName string) (string, error) {
	pos := p.offest(slot) + p.layout.Offset(fieldName)
	return p.tx.ReadString(p.block, pos)
}

func (p *Page) WriteInt32(slot int32, fieldName string, value int32) error {
	pos := p.offest(slot) + p.layout.Offset(fieldName)
	return p.tx.WriteInt32(p.block, pos, value, true)
}

func (p *Page) WriteString(slot int32, fieldName string, value string) error {
	pos := p.offest(slot) + p.layout.Offset(fieldName)
	return p.tx.WriteString(p.block, pos, value, true)
}

func (p *Page) Delete(slot int32) error {
	return p.setFlag(slot, 0)
}

func (p *Page) Format() error {
	var slot int32
	for p.isValidSlot(slot) {
		if err := p.tx.WriteInt32(p.block, p.offest(slot), 0, false); err != nil {
			return err
		}

		schema := p.layout.Schema()
		for _, fieldName := range schema.fields {
			pos := p.offest(slot) + p.layout.Offset(fieldName)
			if schema.FieldType(fieldName) == Integer {
				if err := p.tx.WriteInt32(p.block, pos, 0, false); err != nil {
					return err
				}
			} else {
				if err := p.tx.WriteString(p.block, pos, "", false); err != nil {
					return err
				}
			}
		}
		slot++
	}

	return nil
}

func (p *Page) NextAfter(slot int32) (int32, error) {
	return p.searchAfter(slot, 1)
}

func (p *Page) InsertAfter(slot int32) (int32, error) {
	newSlot, err := p.searchAfter(slot, 0)
	if err != nil {
		return 0, err
	}
	if newSlot >= 0 {
		if err := p.setFlag(newSlot, 1); err != nil {
			return 0, err
		}
	}
	return newSlot, nil
}

func (p *Page) Block() *file.Block {
	return p.block
}

func (p *Page) setFlag(slot int32, flag int32) error {
	return p.tx.WriteInt32(p.block, p.offest(slot), flag, true)
}

func (p *Page) searchAfter(slot int32, flag int32) (int32, error) {
	slot++
	for p.isValidSlot(slot) {
		value, err := p.tx.ReadInt32(p.block, p.offest(slot))
		if err != nil {
			return 0, err
		}
		if value == flag {
			return slot, nil
		}
		slot++
	}
	return -1, nil
}

func (p *Page) isValidSlot(slot int32) bool {
	return p.offest(slot+1) <= p.tx.BlockSize()
}

func (p *Page) offest(slot int32) int32 {
	return slot * p.layout.SlotSize()
}
