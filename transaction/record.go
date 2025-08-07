package transaction

import (
	"simpledb/file"
	"simpledb/log"
)

type RecordType int32

const (
	Checkpoint RecordType = iota
	Start
	Commit
	Rollback
	SetInt
	SetString
)

type Record interface {
	Operator() RecordType
	TxNumber() int32
	Undo(tx *Transaction) error
}

func createLogRecord(log []byte) (record Record, err error) {
	p := file.NewPageFromBuf(log)

	op, err := p.ReadInt32At(0)
	if err != nil {
		return
	}

	switch RecordType(op) {
	case Checkpoint:
		return NewCheckpointRecord()
	case Start:
		return NewStartRecord(p)
	case Commit:
		return NewCommitRecord(p)
	case Rollback:
		return NewRollbackRecord(p)
	case SetInt:
		return NewSetIntRecord(p)
	case SetString:
		return NewSetStringRecord(p)
	default:
		return
	}
}

type CheckpointRecord struct{}

func NewCheckpointRecord() (*CheckpointRecord, error) {
	return &CheckpointRecord{}, nil
}

func (r *CheckpointRecord) Operator() RecordType {
	return Checkpoint
}

func (r *CheckpointRecord) TxNumber() int32 {
	return -1
}

func (r *CheckpointRecord) Undo(tx *Transaction) error {
	// Do nothing because a checkpoint record contains no undo information.
	return nil
}

func WriteCheckpointRecordToLog(logManager *log.Manager) (int32, error) {
	p := file.NewPage(4)

	err := p.WriteInt32At(0, int32(Checkpoint))
	if err != nil {
		return 0, err
	}

	return logManager.Append(p.Buf())
}

type StartRecord struct {
	txNum int32
}

func NewStartRecord(page *file.Page) (*StartRecord, error) {
	txNum, err := page.ReadInt32At(4)
	if err != nil {
		return nil, err
	}
	return &StartRecord{txNum: txNum}, nil
}

func (r *StartRecord) Operator() RecordType {
	return Start
}

func (r *StartRecord) TxNumber() int32 {
	return r.txNum
}

func (r *StartRecord) Undo(tx *Transaction) error {
	// Do nothing because a start record contains no undo information.
	return nil
}

func WriteStartRecordToLog(logManager *log.Manager, txNum int32) (int32, error) {
	p := file.NewPage(2 * 4)

	err := p.WriteInt32At(0, int32(Start))
	if err != nil {
		return 0, err
	}

	err = p.WriteInt32At(4, txNum)
	if err != nil {
		return 0, err
	}

	return logManager.Append(p.Buf())
}

type CommitRecord struct {
	txNum int32
}

func NewCommitRecord(page *file.Page) (*CommitRecord, error) {
	txNum, err := page.ReadInt32At(4)
	if err != nil {
		return nil, err
	}
	return &CommitRecord{txNum: txNum}, nil
}

func (r *CommitRecord) Operator() RecordType {
	return Commit
}

func (r *CommitRecord) TxNumber() int32 {
	return r.txNum
}

func (r *CommitRecord) Undo(tx *Transaction) error {
	// Do nothing because a commit record contains no undo information.
	return nil
}

func WriteCommitRecordToLog(logManager *log.Manager, txNum int32) (int32, error) {
	p := file.NewPage(2 * 4)

	err := p.WriteInt32At(0, int32(Commit))
	if err != nil {
		return 0, err
	}

	err = p.WriteInt32At(4, txNum)
	if err != nil {
		return 0, err
	}

	return logManager.Append(p.Buf())
}

type RollbackRecord struct {
	txNum int32
}

func NewRollbackRecord(page *file.Page) (*RollbackRecord, error) {
	txNum, err := page.ReadInt32At(4)
	if err != nil {
		return nil, err
	}
	return &RollbackRecord{txNum: txNum}, nil
}

func (r *RollbackRecord) Operator() RecordType {
	return Rollback
}

func (r *RollbackRecord) TxNumber() int32 {
	return r.txNum
}

func (r *RollbackRecord) Undo(tx *Transaction) error {
	// Do nothing because a rollback record contains no undo information.
	return nil
}

func WriteRollbackRecordToLog(logManager *log.Manager, txNum int32) (int32, error) {
	p := file.NewPage(2 * 4)

	err := p.WriteInt32At(0, int32(Rollback))
	if err != nil {
		return 0, err
	}

	err = p.WriteInt32At(4, txNum)
	if err != nil {
		return 0, err
	}

	return logManager.Append(p.Buf())
}

type SetIntRecord struct {
	txNum  int32
	offset int32
	val    int32
	block  *file.Block
}

func NewSetIntRecord(page *file.Page) (*SetIntRecord, error) {
	txNum, err := page.ReadInt32At(4)
	if err != nil {
		return nil, err
	}

	filename, err := page.ReadStringAt(8)
	if err != nil {
		return nil, err
	}

	blockNum, err := page.ReadInt32At(8 + page.MaxLength(filename))
	if err != nil {
		return nil, err
	}

	block := file.NewBlock(filename, blockNum)

	offset, err := page.ReadInt32At(8 + page.MaxLength(filename) + 4)
	if err != nil {
		return nil, err
	}

	val, err := page.ReadInt32At(8 + page.MaxLength(filename) + 4 + 4)
	if err != nil {
		return nil, err
	}

	return &SetIntRecord{
		txNum:  txNum,
		offset: offset,
		val:    val,
		block:  block,
	}, nil
}

func (r *SetIntRecord) Operator() RecordType {
	return SetInt
}

func (r *SetIntRecord) TxNumber() int32 {
	return r.txNum
}

func (r *SetIntRecord) Undo(tx *Transaction) error {
	if err := tx.Pin(r.block); err != nil {
		return err
	}

	if err := tx.WriteInt32(r.block, r.offset, r.val, false); err != nil {
		return err
	}

	tx.Unpin(r.block)
	return nil
}

func WriteSetIntRecotrdToLog(logManager *log.Manager, txNum int32, block *file.Block, offset int32, val int32) (int32, error) {
	tpos := int32(4)
	fpos := tpos + 4
	bpos := fpos + 4 + int32(len(block.Filename()))
	opos := bpos + 4
	vpos := opos + 4

	p := file.NewPage(vpos + 4)
	p.WriteInt32At(0, int32(SetInt))
	p.WriteInt32At(tpos, txNum)
	p.WriteStringAt(fpos, block.Filename())
	p.WriteInt32At(bpos, block.Number())
	p.WriteInt32At(opos, offset)
	p.WriteInt32At(vpos, val)

	return logManager.Append(p.Buf())
}

type SetStringRecord struct {
	txNum  int32
	offset int32
	val    string
	block  *file.Block
}

func NewSetStringRecord(page *file.Page) (*SetStringRecord, error) {
	txNum, err := page.ReadInt32At(4)
	if err != nil {
		return nil, err
	}

	filename, err := page.ReadStringAt(8)
	if err != nil {
		return nil, err
	}

	blockNum, err := page.ReadInt32At(8 + page.MaxLength(filename))
	if err != nil {
		return nil, err
	}

	block := file.NewBlock(filename, blockNum)

	offset, err := page.ReadInt32At(8 + page.MaxLength(filename) + 4)
	if err != nil {
		return nil, err
	}

	val, err := page.ReadStringAt(8 + page.MaxLength(filename) + 4 + 4)
	if err != nil {
		return nil, err
	}

	return &SetStringRecord{
		txNum:  txNum,
		offset: offset,
		val:    val,
		block:  block,
	}, nil
}

func (r *SetStringRecord) Operator() RecordType {
	return SetString
}

func (r *SetStringRecord) TxNumber() int32 {
	return r.txNum
}

func (r *SetStringRecord) Undo(tx *Transaction) error {
	if err := tx.Pin(r.block); err != nil {
		return err
	}

	if err := tx.WriteString(r.block, r.offset, r.val, false); err != nil {
		return err
	}

	tx.Unpin(r.block)
	return nil
}

func WriteSetStringRecordToLog(logManager *log.Manager, txNum int32, block *file.Block, offset int32, val string) (int32, error) {
	tpos := int32(4)
	fpos := tpos + 4
	bpos := fpos + 4 + int32(len(block.Filename()))
	opos := bpos + 4
	vpos := opos + 4

	p := file.NewPage(vpos + 4 + int32(len(val)))
	p.WriteInt32At(0, int32(SetString))
	p.WriteInt32At(tpos, txNum)
	p.WriteStringAt(fpos, block.Filename())
	p.WriteInt32At(bpos, block.Number())
	p.WriteInt32At(opos, offset)
	p.WriteStringAt(vpos, val)

	return logManager.Append(p.Buf())
}
