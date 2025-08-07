package transaction

import (
	"sync"
	"sync/atomic"

	"simpledb/buffer"
	"simpledb/file"
	"simpledb/log"
)

var transactionNumber atomic.Int32

type Transaction struct {
	mu                 sync.Mutex
	txNum              int32
	nextTxNum          int32
	fileManager        *file.Manager
	logManager         *log.Manager
	bufferManager      *buffer.Manager
	recoveryManager    *RecoveryManager
	concurrencyManager *ConcurrencyManager
	bufferList         *BufferList
}

func NewTransaction(fileManager *file.Manager, logManager *log.Manager, bufferManager *buffer.Manager) (*Transaction, error) {
	tx := &Transaction{
		fileManager:   fileManager,
		logManager:    logManager,
		bufferManager: bufferManager,
	}

	txNum := transactionNumber.Add(1)
	recoveryManager, err := NewRecoveryManager(logManager, bufferManager, tx, txNum)
	if err != nil {
		return nil, err
	}

	lockTable := NewLockTable()
	concurrencyManager := NewConcurrencyManager(lockTable)

	bufferList := NewBufferList(bufferManager)

	tx.txNum = txNum
	tx.recoveryManager = recoveryManager
	tx.concurrencyManager = concurrencyManager
	tx.bufferList = bufferList

	return tx, nil
}

func (tx *Transaction) Commit() error {
	if err := tx.recoveryManager.Commit(); err != nil {
		return err
	}

	tx.concurrencyManager.Release()
	tx.bufferList.UnpinAll()
	return nil
}

func (tx *Transaction) Rollback() error {
	if err := tx.recoveryManager.Recover(); err != nil {
		return err
	}

	tx.concurrencyManager.Release()
	tx.bufferList.UnpinAll()
	return nil
}

func (tx *Transaction) Recover() error {
	if err := tx.bufferManager.FlushAll(tx.txNum); err != nil {
		return err
	}

	if err := tx.recoveryManager.Recover(); err != nil {
		return err
	}

	return nil
}

func (tx *Transaction) Pin(block *file.Block) error {
	return tx.bufferList.Pin(block)
}

func (tx *Transaction) Unpin(block *file.Block) {
	tx.bufferList.Unpin(block)
}

func (tx *Transaction) ReadInt32(block *file.Block, offset int32) (int32, error) {
	err := tx.concurrencyManager.SLock(block)
	if err != nil {
		return 0, err
	}

	buf := tx.bufferList.GetBuffer(block)
	return buf.Contents().ReadInt32At(offset)
}

func (tx *Transaction) ReadString(block *file.Block, offset int32) (string, error) {
	err := tx.concurrencyManager.SLock(block)
	if err != nil {
		return "", err
	}

	buf := tx.bufferList.GetBuffer(block)
	return buf.Contents().ReadStringAt(offset)
}

func (tx *Transaction) WriteInt32(block *file.Block, offset int32, val int32, log bool) error {
	if err := tx.concurrencyManager.XLock(block); err != nil {
		return err
	}

	buf := tx.bufferList.GetBuffer(block)
	lsn := int32(-1)
	if log {
		var err error
		lsn, err = tx.recoveryManager.SetInt(buf, offset, val)
		if err != nil {
			return err
		}
	}

	if err := buf.Contents().WriteInt32At(offset, val); err != nil {
		return err
	}

	buf.SetModified(tx.txNum, lsn)
	return nil
}

func (tx *Transaction) WriteString(block *file.Block, offset int32, val string, log bool) error {
	if err := tx.concurrencyManager.XLock(block); err != nil {
		return err
	}

	buf := tx.bufferList.GetBuffer(block)
	lsn := int32(-1)
	if log {
		var err error
		lsn, err = tx.recoveryManager.SetString(buf, offset, val)
		if err != nil {
			return err
		}
	}

	if err := buf.Contents().WriteStringAt(offset, val); err != nil {
		return err
	}

	buf.SetModified(tx.txNum, lsn)
	return nil
}

func (tx *Transaction) Size(filename string) (int32, error) {
	dummyBlock := file.NewBlock(filename, -1)
	if err := tx.concurrencyManager.SLock(dummyBlock); err != nil {
		return 0, err
	}
	return tx.fileManager.Size(filename)
}

func (tx *Transaction) Append(filename string) (*file.Block, error) {
	dummyBlock := file.NewBlock(filename, -1)
	if err := tx.concurrencyManager.XLock(dummyBlock); err != nil {
		return nil, err
	}
	return tx.fileManager.Append(filename)
}

func (tx *Transaction) BlockSize() int32 {
	return tx.fileManager.BlockSize()
}

func (tx *Transaction) AvailableBuffers() int32 {
	return tx.bufferManager.Available()
}
