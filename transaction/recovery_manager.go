package transaction

import (
	"simpledb/buffer"
	"simpledb/log"
)

type RecoveryManager struct {
	logManager    *log.Manager
	bufferManager *buffer.Manager
	tx            *Transaction
	txNum         int32
}

func NewRecoveryManager(logManager *log.Manager, bufferManager *buffer.Manager, tx *Transaction, txNum int32) (*RecoveryManager, error) {
	_, err := WriteStartRecordToLog(logManager, txNum)
	if err != nil {
		return nil, err
	}

	return &RecoveryManager{
		logManager:    logManager,
		bufferManager: bufferManager,
		tx:            tx,
		txNum:         txNum,
	}, nil
}

func (m *RecoveryManager) Commit() error {
	err := m.bufferManager.FlushAll(m.txNum)
	if err != nil {
		return err
	}

	lsn, err := WriteCommitRecordToLog(m.logManager, m.txNum)
	if err != nil {
		return err
	}

	return m.logManager.Flush(lsn)
}

func (m *RecoveryManager) Rollback() error {
	err := m.doRollback()
	if err != nil {
		return err
	}

	err = m.bufferManager.FlushAll(m.txNum)
	if err != nil {
		return err
	}

	lsn, err := WriteRollbackRecordToLog(m.logManager, m.txNum)
	if err != nil {
		return err
	}

	return m.logManager.Flush(lsn)
}

func (m *RecoveryManager) Recover() error {
	err := m.doRecover()
	if err != nil {
		return err
	}

	err = m.bufferManager.FlushAll(m.txNum)
	if err != nil {
		return err
	}

	lsn, err := WriteRollbackRecordToLog(m.logManager, m.txNum)
	if err != nil {
		return err
	}

	return m.logManager.Flush(lsn)
}

func (m *RecoveryManager) SetInt(buf *buffer.Buffer, offset int32, newVal int32) (int32, error) {
	oldVal, err := buf.Contents().ReadInt32At(offset)
	if err != nil {
		return 0, err
	}

	return WriteSetIntRecotrdToLog(m.logManager, m.txNum, buf.Block(), offset, oldVal)
}

func (m *RecoveryManager) SetString(buf *buffer.Buffer, offset int32, newVal string) (int32, error) {
	oldVal, err := buf.Contents().ReadStringAt(offset)
	if err != nil {
		return 0, err
	}

	return WriteSetStringRecordToLog(m.logManager, m.txNum, buf.Block(), offset, oldVal)
}

func (m *RecoveryManager) doRollback() error {
	iter, err := m.logManager.Iterator()
	if err != nil {
		return err
	}

	for iter.HasNext() {
		log, err := iter.Next()
		if err != nil {
			return err
		}

		record, err := createLogRecord(log)
		if err != nil {
			return err
		}

		if record.TxNumber() == m.txNum {
			if record.Operator() == Start {
				return nil
			}
			record.Undo(m.tx)
		}
	}

	return nil
}

func (m *RecoveryManager) doRecover() error {
	finishedTxs := make(map[int32]bool)
	iter, err := m.logManager.Iterator()
	if err != nil {
		return err
	}

	for iter.HasNext() {
		log, err := iter.Next()
		if err != nil {
			return err
		}

		record, err := createLogRecord(log)
		if err != nil {
			return err
		}

		if record.Operator() == Checkpoint {
			return nil
		}

		if record.Operator() == Commit || record.Operator() == Rollback {
			finishedTxs[record.TxNumber()] = true
		} else if !finishedTxs[record.TxNumber()] {
			if err := record.Undo(m.tx); err != nil {
				return err
			}
		}
	}

	return nil
}
