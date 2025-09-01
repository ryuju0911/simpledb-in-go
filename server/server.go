package server

import (
	"simpledb/buffer"
	"simpledb/file"
	"simpledb/log"
	"simpledb/transaction"
)

type SimpleDB struct {
	fileManager   *file.Manager
	logManager    *log.Manager
	bufferManager *buffer.Manager
}

func NewSimpleDB(dirName string, blockSize int32, buffSize int32) *SimpleDB {
	fileManager, _ := file.NewManager(dirName, blockSize)
	logManager, _ := log.NewManager(fileManager, "simpledb.log")
	bufferManager := buffer.NewManager(fileManager, logManager, buffSize)

	return &SimpleDB{
		fileManager:   fileManager,
		logManager:    logManager,
		bufferManager: bufferManager,
	}
}

func (s *SimpleDB) NewTx() *transaction.Transaction {
	tx, _ := transaction.NewTransaction(s.fileManager, s.logManager, s.bufferManager)
	return tx
}
