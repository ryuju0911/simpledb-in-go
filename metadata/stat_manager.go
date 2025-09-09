package metadata

import (
	"sync"

	"simpledb/record"
	"simpledb/transaction"
)

type StatInfo struct {
	numBlocks  int32
	numRecords int32
}

func NewStatInfo(numBlocks int32, numRecords int32) StatInfo {
	return StatInfo{numBlocks: numBlocks, numRecords: numRecords}
}

func (si StatInfo) BlocksAccessed() int32 {
	return si.numBlocks
}

func (si StatInfo) RecordsOutput() int32 {
	return si.numRecords
}

// DistinctValues assumes that approximately 1/3 of the values of any field are distinct.
func (si StatInfo) DistinctValues(fieldName string) int32 {
	return 1 + (si.numRecords / 3) // this is widely inaccurate
}

type StatManager struct {
	mu           sync.Mutex
	tableManager *TableManager
	tableStats   map[string]StatInfo
	numCalls     int32
}

func NewStatManager(tableManager *TableManager) *StatManager {
	return &StatManager{
		tableManager: tableManager,
		tableStats:   make(map[string]StatInfo),
		numCalls:     0,
	}
}

func (sm *StatManager) GetStatInfo(tableName string, layout *record.Layout, tx *transaction.Transaction) StatInfo {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.numCalls++
	if sm.numCalls > 100 {
		sm.refreshStatisics(tx)
	}

	info, exist := sm.tableStats[tableName]
	if !exist {
		info = sm.calcTableStats(tableName, layout, tx)
		sm.tableStats[tableName] = info
	}
	return info
}

func (sm *StatManager) refreshStatisics(tx *transaction.Transaction) {
	tableStats := make(map[string]StatInfo)
	sm.numCalls = 0

	tcatLayout := sm.tableManager.GetLayout("tblcat", tx)
	tcat, _ := record.NewTableScan(tx, "tblcat", tcatLayout)
	for tcat.Next() {
		tableName, _ := tcat.ReadString("tblname")
		layout := sm.tableManager.GetLayout(tableName, tx)
		info := sm.calcTableStats(tableName, layout, tx)
		tableStats[tableName] = info
	}
	tcat.Close()
}

func (sm *StatManager) calcTableStats(tableName string, layout *record.Layout, tx *transaction.Transaction) StatInfo {
	var numRecords, numBlocks int32
	tableScan, _ := record.NewTableScan(tx, tableName, layout)
	for tableScan.Next() {
		numRecords++
		numBlocks = tableScan.GetRID().BlockNumber() + 1
	}
	tableScan.Close()
	return NewStatInfo(numBlocks, numRecords)
}
