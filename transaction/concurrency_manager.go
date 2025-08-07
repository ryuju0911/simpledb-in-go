package transaction

import "simpledb/file"

type ConcurrencyManager struct {
	lockTable *LockTable
	locks     map[*file.Block]string
}

func NewConcurrencyManager(lockTable *LockTable) *ConcurrencyManager {
	return &ConcurrencyManager{
		lockTable: lockTable,
		locks:     make(map[*file.Block]string),
	}
}

func (cm *ConcurrencyManager) SLock(block *file.Block) error {
	if _, exit := cm.locks[block]; exit {
		return nil
	}

	err := cm.lockTable.SLock(block)
	if err != nil {
		return err
	}

	cm.locks[block] = "S"
	return nil
}

func (cm *ConcurrencyManager) XLock(block *file.Block) error {
	if cm.hasXLock(block) {
		return nil
	}

	// Transaction having an xlock on a block also has an implied slock on it.
	err := cm.SLock(block)
	if err != nil {
		return err
	}

	err = cm.lockTable.XLock(block)
	if err != nil {
		return err
	}

	cm.locks[block] = "X"
	return nil
}

func (cm *ConcurrencyManager) Release() {
	for block := range cm.locks {
		cm.lockTable.Unlock(block)
	}
	clear(cm.locks)
}

func (cm *ConcurrencyManager) hasXLock(block *file.Block) bool {
	lock, exit := cm.locks[block]
	return exit && lock == "X"
}
