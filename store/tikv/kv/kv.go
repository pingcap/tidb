package kv

import (
	"sync"
	"time"

	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/store/tikv/util"
)

// ReturnedValue pairs the Value and AlreadyLocked flag for PessimisticLock return values result.
type ReturnedValue struct {
	Value         []byte
	AlreadyLocked bool
}

// LockCtx contains information for LockKeys method.
type LockCtx struct {
	Killed                *uint32
	ForUpdateTS           uint64
	LockWaitTime          int64
	WaitStartTime         time.Time
	PessimisticLockWaited *int32
	LockKeysDuration      *int64
	LockKeysCount         *int32
	ReturnValues          bool
	Values                map[string]ReturnedValue
	ValuesLock            sync.Mutex
	LockExpired           *uint32
	Stats                 *util.LockKeysDetails
	ResourceGroupTag      []byte
	OnDeadlock            func(*tikverr.ErrDeadlock)
}
