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

// InitReturnValues creates the map to store returned value.
func (ctx *LockCtx) InitReturnValues(valueLen int) {
	ctx.ReturnValues = true
	ctx.Values = make(map[string]ReturnedValue, valueLen)
}

// GetValueNotLocked returns a value if the key is not already locked.
// (nil, false) means already locked.
func (ctx *LockCtx) GetValueNotLocked(key []byte) ([]byte, bool) {
	rv := ctx.Values[string(key)]
	if !rv.AlreadyLocked {
		return rv.Value, true
	}
	return nil, false
}

// IterateValuesNotLocked applies f to all key-values that are not already
// locked.
func (ctx *LockCtx) IterateValuesNotLocked(f func([]byte, []byte)) {
	ctx.ValuesLock.Lock()
	defer ctx.ValuesLock.Unlock()
	for key, val := range ctx.Values {
		if !val.AlreadyLocked {
			f([]byte(key), val.Value)
		}
	}
}
