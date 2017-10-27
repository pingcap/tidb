package tikv

import (
	"flag"
	"fmt"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/store/tikv/oracle"
	goctx "golang.org/x/net/context"
	"sync"
	"time"
)

var errStopped = errors.New("stopped")

type MockOracle struct {
	sync.RWMutex
	stop   bool
	offset time.Duration
	lastTS uint64
}

func (o *MockOracle) enable() {
	o.Lock()
	defer o.Unlock()
	o.stop = false
}

func (o *MockOracle) disable() {
	o.Lock()
	defer o.Unlock()
	o.stop = true
}

func (o *MockOracle) setOffset(offset time.Duration) {
	o.Lock()
	defer o.Unlock()

	o.offset = offset
}

func (o *MockOracle) AddOffset(d time.Duration) {
	o.Lock()
	defer o.Unlock()

	o.offset += d
}

func (o *MockOracle) GetTimestamp(goctx.Context) (uint64, error) {
	o.Lock()
	defer o.Unlock()

	if o.stop {
		return 0, errors.Trace(errStopped)
	}
	physical := oracle.GetPhysical(time.Now().Add(o.offset))
	ts := oracle.ComposeTS(physical, 0)
	if oracle.ExtractPhysical(o.lastTS) == physical {
		ts = o.lastTS + 1
	}
	o.lastTS = ts
	return ts, nil
}

type mockOracleFuture struct {
	o   *MockOracle
	ctx goctx.Context
}

func (m *mockOracleFuture) Wait() (uint64, error) {
	return m.o.GetTimestamp(m.ctx)
}

func (o *MockOracle) GetTimestampAsync(ctx goctx.Context) oracle.Future {
	return &mockOracleFuture{o, ctx}
}

func (o *MockOracle) IsExpired(lockTimestamp uint64, TTL uint64) bool {
	o.RLock()
	defer o.RUnlock()

	return oracle.GetPhysical(time.Now().Add(o.offset)) >= oracle.ExtractPhysical(lockTimestamp)+int64(TTL)
}

func (o *MockOracle) Close() {

}

// NewTestTiKVStorage creates a TiKVStorage for test.
func NewTestTiKVStorage(withTiKV bool, pdAddrs string) (TiKVStorage, error) {
	if !flag.Parsed() {
		flag.Parse()
	}

	if withTiKV {
		var d Driver
		store, err := d.Open(fmt.Sprintf("tikv://%s", pdAddrs))
		if err != nil {
			return nil, errors.Trace(err)
		}
		return store.(TiKVStorage), nil
	}
	store, err := NewMockTikvStore()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return store.(TiKVStorage), nil
}
