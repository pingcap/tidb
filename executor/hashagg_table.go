package executor

import (
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/goleveldb/leveldb/opt"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
)

// HashAggResultTable interface.
type HashAggResultTable interface {
	Get(key string) ([]aggfuncs.PartialResult, bool, error)
	Put(key string, results []aggfuncs.PartialResult) error
	Foreach(callback func(key string, results []aggfuncs.PartialResult)) error
	Close()
}

type hashAggResultTableImpl struct {
	// dm disk-based map
	sync.RWMutex
	state      int // 0: memory, 1: should spill, 2: spilled, 3: ignore spill
	memResult  map[string][]aggfuncs.PartialResult
	diskResult *leveldb.DB
	memTracker *memory.Tracker
	aggFuncs   []aggfuncs.AggFunc
	tmpDir     string
}

// NewHashAggResultTable allocates a new object.
func NewHashAggResultTable(ctx sessionctx.Context, aggFuncs []aggfuncs.AggFunc, useTmpStorage bool, memTracker *memory.Tracker) *hashAggResultTableImpl {
	t := &hashAggResultTableImpl{
		memResult:  make(map[string][]aggfuncs.PartialResult),
		memTracker: memTracker,
		aggFuncs:   aggFuncs,
	}
	if useTmpStorage {
		oomAction := newHashAggTableImplAction(t)
		ctx.GetSessionVars().StmtCtx.MemTracker.FallbackOldAndSetNewAction(oomAction)
	}
	return t
}

// Get returns value for the key.
func (t *hashAggResultTableImpl) Get(key string) ([]aggfuncs.PartialResult, bool, error) {
	t.RLock()
	defer t.RUnlock()
	if t.state != 2 {
		prs, ok := t.memResult[key]
		return prs, ok, nil
	}
	val, err := t.diskResult.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, false, nil
	} else if err != nil {
		return nil, false, errors.Trace(err)
	}
	prs, err := aggfuncs.DecodePartialResult(t.aggFuncs, val)
	return prs, true, err
}

// Put stores value for a key.
func (t *hashAggResultTableImpl) Put(key string, prs []aggfuncs.PartialResult) error {
	t.Lock()
	if t.state != 2 {
		oldPrs, ok := t.memResult[key]
		oldMem := aggfuncs.PartialResultsMemory(t.aggFuncs, oldPrs)
		newMem := aggfuncs.PartialResultsMemory(t.aggFuncs, prs)
		t.memResult[key] = prs
		t.Unlock()
		delta := newMem - oldMem
		if !ok {
			delta += int64(len(key))
		}
		if delta != 0 && t.state == 0 {
			t.memTracker.Consume(delta)
		}
		return nil
	}

	defer t.Unlock()
	val, err := aggfuncs.EncodePartialResult(t.aggFuncs, prs)
	if err != nil {
		return err
	}
	return errors.Trace(t.diskResult.Put([]byte(key), val, nil))
}

// Foreach iterates the table.
func (t *hashAggResultTableImpl) Foreach(callback func(key string, results []aggfuncs.PartialResult)) error {
	t.RLock()
	defer t.RUnlock()
	if t.state != 2 {
		for key, prs := range t.memResult {
			callback(key, prs)
		}
		return nil
	}
	it := t.diskResult.NewIterator(nil, nil)
	for it.Next() {
		key, val := string(it.Key()), it.Value()
		prs, err := aggfuncs.DecodePartialResult(t.aggFuncs, val)
		if err != nil {
			return err
		}
		callback(key, prs)
	}
	return nil
}

func (t *hashAggResultTableImpl) Close() {
	if t.diskResult != nil {
		terror.Log(t.diskResult.Close())
		t.diskResult = nil
		terror.Log(os.RemoveAll(t.tmpDir))
	}
}

var defaultPartResultFilePrefix = "hashAggPartRes."

func (t *hashAggResultTableImpl) spill() (err error) {
	if err := aggfuncs.SupportDisk(t.aggFuncs); err != nil {
		logutil.BgLogger().Info(err.Error())
		t.state = 3
		return nil
	}
	logutil.BgLogger().Info("spill hash-agg temp result to disk.")
	t.tmpDir, err = ioutil.TempDir(config.GetGlobalConfig().TempStoragePath, defaultPartResultFilePrefix+strconv.Itoa(t.memTracker.Label())+".")
	if err != nil {
		return err
	}
	op := new(opt.Options)
	op.BlockCacheCapacity = opt.GiB
	if t.diskResult, err = leveldb.OpenFile(t.tmpDir, op); err != nil {
		return
	}
	for key, prs := range t.memResult {
		mem := aggfuncs.PartialResultsMemory(t.aggFuncs, prs)
		t.memTracker.ConsumeNoAction(-(mem + int64(len(key))))
		val, err := aggfuncs.EncodePartialResult(t.aggFuncs, prs)
		if err != nil {
			return err
		}
		if err := t.diskResult.Put([]byte(key), val, nil); err != nil {
			return err
		}
	}
	t.memResult = nil
	t.state = 2
	return nil
}

func (t *hashAggResultTableImpl) oomAction() {
	t.Lock()
	defer t.Unlock()
	t.state = 1
	if err := t.spill(); err != nil {
		panic(err)
	}
}

type hashAggTableImplAction struct {
	memory.BaseOOMAction

	t    *hashAggResultTableImpl
	done uint64
}

func newHashAggTableImplAction(t *hashAggResultTableImpl) *hashAggTableImplAction {
	return &hashAggTableImplAction{t: t}
}

// Action implements interface.
func (act *hashAggTableImplAction) Action(t *memory.Tracker) {
	fallback := act.GetFallback()
	if act.t.memTracker.BytesConsumed() == 0 {
		if fallback != nil {
			fallback.Action(t)
		}
		return
	}
	if atomic.CompareAndSwapUint64(&act.done, 0, 1) {
		act.t.oomAction()
		return
	}
	if fallback != nil {
		fallback.Action(t)
	}
}

// SetLogHook implements interface.
func (act *hashAggTableImplAction) SetLogHook(hook func(uint64)) {}

// GetPriority implements interface.
func (act *hashAggTableImplAction) GetPriority() int64 {
	return memory.DefSpillPriority
}
