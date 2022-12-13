// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"context"
	"encoding/hex"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/store/tikv/client"
	"github.com/pingcap/tidb/store/tikv/config"
	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/retry"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/store/tikv/unionstore"
	"github.com/pingcap/tidb/store/tikv/util"
	"github.com/prometheus/client_golang/prometheus"
	zap "go.uber.org/zap"
)

// If the duration of a single request exceeds the slowRequestThreshold, a warning log will be logged.
const slowRequestThreshold = time.Minute

type twoPhaseCommitAction interface {
	handleSingleBatch(*twoPhaseCommitter, *Backoffer, batchMutations) error
	tiKVTxnRegionsNumHistogram() prometheus.Observer
	String() string
}

// Global variable set by config file.
var (
	ManagedLockTTL uint64 = 20000 // 20s
)

// twoPhaseCommitter executes a two-phase commit protocol.
type twoPhaseCommitter struct {
	store               *KVStore
	txn                 *KVTxn
	startTS             uint64
	mutations           *memBufferMutations
	lockTTL             uint64
	commitTS            uint64
	priority            pb.CommandPri
	sessionID           uint64 // sessionID is used for log.
	cleanWg             sync.WaitGroup
	detail              unsafe.Pointer
	txnSize             int
	hasNoNeedCommitKeys bool

	primaryKey  []byte
	forUpdateTS uint64

	mu struct {
		sync.RWMutex
		undeterminedErr error // undeterminedErr saves the rpc error we encounter when commit primary key.
		committed       bool
	}
	syncLog bool
	// For pessimistic transaction
	isPessimistic bool
	isFirstLock   bool
	// regionTxnSize stores the number of keys involved in each region
	regionTxnSize map[uint64]int
	// Used by pessimistic transaction and large transaction.
	ttlManager

	testingKnobs struct {
		acAfterCommitPrimary chan struct{}
		bkAfterCommitPrimary chan struct{}
		noFallBack           bool
	}

	useAsyncCommit    uint32
	minCommitTS       uint64
	maxCommitTS       uint64
	prewriteStarted   bool
	prewriteCancelled uint32
	useOnePC          uint32
	onePCCommitTS     uint64

	hasTriedAsyncCommit bool
	hasTriedOnePC       bool

	// doingAmend means the amend prewrite is ongoing.
	doingAmend bool

	binlog BinlogExecutor

	resourceGroupTag []byte
}

type memBufferMutations struct {
	storage *unionstore.MemDB
	handles []unionstore.MemKeyHandle
}

func newMemBufferMutations(sizeHint int, storage *unionstore.MemDB) *memBufferMutations {
	return &memBufferMutations{
		handles: make([]unionstore.MemKeyHandle, 0, sizeHint),
		storage: storage,
	}
}

func (m *memBufferMutations) Len() int {
	return len(m.handles)
}

func (m *memBufferMutations) GetKey(i int) []byte {
	return m.storage.GetKeyByHandle(m.handles[i])
}

func (m *memBufferMutations) GetKeys() [][]byte {
	ret := make([][]byte, m.Len())
	for i := range ret {
		ret[i] = m.GetKey(i)
	}
	return ret
}

func (m *memBufferMutations) GetValue(i int) []byte {
	v, _ := m.storage.GetValueByHandle(m.handles[i])
	return v
}

func (m *memBufferMutations) GetOp(i int) pb.Op {
	return pb.Op(m.handles[i].UserData >> 1)
}

func (m *memBufferMutations) IsPessimisticLock(i int) bool {
	return m.handles[i].UserData&1 != 0
}

func (m *memBufferMutations) Slice(from, to int) CommitterMutations {
	return &memBufferMutations{
		handles: m.handles[from:to],
		storage: m.storage,
	}
}

func (m *memBufferMutations) Push(op pb.Op, isPessimisticLock bool, handle unionstore.MemKeyHandle) {
	aux := uint16(op) << 1
	if isPessimisticLock {
		aux |= 1
	}
	handle.UserData = aux
	m.handles = append(m.handles, handle)
}

// CommitterMutations contains the mutations to be submitted.
type CommitterMutations interface {
	Len() int
	GetKey(i int) []byte
	GetKeys() [][]byte
	GetOp(i int) pb.Op
	GetValue(i int) []byte
	IsPessimisticLock(i int) bool
	Slice(from, to int) CommitterMutations
}

// PlainMutations contains transaction operations.
type PlainMutations struct {
	ops               []pb.Op
	keys              [][]byte
	values            [][]byte
	isPessimisticLock []bool
}

// NewPlainMutations creates a PlainMutations object with sizeHint reserved.
func NewPlainMutations(sizeHint int) PlainMutations {
	return PlainMutations{
		ops:               make([]pb.Op, 0, sizeHint),
		keys:              make([][]byte, 0, sizeHint),
		values:            make([][]byte, 0, sizeHint),
		isPessimisticLock: make([]bool, 0, sizeHint),
	}
}

// Slice return a sub mutations in range [from, to).
func (c *PlainMutations) Slice(from, to int) CommitterMutations {
	var res PlainMutations
	res.keys = c.keys[from:to]
	if c.ops != nil {
		res.ops = c.ops[from:to]
	}
	if c.values != nil {
		res.values = c.values[from:to]
	}
	if c.isPessimisticLock != nil {
		res.isPessimisticLock = c.isPessimisticLock[from:to]
	}
	return &res
}

// Push another mutation into mutations.
func (c *PlainMutations) Push(op pb.Op, key []byte, value []byte, isPessimisticLock bool) {
	c.ops = append(c.ops, op)
	c.keys = append(c.keys, key)
	c.values = append(c.values, value)
	c.isPessimisticLock = append(c.isPessimisticLock, isPessimisticLock)
}

// Len returns the count of mutations.
func (c *PlainMutations) Len() int {
	return len(c.keys)
}

// GetKey returns the key at index.
func (c *PlainMutations) GetKey(i int) []byte {
	return c.keys[i]
}

// GetKeys returns the keys.
func (c *PlainMutations) GetKeys() [][]byte {
	return c.keys
}

// GetOps returns the key ops.
func (c *PlainMutations) GetOps() []pb.Op {
	return c.ops
}

// GetValues returns the key values.
func (c *PlainMutations) GetValues() [][]byte {
	return c.values
}

// GetPessimisticFlags returns the key pessimistic flags.
func (c *PlainMutations) GetPessimisticFlags() []bool {
	return c.isPessimisticLock
}

// GetOp returns the key op at index.
func (c *PlainMutations) GetOp(i int) pb.Op {
	return c.ops[i]
}

// GetValue returns the key value at index.
func (c *PlainMutations) GetValue(i int) []byte {
	if len(c.values) <= i {
		return nil
	}
	return c.values[i]
}

// IsPessimisticLock returns the key pessimistic flag at index.
func (c *PlainMutations) IsPessimisticLock(i int) bool {
	return c.isPessimisticLock[i]
}

// PlainMutation represents a single transaction operation.
type PlainMutation struct {
	KeyOp             pb.Op
	Key               []byte
	Value             []byte
	IsPessimisticLock bool
}

// MergeMutations append input mutations into current mutations.
func (c *PlainMutations) MergeMutations(mutations PlainMutations) {
	c.ops = append(c.ops, mutations.ops...)
	c.keys = append(c.keys, mutations.keys...)
	c.values = append(c.values, mutations.values...)
	c.isPessimisticLock = append(c.isPessimisticLock, mutations.isPessimisticLock...)
}

// AppendMutation merges a single Mutation into the current mutations.
func (c *PlainMutations) AppendMutation(mutation PlainMutation) {
	c.ops = append(c.ops, mutation.KeyOp)
	c.keys = append(c.keys, mutation.Key)
	c.values = append(c.values, mutation.Value)
	c.isPessimisticLock = append(c.isPessimisticLock, mutation.IsPessimisticLock)
}

// newTwoPhaseCommitter creates a twoPhaseCommitter.
func newTwoPhaseCommitter(txn *KVTxn, sessionID uint64) (*twoPhaseCommitter, error) {
	return &twoPhaseCommitter{
		store:         txn.store,
		txn:           txn,
		startTS:       txn.StartTS(),
		sessionID:     sessionID,
		regionTxnSize: map[uint64]int{},
		ttlManager: ttlManager{
			ch: make(chan struct{}),
		},
		isPessimistic: txn.IsPessimistic(),
		binlog:        txn.binlog,
	}, nil
}

func (c *twoPhaseCommitter) extractKeyExistsErr(err *tikverr.ErrKeyExist) error {
	if !c.txn.us.HasPresumeKeyNotExists(err.GetKey()) {
		return errors.Errorf("session %d, existErr for key:%s should not be nil", c.sessionID, err.GetKey())
	}
	return errors.Trace(err)
}

// KVFilter is a filter that filters out unnecessary KV pairs.
type KVFilter interface {
	// IsUnnecessaryKeyValue returns whether this KV pair should be committed.
	IsUnnecessaryKeyValue(key, value []byte, flags kv.KeyFlags) (bool, error)
}

func (c *twoPhaseCommitter) initKeysAndMutations() error {
	var size, putCnt, delCnt, lockCnt, checkCnt int

	txn := c.txn
	memBuf := txn.GetMemBuffer()
	sizeHint := txn.us.GetMemBuffer().Len()
	c.mutations = newMemBufferMutations(sizeHint, memBuf)
	c.isPessimistic = txn.IsPessimistic()
	filter := txn.kvFilter

	var err error
	for it := memBuf.IterWithFlags(nil, nil); it.Valid(); err = it.Next() {
		_ = err
		key := it.Key()
		flags := it.Flags()
		var value []byte
		var op pb.Op

		if !it.HasValue() {
			if !flags.HasLocked() {
				continue
			}
			op = pb.Op_Lock
			lockCnt++
		} else {
			value = it.Value()
			if len(value) > 0 {
				var isUnnecessaryKV bool
				if filter != nil {
					isUnnecessaryKV, err = filter.IsUnnecessaryKeyValue(key, value, flags)
					if err != nil {
						return err
					}
				}
				if isUnnecessaryKV {
					if !flags.HasLocked() {
						continue
					}
					// If the key was locked before, we should prewrite the lock even if
					// the KV needn't be committed according to the filter. Otherwise, we
					// were forgetting removing pessimistic locks added before.
					op = pb.Op_Lock
					lockCnt++
				} else {
					op = pb.Op_Put
					if flags.HasPresumeKeyNotExists() {
						op = pb.Op_Insert
					}
					putCnt++
				}
			} else {
				if !txn.IsPessimistic() && flags.HasPresumeKeyNotExists() {
					// delete-your-writes keys in optimistic txn need check not exists in prewrite-phase
					// due to `Op_CheckNotExists` doesn't prewrite lock, so mark those keys should not be used in commit-phase.
					op = pb.Op_CheckNotExists
					checkCnt++
					memBuf.UpdateFlags(key, kv.SetPrewriteOnly)
				} else {
					// normal delete keys in optimistic txn can be delete without not exists checking
					// delete-your-writes keys in pessimistic txn can ensure must be no exists so can directly delete them
					op = pb.Op_Del
					delCnt++
				}
			}
		}

		var isPessimistic bool
		if flags.HasLocked() {
			isPessimistic = c.isPessimistic
		}
		c.mutations.Push(op, isPessimistic, it.Handle())
		size += len(key) + len(value)

		if len(c.primaryKey) == 0 && op != pb.Op_CheckNotExists {
			c.primaryKey = key
		}
	}

	if c.mutations.Len() == 0 {
		return nil
	}
	c.txnSize = size

	const logEntryCount = 10000
	const logSize = 4 * 1024 * 1024 // 4MB
	if c.mutations.Len() > logEntryCount || size > logSize {
		logutil.BgLogger().Info("[BIG_TXN]",
			zap.Uint64("session", c.sessionID),
			zap.String("key sample", kv.StrKey(c.mutations.GetKey(0))),
			zap.Int("size", size),
			zap.Int("keys", c.mutations.Len()),
			zap.Int("puts", putCnt),
			zap.Int("dels", delCnt),
			zap.Int("locks", lockCnt),
			zap.Int("checks", checkCnt),
			zap.Uint64("txnStartTS", txn.startTS))
	}

	// Sanity check for startTS.
	if txn.StartTS() == math.MaxUint64 {
		err = errors.Errorf("try to commit with invalid txnStartTS: %d", txn.StartTS())
		logutil.BgLogger().Error("commit failed",
			zap.Uint64("session", c.sessionID),
			zap.Error(err))
		return errors.Trace(err)
	}

	commitDetail := &util.CommitDetails{WriteSize: size, WriteKeys: c.mutations.Len()}
	metrics.TiKVTxnWriteKVCountHistogram.Observe(float64(commitDetail.WriteKeys))
	metrics.TiKVTxnWriteSizeHistogram.Observe(float64(commitDetail.WriteSize))
	c.hasNoNeedCommitKeys = checkCnt > 0
	c.lockTTL = txnLockTTL(txn.startTime, size)
	c.priority = txn.priority.ToPB()
	c.syncLog = txn.syncLog
	c.resourceGroupTag = txn.resourceGroupTag
	c.setDetail(commitDetail)
	return nil
}

func (c *twoPhaseCommitter) primary() []byte {
	if len(c.primaryKey) == 0 {
		return c.mutations.GetKey(0)
	}
	return c.primaryKey
}

// asyncSecondaries returns all keys that must be checked in the recovery phase of an async commit.
func (c *twoPhaseCommitter) asyncSecondaries() [][]byte {
	secondaries := make([][]byte, 0, c.mutations.Len())
	for i := 0; i < c.mutations.Len(); i++ {
		k := c.mutations.GetKey(i)
		if bytes.Equal(k, c.primary()) || c.mutations.GetOp(i) == pb.Op_CheckNotExists {
			continue
		}
		secondaries = append(secondaries, k)
	}
	return secondaries
}

const bytesPerMiB = 1024 * 1024

func txnLockTTL(startTime time.Time, txnSize int) uint64 {
	// Increase lockTTL for large transactions.
	// The formula is `ttl = ttlFactor * sqrt(sizeInMiB)`.
	// When writeSize is less than 256KB, the base ttl is defaultTTL (3s);
	// When writeSize is 1MiB, 4MiB, or 10MiB, ttl is 6s, 12s, 20s correspondingly;
	lockTTL := defaultLockTTL
	if txnSize >= txnCommitBatchSize {
		sizeMiB := float64(txnSize) / bytesPerMiB
		lockTTL = uint64(float64(ttlFactor) * math.Sqrt(sizeMiB))
		if lockTTL < defaultLockTTL {
			lockTTL = defaultLockTTL
		}
		if lockTTL > ManagedLockTTL {
			lockTTL = ManagedLockTTL
		}
	}

	// Increase lockTTL by the transaction's read time.
	// When resolving a lock, we compare current ts and startTS+lockTTL to decide whether to clean up. If a txn
	// takes a long time to read, increasing its TTL will help to prevent it from been aborted soon after prewrite.
	elapsed := time.Since(startTime) / time.Millisecond
	return lockTTL + uint64(elapsed)
}

var preSplitDetectThreshold uint32 = 100000
var preSplitSizeThreshold uint32 = 32 << 20

// doActionOnMutations groups keys into primary batch and secondary batches, if primary batch exists in the key,
// it does action on primary batch first, then on secondary batches. If action is commit, secondary batches
// is done in background goroutine.
func (c *twoPhaseCommitter) doActionOnMutations(bo *Backoffer, action twoPhaseCommitAction, mutations CommitterMutations) error {
	if mutations.Len() == 0 {
		return nil
	}
	groups, err := c.groupMutations(bo, mutations)
	if err != nil {
		return errors.Trace(err)
	}

	// This is redundant since `doActionOnGroupMutations` will still split groups into batches and
	// check the number of batches. However we don't want the check fail after any code changes.
	c.checkOnePCFallBack(action, len(groups))

	return c.doActionOnGroupMutations(bo, action, groups)
}

// groupMutations groups mutations by region, then checks for any large groups and in that case pre-splits the region.
func (c *twoPhaseCommitter) groupMutations(bo *Backoffer, mutations CommitterMutations) ([]groupedMutations, error) {
	groups, err := c.store.regionCache.groupSortedMutationsByRegion(bo, mutations)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Pre-split regions to avoid too much write workload into a single region.
	// In the large transaction case, this operation is important to avoid TiKV 'server is busy' error.
	var didPreSplit bool
	preSplitDetectThresholdVal := atomic.LoadUint32(&preSplitDetectThreshold)
	for _, group := range groups {
		if uint32(group.mutations.Len()) >= preSplitDetectThresholdVal {
			logutil.BgLogger().Info("2PC detect large amount of mutations on a single region",
				zap.Uint64("region", group.region.GetID()),
				zap.Int("mutations count", group.mutations.Len()))
			if c.store.preSplitRegion(bo.GetCtx(), group) {
				didPreSplit = true
			}
		}
	}
	// Reload region cache again.
	if didPreSplit {
		groups, err = c.store.regionCache.groupSortedMutationsByRegion(bo, mutations)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return groups, nil
}

// doActionOnGroupedMutations splits groups into batches (there is one group per region, and potentially many batches per group, but all mutations
// in a batch will belong to the same region).
func (c *twoPhaseCommitter) doActionOnGroupMutations(bo *Backoffer, action twoPhaseCommitAction, groups []groupedMutations) error {
	action.tiKVTxnRegionsNumHistogram().Observe(float64(len(groups)))

	var sizeFunc = c.keySize

	switch act := action.(type) {
	case actionPrewrite:
		// Do not update regionTxnSize on retries. They are not used when building a PrewriteRequest.
		if !act.retry {
			for _, group := range groups {
				c.regionTxnSize[group.region.id] = group.mutations.Len()
			}
		}
		sizeFunc = c.keyValueSize
		atomic.AddInt32(&c.getDetail().PrewriteRegionNum, int32(len(groups)))
	case actionPessimisticLock:
		if act.LockCtx.Stats != nil {
			act.LockCtx.Stats.RegionNum = int32(len(groups))
		}
	}

	batchBuilder := newBatched(c.primary())
	for _, group := range groups {
		batchBuilder.appendBatchMutationsBySize(group.region, group.mutations, sizeFunc, txnCommitBatchSize)
	}
	firstIsPrimary := batchBuilder.setPrimary()

	actionCommit, actionIsCommit := action.(actionCommit)
	_, actionIsCleanup := action.(actionCleanup)
	_, actionIsPessimiticLock := action.(actionPessimisticLock)

	c.checkOnePCFallBack(action, len(batchBuilder.allBatches()))

	var err error
	failpoint.Inject("skipKeyReturnOK", func(val failpoint.Value) {
		valStr, ok := val.(string)
		if ok && c.sessionID > 0 {
			if firstIsPrimary && actionIsPessimiticLock {
				logutil.Logger(bo.GetCtx()).Warn("pessimisticLock failpoint", zap.String("valStr", valStr))
				switch valStr {
				case "pessimisticLockSkipPrimary":
					err = c.doActionOnBatches(bo, action, batchBuilder.allBatches())
					failpoint.Return(err)
				case "pessimisticLockSkipSecondary":
					err = c.doActionOnBatches(bo, action, batchBuilder.primaryBatch())
					failpoint.Return(err)
				}
			}
		}
	})
	failpoint.Inject("pessimisticRollbackDoNth", func() {
		_, actionIsPessimisticRollback := action.(actionPessimisticRollback)
		if actionIsPessimisticRollback && c.sessionID > 0 {
			logutil.Logger(bo.GetCtx()).Warn("pessimisticRollbackDoNth failpoint")
			failpoint.Return(nil)
		}
	})

	if firstIsPrimary &&
		((actionIsCommit && !c.isAsyncCommit()) || actionIsCleanup || actionIsPessimiticLock) {
		// primary should be committed(not async commit)/cleanup/pessimistically locked first
		err = c.doActionOnBatches(bo, action, batchBuilder.primaryBatch())
		if err != nil {
			return errors.Trace(err)
		}
		if actionIsCommit && c.testingKnobs.bkAfterCommitPrimary != nil && c.testingKnobs.acAfterCommitPrimary != nil {
			c.testingKnobs.acAfterCommitPrimary <- struct{}{}
			<-c.testingKnobs.bkAfterCommitPrimary
		}
		batchBuilder.forgetPrimary()
	}
	// Already spawned a goroutine for async commit transaction.
	if actionIsCommit && !actionCommit.retry && !c.isAsyncCommit() {
		secondaryBo := retry.NewBackofferWithVars(context.Background(), CommitSecondaryMaxBackoff, c.txn.vars)
		go func() {
			if c.sessionID > 0 {
				failpoint.Inject("beforeCommitSecondaries", func(v failpoint.Value) {
					if s, ok := v.(string); !ok {
						logutil.Logger(bo.GetCtx()).Info("[failpoint] sleep 2s before commit secondary keys",
							zap.Uint64("sessionID", c.sessionID), zap.Uint64("txnStartTS", c.startTS), zap.Uint64("txnCommitTS", c.commitTS))
						time.Sleep(2 * time.Second)
					} else if s == "skip" {
						logutil.Logger(bo.GetCtx()).Info("[failpoint] injected skip committing secondaries",
							zap.Uint64("sessionID", c.sessionID), zap.Uint64("txnStartTS", c.startTS), zap.Uint64("txnCommitTS", c.commitTS))
						failpoint.Return()
					}
				})
			}

			e := c.doActionOnBatches(secondaryBo, action, batchBuilder.allBatches())
			if e != nil {
				logutil.BgLogger().Debug("2PC async doActionOnBatches",
					zap.Uint64("session", c.sessionID),
					zap.Stringer("action type", action),
					zap.Error(e))
				metrics.SecondaryLockCleanupFailureCounterCommit.Inc()
			}
		}()
	} else {
		err = c.doActionOnBatches(bo, action, batchBuilder.allBatches())
	}
	return errors.Trace(err)
}

// doActionOnBatches does action to batches in parallel.
func (c *twoPhaseCommitter) doActionOnBatches(bo *Backoffer, action twoPhaseCommitAction, batches []batchMutations) error {
	if len(batches) == 0 {
		return nil
	}

	noNeedFork := len(batches) == 1
	if !noNeedFork {
		if ac, ok := action.(actionCommit); ok && ac.retry {
			noNeedFork = true
		}
	}
	if noNeedFork {
		for _, b := range batches {
			e := action.handleSingleBatch(c, bo, b)
			if e != nil {
				logutil.BgLogger().Debug("2PC doActionOnBatches failed",
					zap.Uint64("session", c.sessionID),
					zap.Stringer("action type", action),
					zap.Error(e),
					zap.Uint64("txnStartTS", c.startTS))
				return errors.Trace(e)
			}
		}
		return nil
	}
	rateLim := len(batches)
	// Set rateLim here for the large transaction.
	// If the rate limit is too high, tikv will report service is busy.
	// If the rate limit is too low, we can't full utilize the tikv's throughput.
	// TODO: Find a self-adaptive way to control the rate limit here.
	if rateLim > config.GetGlobalConfig().CommitterConcurrency {
		rateLim = config.GetGlobalConfig().CommitterConcurrency
	}
	batchExecutor := newBatchExecutor(rateLim, c, action, bo)
	err := batchExecutor.process(batches)
	return errors.Trace(err)
}

func (c *twoPhaseCommitter) keyValueSize(key, value []byte) int {
	return len(key) + len(value)
}

func (c *twoPhaseCommitter) keySize(key, value []byte) int {
	return len(key)
}

type ttlManagerState uint32

const (
	stateUninitialized ttlManagerState = iota
	stateRunning
	stateClosed
)

type ttlManager struct {
	state   ttlManagerState
	ch      chan struct{}
	lockCtx *kv.LockCtx
}

func (tm *ttlManager) run(c *twoPhaseCommitter, lockCtx *kv.LockCtx) {
	// Run only once.
	if !atomic.CompareAndSwapUint32((*uint32)(&tm.state), uint32(stateUninitialized), uint32(stateRunning)) {
		return
	}
	tm.lockCtx = lockCtx
	noKeepAlive := false
	failpoint.Inject("doNotKeepAlive", func() {
		noKeepAlive = true
	})

	if !noKeepAlive {
		go tm.keepAlive(c)
	}
}

func (tm *ttlManager) close() {
	if !atomic.CompareAndSwapUint32((*uint32)(&tm.state), uint32(stateRunning), uint32(stateClosed)) {
		return
	}
	close(tm.ch)
}

const keepAliveMaxBackoff = 20000        // 20 seconds
const pessimisticLockMaxBackoff = 600000 // 10 minutes
const maxConsecutiveFailure = 10

func (tm *ttlManager) keepAlive(c *twoPhaseCommitter) {
	// Ticker is set to 1/2 of the ManagedLockTTL.
	ticker := time.NewTicker(time.Duration(atomic.LoadUint64(&ManagedLockTTL)) * time.Millisecond / 2)
	defer ticker.Stop()
	keepFail := 0
	for {
		select {
		case <-tm.ch:
			return
		case <-ticker.C:
			// If kill signal is received, the ttlManager should exit.
			if tm.lockCtx != nil && tm.lockCtx.Killed != nil && atomic.LoadUint32(tm.lockCtx.Killed) != 0 {
				return
			}
			bo := retry.NewBackofferWithVars(context.Background(), keepAliveMaxBackoff, c.txn.vars)
			now, err := c.store.getTimestampWithRetry(bo, c.txn.GetScope())
			if err != nil {
				logutil.Logger(bo.GetCtx()).Warn("keepAlive get tso fail",
					zap.Error(err))
				return
			}

			uptime := uint64(oracle.ExtractPhysical(now) - oracle.ExtractPhysical(c.startTS))
			if uptime > config.GetGlobalConfig().MaxTxnTTL {
				// Checks maximum lifetime for the ttlManager, so when something goes wrong
				// the key will not be locked forever.
				logutil.Logger(bo.GetCtx()).Info("ttlManager live up to its lifetime",
					zap.Uint64("txnStartTS", c.startTS),
					zap.Uint64("uptime", uptime),
					zap.Uint64("maxTxnTTL", config.GetGlobalConfig().MaxTxnTTL))
				metrics.TiKVTTLLifeTimeReachCounter.Inc()
				// the pessimistic locks may expire if the ttl manager has timed out, set `LockExpired` flag
				// so that this transaction could only commit or rollback with no more statement executions
				if c.isPessimistic && tm.lockCtx != nil && tm.lockCtx.LockExpired != nil {
					atomic.StoreUint32(tm.lockCtx.LockExpired, 1)
				}
				return
			}

			newTTL := uptime + atomic.LoadUint64(&ManagedLockTTL)
			logutil.Logger(bo.GetCtx()).Info("send TxnHeartBeat",
				zap.Uint64("startTS", c.startTS), zap.Uint64("newTTL", newTTL))
			startTime := time.Now()
			_, stopHeartBeat, err := sendTxnHeartBeat(bo, c.store, c.primary(), c.startTS, newTTL)
			if err != nil {
				keepFail++
				metrics.TxnHeartBeatHistogramError.Observe(time.Since(startTime).Seconds())
				logutil.Logger(bo.GetCtx()).Debug("send TxnHeartBeat failed",
					zap.Error(err),
					zap.Uint64("txnStartTS", c.startTS))
				if stopHeartBeat || keepFail > maxConsecutiveFailure {
					logutil.Logger(bo.GetCtx()).Warn("stop TxnHeartBeat",
						zap.Error(err),
						zap.Int("consecutiveFailure", keepFail),
						zap.Uint64("txnStartTS", c.startTS))
					return
				}
				continue
			}
			keepFail = 0
			metrics.TxnHeartBeatHistogramOK.Observe(time.Since(startTime).Seconds())
		}
	}
}

func sendTxnHeartBeat(bo *Backoffer, store *KVStore, primary []byte, startTS, ttl uint64) (newTTL uint64, stopHeartBeat bool, err error) {
	req := tikvrpc.NewRequest(tikvrpc.CmdTxnHeartBeat, &pb.TxnHeartBeatRequest{
		PrimaryLock:   primary,
		StartVersion:  startTS,
		AdviseLockTtl: ttl,
	})
	for {
		loc, err := store.GetRegionCache().LocateKey(bo, primary)
		if err != nil {
			return 0, false, errors.Trace(err)
		}
		resp, err := store.SendReq(bo, req, loc.Region, client.ReadTimeoutShort)
		if err != nil {
			return 0, false, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return 0, false, errors.Trace(err)
		}
		if regionErr != nil {
			// For other region error and the fake region error, backoff because
			// there's something wrong.
			// For the real EpochNotMatch error, don't backoff.
			if regionErr.GetEpochNotMatch() == nil || isFakeRegionError(regionErr) {
				err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
				if err != nil {
					return 0, false, errors.Trace(err)
				}
			}
			continue
		}
		if resp.Resp == nil {
			return 0, false, errors.Trace(tikverr.ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*pb.TxnHeartBeatResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			return 0, true, errors.Errorf("txn %d heartbeat fail, primary key = %v, err = %s", startTS, hex.EncodeToString(primary), extractKeyErr(keyErr))
		}
		return cmdResp.GetLockTtl(), false, nil
	}
}

// checkAsyncCommit checks if async commit protocol is available for current transaction commit, true is returned if possible.
func (c *twoPhaseCommitter) checkAsyncCommit() bool {
	// Disable async commit in local transactions
	if c.txn.GetScope() != oracle.GlobalTxnScope {
		return false
	}

	asyncCommitCfg := config.GetGlobalConfig().TiKVClient.AsyncCommit
	// TODO the keys limit need more tests, this value makes the unit test pass by now.
	// Async commit is not compatible with Binlog because of the non unique timestamp issue.
	if c.sessionID > 0 && c.txn.enableAsyncCommit &&
		uint(c.mutations.Len()) <= asyncCommitCfg.KeysLimit &&
		!c.shouldWriteBinlog() {
		totalKeySize := uint64(0)
		for i := 0; i < c.mutations.Len(); i++ {
			totalKeySize += uint64(len(c.mutations.GetKey(i)))
			if totalKeySize > asyncCommitCfg.TotalKeySizeLimit {
				return false
			}
		}
		return true
	}
	return false
}

// checkOnePC checks if 1PC protocol is available for current transaction.
func (c *twoPhaseCommitter) checkOnePC() bool {
	// Disable 1PC in local transactions
	if c.txn.GetScope() != oracle.GlobalTxnScope {
		return false
	}

	return c.sessionID > 0 && !c.shouldWriteBinlog() && c.txn.enable1PC
}

func (c *twoPhaseCommitter) needLinearizability() bool {
	return !c.txn.causalConsistency
}

func (c *twoPhaseCommitter) isAsyncCommit() bool {
	return atomic.LoadUint32(&c.useAsyncCommit) > 0
}

func (c *twoPhaseCommitter) setAsyncCommit(val bool) {
	if val {
		atomic.StoreUint32(&c.useAsyncCommit, 1)
	} else {
		atomic.StoreUint32(&c.useAsyncCommit, 0)
	}
}

func (c *twoPhaseCommitter) isOnePC() bool {
	return atomic.LoadUint32(&c.useOnePC) > 0
}

func (c *twoPhaseCommitter) setOnePC(val bool) {
	if val {
		atomic.StoreUint32(&c.useOnePC, 1)
	} else {
		atomic.StoreUint32(&c.useOnePC, 0)
	}
}

func (c *twoPhaseCommitter) checkOnePCFallBack(action twoPhaseCommitAction, batchCount int) {
	if _, ok := action.(actionPrewrite); ok {
		if batchCount > 1 {
			c.setOnePC(false)
		}
	}
}

const (
	cleanupMaxBackoff = 20000
	tsoMaxBackoff     = 15000
)

// VeryLongMaxBackoff is the max sleep time of transaction commit.
var VeryLongMaxBackoff = uint64(600000) // 10mins

func (c *twoPhaseCommitter) cleanup(ctx context.Context) {
	c.cleanWg.Add(1)
	go func() {
		failpoint.Inject("commitFailedSkipCleanup", func() {
			logutil.Logger(ctx).Info("[failpoint] injected skip cleanup secondaries on failure",
				zap.Uint64("txnStartTS", c.startTS))
			c.cleanWg.Done()
			failpoint.Return()
		})

		cleanupKeysCtx := context.WithValue(context.Background(), retry.TxnStartKey, ctx.Value(retry.TxnStartKey))
		var err error
		if !c.isOnePC() {
			err = c.cleanupMutations(retry.NewBackofferWithVars(cleanupKeysCtx, cleanupMaxBackoff, c.txn.vars), c.mutations)
		} else if c.isPessimistic {
			err = c.pessimisticRollbackMutations(retry.NewBackofferWithVars(cleanupKeysCtx, cleanupMaxBackoff, c.txn.vars), c.mutations)
		}

		if err != nil {
			metrics.SecondaryLockCleanupFailureCounterRollback.Inc()
			logutil.Logger(ctx).Info("2PC cleanup failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS),
				zap.Bool("isPessimistic", c.isPessimistic), zap.Bool("isOnePC", c.isOnePC()))
		} else {
			logutil.Logger(ctx).Debug("2PC clean up done",
				zap.Uint64("txnStartTS", c.startTS), zap.Bool("isPessimistic", c.isPessimistic),
				zap.Bool("isOnePC", c.isOnePC()))
		}
		c.cleanWg.Done()
	}()
}

// execute executes the two-phase commit protocol.
func (c *twoPhaseCommitter) execute(ctx context.Context) (err error) {
	var binlogSkipped bool
	defer func() {
		if c.isOnePC() {
			// The error means the 1PC transaction failed.
			if err != nil {
				if c.getUndeterminedErr() == nil {
					c.cleanup(ctx)
				}
				metrics.OnePCTxnCounterError.Inc()
			} else {
				metrics.OnePCTxnCounterOk.Inc()
			}
		} else if c.isAsyncCommit() {
			// The error means the async commit should not succeed.
			if err != nil {
				if c.getUndeterminedErr() == nil {
					c.cleanup(ctx)
				}
				metrics.AsyncCommitTxnCounterError.Inc()
			} else {
				metrics.AsyncCommitTxnCounterOk.Inc()
			}
		} else {
			// Always clean up all written keys if the txn does not commit.
			c.mu.RLock()
			committed := c.mu.committed
			undetermined := c.mu.undeterminedErr != nil
			c.mu.RUnlock()
			if !committed && !undetermined {
				c.cleanup(ctx)
				metrics.TwoPCTxnCounterError.Inc()
			} else {
				metrics.TwoPCTxnCounterOk.Inc()
			}
			c.txn.commitTS = c.commitTS
			if binlogSkipped {
				c.binlog.Skip()
				return
			}
			if !c.shouldWriteBinlog() {
				return
			}
			if err != nil {
				c.binlog.Commit(ctx, 0)
			} else {
				c.binlog.Commit(ctx, int64(c.commitTS))
			}
		}
	}()

	commitTSMayBeCalculated := false
	// Check async commit is available or not.
	if c.checkAsyncCommit() {
		commitTSMayBeCalculated = true
		c.setAsyncCommit(true)
		c.hasTriedAsyncCommit = true
	}
	// Check if 1PC is enabled.
	if c.checkOnePC() {
		commitTSMayBeCalculated = true
		c.setOnePC(true)
		c.hasTriedOnePC = true
	}

	// TODO(youjiali1995): It's better to use different maxSleep for different operations
	// and distinguish permanent errors from temporary errors, for example:
	//   - If all PDs are down, all requests to PD will fail due to network error.
	//     The maxSleep should't be very long in this case.
	//   - If the region isn't found in PD, it's possible the reason is write-stall.
	//     The maxSleep can be long in this case.
	bo := retry.NewBackofferWithVars(ctx, int(atomic.LoadUint64(&VeryLongMaxBackoff)), c.txn.vars)

	// If we want to use async commit or 1PC and also want linearizability across
	// all nodes, we have to make sure the commit TS of this transaction is greater
	// than the snapshot TS of all existent readers. So we get a new timestamp
	// from PD and plus one as our MinCommitTS.
	if commitTSMayBeCalculated && c.needLinearizability() {
		failpoint.Inject("getMinCommitTSFromTSO", nil)
		latestTS, err := c.store.getTimestampWithRetry(bo, c.txn.GetScope())
		// If we fail to get a timestamp from PD, we just propagate the failure
		// instead of falling back to the normal 2PC because a normal 2PC will
		// also be likely to fail due to the same timestamp issue.
		if err != nil {
			return errors.Trace(err)
		}
		// Plus 1 to avoid producing the same commit TS with previously committed transactions
		c.minCommitTS = latestTS + 1
	}
	// Calculate maxCommitTS if necessary
	if commitTSMayBeCalculated {
		if err = c.calculateMaxCommitTS(ctx); err != nil {
			return errors.Trace(err)
		}
	}

	if c.sessionID > 0 {
		failpoint.Inject("beforePrewrite", nil)
	}

	c.prewriteStarted = true
	var binlogChan <-chan BinlogWriteResult
	if c.shouldWriteBinlog() {
		binlogChan = c.binlog.Prewrite(ctx, c.primary())
	}

	start := time.Now()
	err = c.prewriteMutations(bo, c.mutations)

	if err != nil {
		// TODO: Now we return an undetermined error as long as one of the prewrite
		// RPCs fails. However, if there are multiple errors and some of the errors
		// are not RPC failures, we can return the actual error instead of undetermined.
		if undeterminedErr := c.getUndeterminedErr(); undeterminedErr != nil {
			logutil.Logger(ctx).Error("2PC commit result undetermined",
				zap.Error(err),
				zap.NamedError("rpcErr", undeterminedErr),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(terror.ErrResultUndetermined)
		}
	}

	commitDetail := c.getDetail()
	commitDetail.PrewriteTime = time.Since(start)
	if bo.GetTotalSleep() > 0 {
		boSleep := int64(bo.GetTotalSleep()) * int64(time.Millisecond)
		commitDetail.Mu.Lock()
		if boSleep > commitDetail.Mu.CommitBackoffTime {
			commitDetail.Mu.CommitBackoffTime = boSleep
			commitDetail.Mu.BackoffTypes = bo.GetTypes()
		}
		commitDetail.Mu.Unlock()
	}

	if binlogChan != nil {
		startWaitBinlog := time.Now()
		binlogWriteResult := <-binlogChan
		commitDetail.WaitPrewriteBinlogTime = time.Since(startWaitBinlog)
		if binlogWriteResult != nil {
			binlogSkipped = binlogWriteResult.Skipped()
			binlogErr := binlogWriteResult.GetError()
			if binlogErr != nil {
				return binlogErr
			}
		}
	}
	if err != nil {
		logutil.Logger(ctx).Debug("2PC failed on prewrite",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}

	// strip check_not_exists keys that no need to commit.
	c.stripNoNeedCommitKeys()

	var commitTS uint64

	if c.isOnePC() {
		if c.onePCCommitTS == 0 {
			err = errors.Errorf("session %d invalid onePCCommitTS for 1PC protocol after prewrite, startTS=%v", c.sessionID, c.startTS)
			return errors.Trace(err)
		}
		c.commitTS = c.onePCCommitTS
		c.txn.commitTS = c.commitTS
		logutil.Logger(ctx).Debug("1PC protocol is used to commit this txn",
			zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", c.commitTS),
			zap.Uint64("session", c.sessionID))
		return nil
	}

	if c.onePCCommitTS != 0 {
		logutil.Logger(ctx).Fatal("non 1PC transaction committed in 1PC",
			zap.Uint64("session", c.sessionID), zap.Uint64("startTS", c.startTS))
	}

	if c.isAsyncCommit() {
		if c.minCommitTS == 0 {
			err = errors.Errorf("session %d invalid minCommitTS for async commit protocol after prewrite, startTS=%v", c.sessionID, c.startTS)
			return errors.Trace(err)
		}
		commitTS = c.minCommitTS
	} else {
		start = time.Now()
		logutil.Event(ctx, "start get commit ts")
		commitTS, err = c.store.getTimestampWithRetry(retry.NewBackofferWithVars(ctx, tsoMaxBackoff, c.txn.vars), c.txn.GetScope())
		if err != nil {
			logutil.Logger(ctx).Warn("2PC get commitTS failed",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(err)
		}
		commitDetail.GetCommitTsTime = time.Since(start)
		logutil.Event(ctx, "finish get commit ts")
		logutil.SetTag(ctx, "commitTs", commitTS)
	}

	if !c.isAsyncCommit() {
		tryAmend := c.isPessimistic && c.sessionID > 0 && c.txn.schemaAmender != nil
		if !tryAmend {
			_, _, err = c.checkSchemaValid(ctx, commitTS, c.txn.schemaVer, false)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			relatedSchemaChange, memAmended, err := c.checkSchemaValid(ctx, commitTS, c.txn.schemaVer, true)
			if err != nil {
				return errors.Trace(err)
			}
			if memAmended {
				// Get new commitTS and check schema valid again.
				newCommitTS, err := c.getCommitTS(ctx, commitDetail)
				if err != nil {
					return errors.Trace(err)
				}
				// If schema check failed between commitTS and newCommitTs, report schema change error.
				_, _, err = c.checkSchemaValid(ctx, newCommitTS, relatedSchemaChange.LatestInfoSchema, false)
				if err != nil {
					logutil.Logger(ctx).Info("schema check after amend failed, it means the schema version changed again",
						zap.Uint64("startTS", c.startTS),
						zap.Uint64("amendTS", commitTS),
						zap.Int64("amendedSchemaVersion", relatedSchemaChange.LatestInfoSchema.SchemaMetaVersion()),
						zap.Uint64("newCommitTS", newCommitTS))
					return errors.Trace(err)
				}
				commitTS = newCommitTS
			}
		}
	}
	atomic.StoreUint64(&c.commitTS, commitTS)

	if c.store.oracle.IsExpired(c.startTS, MaxTxnTimeUse, &oracle.Option{TxnScope: oracle.GlobalTxnScope}) {
		err = errors.Errorf("session %d txn takes too much time, txnStartTS: %d, comm: %d",
			c.sessionID, c.startTS, c.commitTS)
		return err
	}

	if c.sessionID > 0 {
		failpoint.Inject("beforeCommit", func(val failpoint.Value) {
			// Pass multiple instructions in one string, delimited by commas, to trigger multiple behaviors, like
			// `return("delay,fail")`. Then they will be executed sequentially at once.
			if v, ok := val.(string); ok {
				for _, action := range strings.Split(v, ",") {
					// Async commit transactions cannot return error here, since it's already successful.
					if action == "fail" && !c.isAsyncCommit() {
						logutil.Logger(ctx).Info("[failpoint] injected failure before commit", zap.Uint64("txnStartTS", c.startTS))
						failpoint.Return(errors.New("injected failure before commit"))
					} else if action == "delay" {
						duration := time.Duration(rand.Int63n(int64(time.Second) * 5))
						logutil.Logger(ctx).Info("[failpoint] injected delay before commit",
							zap.Uint64("txnStartTS", c.startTS), zap.Duration("duration", duration))
						time.Sleep(duration)
					}
				}
			}
		})
	}

	if c.isAsyncCommit() {
		// For async commit protocol, the commit is considered success here.
		c.txn.commitTS = c.commitTS
		logutil.Logger(ctx).Debug("2PC will use async commit protocol to commit this txn",
			zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", c.commitTS),
			zap.Uint64("sessionID", c.sessionID))
		go func() {
			failpoint.Inject("asyncCommitDoNothing", func() {
				failpoint.Return()
			})
			commitBo := retry.NewBackofferWithVars(ctx, CommitSecondaryMaxBackoff, c.txn.vars)
			err := c.commitMutations(commitBo, c.mutations)
			if err != nil {
				logutil.Logger(ctx).Warn("2PC async commit failed", zap.Uint64("sessionID", c.sessionID),
					zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", c.commitTS), zap.Error(err))
			}
		}()
		return nil
	}
	return c.commitTxn(ctx, commitDetail)
}

func (c *twoPhaseCommitter) commitTxn(ctx context.Context, commitDetail *util.CommitDetails) error {
	c.txn.GetMemBuffer().DiscardValues()
	start := time.Now()

	// Use the VeryLongMaxBackoff to commit the primary key.
	commitBo := retry.NewBackofferWithVars(ctx, int(atomic.LoadUint64(&VeryLongMaxBackoff)), c.txn.vars)
	err := c.commitMutations(commitBo, c.mutations)
	commitDetail.CommitTime = time.Since(start)
	if commitBo.GetTotalSleep() > 0 {
		commitDetail.Mu.Lock()
		commitDetail.Mu.CommitBackoffTime += int64(commitBo.GetTotalSleep()) * int64(time.Millisecond)
		commitDetail.Mu.BackoffTypes = append(commitDetail.Mu.BackoffTypes, commitBo.GetTypes()...)
		commitDetail.Mu.Unlock()
	}
	if err != nil {
		if undeterminedErr := c.getUndeterminedErr(); undeterminedErr != nil {
			logutil.Logger(ctx).Error("2PC commit result undetermined",
				zap.Error(err),
				zap.NamedError("rpcErr", undeterminedErr),
				zap.Uint64("txnStartTS", c.startTS))
			err = errors.Trace(terror.ErrResultUndetermined)
		}
		if !c.mu.committed {
			logutil.Logger(ctx).Debug("2PC failed on commit",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(err)
		}
		logutil.Logger(ctx).Debug("got some exceptions, but 2PC was still successful",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
	}
	return nil
}

func (c *twoPhaseCommitter) stripNoNeedCommitKeys() {
	if !c.hasNoNeedCommitKeys {
		return
	}
	m := c.mutations
	var newIdx int
	for oldIdx := range m.handles {
		key := m.GetKey(oldIdx)
		flags, err := c.txn.GetMemBuffer().GetFlags(key)
		if err == nil && flags.HasPrewriteOnly() {
			continue
		}
		m.handles[newIdx] = m.handles[oldIdx]
		newIdx++
	}
	c.mutations.handles = c.mutations.handles[:newIdx]
}

// SchemaVer is the infoSchema which will return the schema version.
type SchemaVer interface {
	// SchemaMetaVersion returns the meta schema version.
	SchemaMetaVersion() int64
}

// SchemaLeaseChecker is used to validate schema version is not changed during transaction execution.
type SchemaLeaseChecker interface {
	// CheckBySchemaVer checks if the schema has changed for the transaction related tables between the startSchemaVer
	// and the schema version at txnTS, all the related schema changes will be returned.
	CheckBySchemaVer(txnTS uint64, startSchemaVer SchemaVer) (*RelatedSchemaChange, error)
}

// RelatedSchemaChange contains information about schema diff between two schema versions.
type RelatedSchemaChange struct {
	PhyTblIDS        []int64
	ActionTypes      []uint64
	LatestInfoSchema SchemaVer
	Amendable        bool
}

func (c *twoPhaseCommitter) amendPessimisticLock(ctx context.Context, addMutations CommitterMutations) error {
	keysNeedToLock := NewPlainMutations(addMutations.Len())
	for i := 0; i < addMutations.Len(); i++ {
		if addMutations.IsPessimisticLock(i) {
			keysNeedToLock.Push(addMutations.GetOp(i), addMutations.GetKey(i), addMutations.GetValue(i), addMutations.IsPessimisticLock(i))
		}
	}
	// For unique index amend, we need to pessimistic lock the generated new index keys first.
	// Set doingAmend to true to force the pessimistic lock do the exist check for these keys.
	c.doingAmend = true
	defer func() { c.doingAmend = false }()
	if keysNeedToLock.Len() > 0 {
		lCtx := &kv.LockCtx{
			Killed:        c.lockCtx.Killed,
			ForUpdateTS:   c.forUpdateTS,
			LockWaitTime:  c.lockCtx.LockWaitTime,
			WaitStartTime: time.Now(),
		}
		tryTimes := uint(0)
		retryLimit := config.GetGlobalConfig().PessimisticTxn.MaxRetryCount
		var err error
		for tryTimes < retryLimit {
			pessimisticLockBo := retry.NewBackofferWithVars(ctx, pessimisticLockMaxBackoff, c.txn.vars)
			err = c.pessimisticLockMutations(pessimisticLockBo, lCtx, &keysNeedToLock)
			if err != nil {
				// KeysNeedToLock won't change, so don't async rollback pessimistic locks here for write conflict.
				if _, ok := errors.Cause(err).(*tikverr.ErrWriteConflict); ok {
					newForUpdateTSVer, err := c.store.CurrentTimestamp(oracle.GlobalTxnScope)
					if err != nil {
						return errors.Trace(err)
					}
					lCtx.ForUpdateTS = newForUpdateTSVer
					c.forUpdateTS = newForUpdateTSVer
					logutil.Logger(ctx).Info("amend pessimistic lock pessimistic retry lock",
						zap.Uint("tryTimes", tryTimes), zap.Uint64("startTS", c.startTS),
						zap.Uint64("newForUpdateTS", c.forUpdateTS))
					tryTimes++
					continue
				}
				logutil.Logger(ctx).Warn("amend pessimistic lock has failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS))
				return err
			}
			logutil.Logger(ctx).Info("amend pessimistic lock finished", zap.Uint64("startTS", c.startTS),
				zap.Uint64("forUpdateTS", c.forUpdateTS), zap.Int("keys", keysNeedToLock.Len()))
			break
		}
		if err != nil {
			logutil.Logger(ctx).Warn("amend pessimistic lock failed after retry",
				zap.Uint("tryTimes", tryTimes), zap.Uint64("startTS", c.startTS))
			return err
		}
	}
	return nil
}

func (c *twoPhaseCommitter) tryAmendTxn(ctx context.Context, startInfoSchema SchemaVer, change *RelatedSchemaChange) (bool, error) {
	addMutations, err := c.txn.schemaAmender.AmendTxn(ctx, startInfoSchema, change, c.mutations)
	if err != nil {
		return false, err
	}
	// Add new mutations to the mutation list or prewrite them if prewrite already starts.
	if addMutations != nil && addMutations.Len() > 0 {
		err = c.amendPessimisticLock(ctx, addMutations)
		if err != nil {
			logutil.Logger(ctx).Info("amendPessimisticLock has failed", zap.Error(err))
			return false, err
		}
		if c.prewriteStarted {
			prewriteBo := retry.NewBackofferWithVars(ctx, PrewriteMaxBackoff, c.txn.vars)
			err = c.prewriteMutations(prewriteBo, addMutations)
			if err != nil {
				logutil.Logger(ctx).Warn("amend prewrite has failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS))
				return false, err
			}
			logutil.Logger(ctx).Info("amend prewrite finished", zap.Uint64("txnStartTS", c.startTS))
			return true, nil
		}
		memBuf := c.txn.GetMemBuffer()
		for i := 0; i < addMutations.Len(); i++ {
			key := addMutations.GetKey(i)
			op := addMutations.GetOp(i)
			var err error
			if op == pb.Op_Del {
				err = memBuf.Delete(key)
			} else {
				err = memBuf.Set(key, addMutations.GetValue(i))
			}
			if err != nil {
				logutil.Logger(ctx).Warn("amend mutations has failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS))
				return false, err
			}
			handle := c.txn.GetMemBuffer().IterWithFlags(key, nil).Handle()
			c.mutations.Push(op, addMutations.IsPessimisticLock(i), handle)
		}
	}
	return false, nil
}

func (c *twoPhaseCommitter) getCommitTS(ctx context.Context, commitDetail *util.CommitDetails) (uint64, error) {
	start := time.Now()
	logutil.Event(ctx, "start get commit ts")
	commitTS, err := c.store.getTimestampWithRetry(retry.NewBackofferWithVars(ctx, tsoMaxBackoff, c.txn.vars), c.txn.GetScope())
	if err != nil {
		logutil.Logger(ctx).Warn("2PC get commitTS failed",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return 0, errors.Trace(err)
	}
	commitDetail.GetCommitTsTime = time.Since(start)
	logutil.Event(ctx, "finish get commit ts")
	logutil.SetTag(ctx, "commitTS", commitTS)

	// Check commitTS.
	if commitTS <= c.startTS {
		err = errors.Errorf("session %d invalid transaction tso with txnStartTS=%v while txnCommitTS=%v",
			c.sessionID, c.startTS, commitTS)
		logutil.BgLogger().Error("invalid transaction", zap.Error(err))
		return 0, errors.Trace(err)
	}
	return commitTS, nil
}

// checkSchemaValid checks if the schema has changed, if tryAmend is set to true, committer will try to amend
// this transaction using the related schema changes.
func (c *twoPhaseCommitter) checkSchemaValid(ctx context.Context, checkTS uint64, startInfoSchema SchemaVer,
	tryAmend bool) (*RelatedSchemaChange, bool, error) {
	failpoint.Inject("failCheckSchemaValid", func() {
		logutil.Logger(ctx).Info("[failpoint] injected fail schema check",
			zap.Uint64("txnStartTS", c.startTS))
		err := errors.Errorf("mock check schema valid failure")
		failpoint.Return(nil, false, err)
	})
	if c.txn.schemaLeaseChecker == nil {
		if c.sessionID > 0 {
			logutil.Logger(ctx).Warn("schemaLeaseChecker is not set for this transaction",
				zap.Uint64("sessionID", c.sessionID),
				zap.Uint64("startTS", c.startTS),
				zap.Uint64("commitTS", checkTS))
		}
		return nil, false, nil
	}
	relatedChanges, err := c.txn.schemaLeaseChecker.CheckBySchemaVer(checkTS, startInfoSchema)
	if err != nil {
		if tryAmend && relatedChanges != nil && relatedChanges.Amendable && c.txn.schemaAmender != nil {
			memAmended, amendErr := c.tryAmendTxn(ctx, startInfoSchema, relatedChanges)
			if amendErr != nil {
				logutil.BgLogger().Info("txn amend has failed", zap.Uint64("sessionID", c.sessionID),
					zap.Uint64("startTS", c.startTS), zap.Error(amendErr))
				return nil, false, err
			}
			logutil.Logger(ctx).Info("amend txn successfully",
				zap.Uint64("sessionID", c.sessionID), zap.Uint64("txn startTS", c.startTS), zap.Bool("memAmended", memAmended),
				zap.Uint64("checkTS", checkTS), zap.Int64("startInfoSchemaVer", startInfoSchema.SchemaMetaVersion()),
				zap.Int64s("table ids", relatedChanges.PhyTblIDS), zap.Uint64s("action types", relatedChanges.ActionTypes))
			return relatedChanges, memAmended, nil
		}
		return nil, false, errors.Trace(err)
	}
	return nil, false, nil
}

func (c *twoPhaseCommitter) calculateMaxCommitTS(ctx context.Context) error {
	// Amend txn with current time first, then we can make sure we have another SafeWindow time to commit
	currentTS := oracle.ComposeTS(int64(time.Since(c.txn.startTime)/time.Millisecond), 0) + c.startTS
	_, _, err := c.checkSchemaValid(ctx, currentTS, c.txn.schemaVer, true)
	if err != nil {
		logutil.Logger(ctx).Info("Schema changed for async commit txn",
			zap.Error(err),
			zap.Uint64("startTS", c.startTS))
		return errors.Trace(err)
	}

	safeWindow := config.GetGlobalConfig().TiKVClient.AsyncCommit.SafeWindow
	maxCommitTS := oracle.ComposeTS(int64(safeWindow/time.Millisecond), 0) + currentTS
	logutil.BgLogger().Debug("calculate MaxCommitTS",
		zap.Time("startTime", c.txn.startTime),
		zap.Duration("safeWindow", safeWindow),
		zap.Uint64("startTS", c.startTS),
		zap.Uint64("maxCommitTS", maxCommitTS))

	c.maxCommitTS = maxCommitTS
	return nil
}

func (c *twoPhaseCommitter) shouldWriteBinlog() bool {
	return c.binlog != nil
}

// TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
// Key+Value size below 16KB.
const txnCommitBatchSize = 16 * 1024

type batchMutations struct {
	region    RegionVerID
	mutations CommitterMutations
	isPrimary bool
}

func (b *batchMutations) relocate(bo *Backoffer, c *RegionCache) (bool, error) {
	begin, end := b.mutations.GetKey(0), b.mutations.GetKey(b.mutations.Len()-1)
	loc, err := c.LocateKey(bo, begin)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !loc.Contains(end) {
		return false, nil
	}
	b.region = loc.Region
	return true, nil
}

type batched struct {
	batches    []batchMutations
	primaryIdx int
	primaryKey []byte
}

func newBatched(primaryKey []byte) *batched {
	return &batched{
		primaryIdx: -1,
		primaryKey: primaryKey,
	}
}

// appendBatchMutationsBySize appends mutations to b. It may split the keys to make
// sure each batch's size does not exceed the limit.
func (b *batched) appendBatchMutationsBySize(region RegionVerID, mutations CommitterMutations, sizeFn func(k, v []byte) int, limit int) {
	failpoint.Inject("twoPCRequestBatchSizeLimit", func() {
		limit = 1
	})

	var start, end int
	for start = 0; start < mutations.Len(); start = end {
		var size int
		for end = start; end < mutations.Len() && size < limit; end++ {
			var k, v []byte
			k = mutations.GetKey(end)
			v = mutations.GetValue(end)
			size += sizeFn(k, v)
			if b.primaryIdx < 0 && bytes.Equal(k, b.primaryKey) {
				b.primaryIdx = len(b.batches)
			}
		}
		b.batches = append(b.batches, batchMutations{
			region:    region,
			mutations: mutations.Slice(start, end),
		})
	}
}

func (b *batched) setPrimary() bool {
	// If the batches include the primary key, put it to the first
	if b.primaryIdx >= 0 {
		if len(b.batches) > 0 {
			b.batches[b.primaryIdx].isPrimary = true
			b.batches[0], b.batches[b.primaryIdx] = b.batches[b.primaryIdx], b.batches[0]
			b.primaryIdx = 0
		}
		return true
	}

	return false
}

func (b *batched) allBatches() []batchMutations {
	return b.batches
}

// primaryBatch returns the batch containing the primary key.
// Precondition: `b.setPrimary() == true`
func (b *batched) primaryBatch() []batchMutations {
	return b.batches[:1]
}

func (b *batched) forgetPrimary() {
	if len(b.batches) == 0 {
		return
	}
	b.batches = b.batches[1:]
}

// batchExecutor is txn controller providing rate control like utils
type batchExecutor struct {
	rateLim           int                  // concurrent worker numbers
	rateLimiter       *util.RateLimit      // rate limiter for concurrency control, maybe more strategies
	committer         *twoPhaseCommitter   // here maybe more different type committer in the future
	action            twoPhaseCommitAction // the work action type
	backoffer         *Backoffer           // Backoffer
	tokenWaitDuration time.Duration        // get token wait time
}

// newBatchExecutor create processor to handle concurrent batch works(prewrite/commit etc)
func newBatchExecutor(rateLimit int, committer *twoPhaseCommitter,
	action twoPhaseCommitAction, backoffer *Backoffer) *batchExecutor {
	return &batchExecutor{rateLimit, nil, committer,
		action, backoffer, 0}
}

// initUtils do initialize batchExecutor related policies like rateLimit util
func (batchExe *batchExecutor) initUtils() error {
	// init rateLimiter by injected rate limit number
	batchExe.rateLimiter = util.NewRateLimit(batchExe.rateLim)
	return nil
}

// startWork concurrently do the work for each batch considering rate limit
func (batchExe *batchExecutor) startWorker(exitCh chan struct{}, ch chan error, batches []batchMutations) {
	for idx, batch1 := range batches {
		waitStart := time.Now()
		if exit := batchExe.rateLimiter.GetToken(exitCh); !exit {
			batchExe.tokenWaitDuration += time.Since(waitStart)
			batch := batch1
			go func() {
				defer batchExe.rateLimiter.PutToken()
				var singleBatchBackoffer *Backoffer
				if _, ok := batchExe.action.(actionCommit); ok {
					// Because the secondary batches of the commit actions are implemented to be
					// committed asynchronously in background goroutines, we should not
					// fork a child context and call cancel() while the foreground goroutine exits.
					// Otherwise the background goroutines will be canceled execeptionally.
					// Here we makes a new clone of the original backoffer for this goroutine
					// exclusively to avoid the data race when using the same backoffer
					// in concurrent goroutines.
					singleBatchBackoffer = batchExe.backoffer.Clone()
				} else {
					var singleBatchCancel context.CancelFunc
					singleBatchBackoffer, singleBatchCancel = batchExe.backoffer.Fork()
					defer singleBatchCancel()
				}
				ch <- batchExe.action.handleSingleBatch(batchExe.committer, singleBatchBackoffer, batch)
				commitDetail := batchExe.committer.getDetail()
				// For prewrite, we record the max backoff time
				if _, ok := batchExe.action.(actionPrewrite); ok {
					commitDetail.Mu.Lock()
					boSleep := int64(singleBatchBackoffer.GetTotalSleep()) * int64(time.Millisecond)
					if boSleep > commitDetail.Mu.CommitBackoffTime {
						commitDetail.Mu.CommitBackoffTime = boSleep
						commitDetail.Mu.BackoffTypes = singleBatchBackoffer.GetTypes()
					}
					commitDetail.Mu.Unlock()
				}
				// Backoff time in the 2nd phase of a non-async-commit txn is added
				// in the commitTxn method, so we don't add it here.
			}()
		} else {
			logutil.Logger(batchExe.backoffer.GetCtx()).Info("break startWorker",
				zap.Stringer("action", batchExe.action), zap.Int("batch size", len(batches)),
				zap.Int("index", idx))
			break
		}
	}
}

// process will start worker routine and collect results
func (batchExe *batchExecutor) process(batches []batchMutations) error {
	var err error
	err = batchExe.initUtils()
	if err != nil {
		logutil.Logger(batchExe.backoffer.GetCtx()).Error("batchExecutor initUtils failed", zap.Error(err))
		return err
	}

	// For prewrite, stop sending other requests after receiving first error.
	var cancel context.CancelFunc
	if _, ok := batchExe.action.(actionPrewrite); ok {
		batchExe.backoffer, cancel = batchExe.backoffer.Fork()
		defer cancel()
	}
	// concurrently do the work for each batch.
	ch := make(chan error, len(batches))
	exitCh := make(chan struct{})
	go batchExe.startWorker(exitCh, ch, batches)
	// check results
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			logutil.Logger(batchExe.backoffer.GetCtx()).Debug("2PC doActionOnBatch failed",
				zap.Uint64("session", batchExe.committer.sessionID),
				zap.Stringer("action type", batchExe.action),
				zap.Error(e),
				zap.Uint64("txnStartTS", batchExe.committer.startTS))
			// Cancel other requests and return the first error.
			if cancel != nil {
				logutil.Logger(batchExe.backoffer.GetCtx()).Debug("2PC doActionOnBatch to cancel other actions",
					zap.Uint64("session", batchExe.committer.sessionID),
					zap.Stringer("action type", batchExe.action),
					zap.Uint64("txnStartTS", batchExe.committer.startTS))
				atomic.StoreUint32(&batchExe.committer.prewriteCancelled, 1)
				cancel()
			}
			if err == nil {
				err = e
			}
		}
	}
	close(exitCh)
	if batchExe.tokenWaitDuration > 0 {
		metrics.TiKVTokenWaitDuration.Observe(float64(batchExe.tokenWaitDuration.Nanoseconds()))
	}
	return err
}

func (c *twoPhaseCommitter) setDetail(d *util.CommitDetails) {
	atomic.StorePointer(&c.detail, unsafe.Pointer(d))
}

func (c *twoPhaseCommitter) getDetail() *util.CommitDetails {
	return (*util.CommitDetails)(atomic.LoadPointer(&c.detail))
}

func (c *twoPhaseCommitter) setUndeterminedErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.undeterminedErr = err
}

func (c *twoPhaseCommitter) getUndeterminedErr() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.undeterminedErr
}

func (c *twoPhaseCommitter) mutationsOfKeys(keys [][]byte) CommitterMutations {
	var res PlainMutations
	for i := 0; i < c.mutations.Len(); i++ {
		for _, key := range keys {
			if bytes.Equal(c.mutations.GetKey(i), key) {
				res.Push(c.mutations.GetOp(i), c.mutations.GetKey(i), c.mutations.GetValue(i), c.mutations.IsPessimisticLock(i))
				break
			}
		}
	}
	return &res
}
