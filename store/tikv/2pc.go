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
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

type twoPhaseCommitAction interface {
	handleSingleBatch(*twoPhaseCommitter, *Backoffer, batchKeys) error
	String() string
}

type actionPrewrite struct{}
type actionCommit struct{}
type actionCleanup struct{}
type actionPessimisticLock struct {
	*kv.LockCtx
}
type actionPessimisticRollback struct{}

var (
	_ twoPhaseCommitAction = actionPrewrite{}
	_ twoPhaseCommitAction = actionCommit{}
	_ twoPhaseCommitAction = actionCleanup{}
	_ twoPhaseCommitAction = actionPessimisticLock{}
	_ twoPhaseCommitAction = actionPessimisticRollback{}
)

var (
	tikvSecondaryLockCleanupFailureCounterCommit   = metrics.TiKVSecondaryLockCleanupFailureCounter.WithLabelValues("commit")
	tikvSecondaryLockCleanupFailureCounterRollback = metrics.TiKVSecondaryLockCleanupFailureCounter.WithLabelValues("rollback")
	tiKVTxnHeartBeatHistogramOK                    = metrics.TiKVTxnHeartBeatHistogram.WithLabelValues("ok")
	tiKVTxnHeartBeatHistogramError                 = metrics.TiKVTxnHeartBeatHistogram.WithLabelValues("err")
)

// Global variable set by config file.
var (
	PessimisticLockTTL uint64 = 20000 // 20s
)

func (actionPrewrite) String() string {
	return "prewrite"
}

func (actionCommit) String() string {
	return "commit"
}

func (actionCleanup) String() string {
	return "cleanup"
}

func (actionPessimisticLock) String() string {
	return "pessimistic_lock"
}

func (actionPessimisticRollback) String() string {
	return "pessimistic_rollback"
}

// metricsTag returns detail tag for metrics.
func metricsTag(ca twoPhaseCommitAction) string {
	return "2pc_" + ca.String()
}

// twoPhaseCommitter executes a two-phase commit protocol.
type twoPhaseCommitter struct {
	store     *tikvStore
	txn       *tikvTxn
	startTS   uint64
	keys      [][]byte
	mutations map[string]*mutationEx
	lockTTL   uint64
	commitTS  uint64
	priority  pb.CommandPri
	connID    uint64 // connID is used for log.
	cleanWg   sync.WaitGroup
	// maxTxnTimeUse represents max time a Txn may use (in ms) from its startTS to commitTS.
	// We use it to guarantee GC worker will not influence any active txn. The value
	// should be less than GC life time.
	maxTxnTimeUse    uint64
	detail           unsafe.Pointer
	primaryKey       []byte
	forUpdateTS      uint64
	pessimisticTTL   uint64
	noNeedCommitKeys map[string]struct{}

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
	}
}

type mutationEx struct {
	pb.Mutation
	asserted          bool
	isPessimisticLock bool
}

// newTwoPhaseCommitter creates a twoPhaseCommitter.
func newTwoPhaseCommitter(txn *tikvTxn, connID uint64) (*twoPhaseCommitter, error) {
	return &twoPhaseCommitter{
		store:         txn.store,
		txn:           txn,
		startTS:       txn.StartTS(),
		connID:        connID,
		regionTxnSize: map[uint64]int{},
		ttlManager: ttlManager{
			ch: make(chan struct{}),
		},
	}, nil
}

func sendTxnHeartBeat(bo *Backoffer, store *tikvStore, primary []byte, startTS, ttl uint64) (uint64, error) {
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdTxnHeartBeat,
		TxnHeartBeat: &pb.TxnHeartBeatRequest{
			PrimaryLock:   primary,
			StartVersion:  startTS,
			AdviseLockTtl: ttl,
		},
	}
	for {
		loc, err := store.GetRegionCache().LocateKey(bo, primary)
		if err != nil {
			return 0, errors.Trace(err)
		}
		resp, err := store.SendReq(bo, req, loc.Region, readTimeoutShort)
		if err != nil {
			return 0, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return 0, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return 0, errors.Trace(err)
			}
			continue
		}
		if resp.TxnHeartBeat == nil {
			return 0, errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.TxnHeartBeat
		if keyErr := cmdResp.GetError(); keyErr != nil {
			return 0, errors.Errorf("txn %d heartbeat fail, primary key = %v, err = %s", startTS, primary, keyErr.Abort)
		}
		return cmdResp.GetLockTtl(), nil
	}
}

func (c *twoPhaseCommitter) initKeysAndMutations() error {
	var (
		keys            [][]byte
		size            int
		putCnt          int
		delCnt          int
		lockCnt         int
		noNeedCommitKey = make(map[string]struct{})
	)
	mutations := make(map[string]*mutationEx)
	txn := c.txn
	c.isPessimistic = txn.IsPessimistic()
	if c.isPessimistic && len(c.primaryKey) > 0 {
		keys = append(keys, c.primaryKey)
		mutations[string(c.primaryKey)] = &mutationEx{
			Mutation: pb.Mutation{
				Op:  pb.Op_Lock,
				Key: c.primaryKey,
			},
			isPessimisticLock: true,
		}
	}
	err := txn.us.WalkBuffer(func(k kv.Key, v []byte) error {
		var op pb.Op
		if len(v) > 0 {
			if tablecodec.IsUntouchedIndexKValue(k, v) {
				return nil
			}
			op = pb.Op_Put
			if c := txn.us.LookupConditionPair(k); c != nil && c.ShouldNotExist() {
				op = pb.Op_Insert
			}
			mutations[string(k)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:    op,
					Key:   k,
					Value: v,
				},
			}
			putCnt++
		} else {
			if !txn.IsPessimistic() && txn.us.LookupConditionPair(k) != nil {
				// delete-your-writes keys in optimistic txn need check not exists in prewrite-phase
				// due to `Op_CheckNotExists` doesn't prewrite lock, so mark those keys should not be used in commit-phase.
				op = pb.Op_CheckNotExists
				noNeedCommitKey[string(k)] = struct{}{}
			} else {
				// normal delete keys in optimistic txn can be delete without not exists checking
				// delete-your-writes keys in pessimistic txn can ensure must be no exists so can directly delete them
				op = pb.Op_Del
				delCnt++
			}
			mutations[string(k)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:  op,
					Key: k,
				},
			}
		}
		if c.isPessimistic {
			if !bytes.Equal(k, c.primaryKey) {
				keys = append(keys, k)
			}
		} else {
			if len(c.primaryKey) == 0 && op != pb.Op_CheckNotExists {
				c.primaryKey = k
			}
			keys = append(keys, k)
		}
		entrySize := len(k) + len(v)
		if entrySize > kv.TxnEntrySizeLimit {
			return kv.ErrEntryTooLarge.GenWithStackByArgs(kv.TxnEntrySizeLimit, entrySize)
		}
		size += entrySize
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	for _, lockKey := range txn.lockKeys {
		muEx, ok := mutations[string(lockKey)]
		if !ok {
			mutations[string(lockKey)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:  pb.Op_Lock,
					Key: lockKey,
				},
				isPessimisticLock: c.isPessimistic,
			}
			lockCnt++
			keys = append(keys, lockKey)
			size += len(lockKey)
		} else {
			muEx.isPessimisticLock = c.isPessimistic
		}
	}
	if len(keys) == 0 {
		return nil
	}

	for _, pair := range txn.assertions {
		mutation, ok := mutations[string(pair.key)]
		if !ok {
			// It's possible when a transaction inserted a key then deleted it later.
			continue
		}
		// Only apply the first assertion!
		if mutation.asserted {
			continue
		}
		switch pair.assertion {
		case kv.Exist:
			mutation.Assertion = pb.Assertion_Exist
		case kv.NotExist:
			mutation.Assertion = pb.Assertion_NotExist
		default:
			mutation.Assertion = pb.Assertion_None
		}
		mutation.asserted = true
	}

	entrylimit := atomic.LoadUint64(&kv.TxnEntryCountLimit)
	if len(keys) > int(entrylimit) || size > int(kv.TxnTotalSizeLimit) {
		return kv.ErrTxnTooLarge
	}
	const logEntryCount = 10000
	const logSize = 4 * 1024 * 1024 // 4MB
	if len(keys) > logEntryCount || size > logSize {
		tableID := tablecodec.DecodeTableID(keys[0])
		logutil.Logger(context.Background()).Info("[BIG_TXN]",
			zap.Uint64("con", c.connID),
			zap.Int64("table ID", tableID),
			zap.Int("size", size),
			zap.Int("keys", len(keys)),
			zap.Int("puts", putCnt),
			zap.Int("dels", delCnt),
			zap.Int("locks", lockCnt),
			zap.Int("checks", len(noNeedCommitKey)),
			zap.Uint64("txnStartTS", txn.startTS))
	}

	// Convert from sec to ms
	maxTxnTimeUse := uint64(config.GetGlobalConfig().TiKVClient.MaxTxnTimeUse) * 1000

	// Sanity check for startTS.
	if txn.StartTS() == math.MaxUint64 {
		err = errors.Errorf("try to commit with invalid txnStartTS: %d", txn.StartTS())
		logutil.Logger(context.Background()).Error("commit failed",
			zap.Uint64("conn", c.connID),
			zap.Error(err))
		return errors.Trace(err)
	}

	commitDetail := &execdetails.CommitDetails{WriteSize: size, WriteKeys: len(keys)}
	metrics.TiKVTxnWriteKVCountHistogram.Observe(float64(commitDetail.WriteKeys))
	metrics.TiKVTxnWriteSizeHistogram.Observe(float64(commitDetail.WriteSize))
	c.maxTxnTimeUse = maxTxnTimeUse
	c.keys = keys
	c.noNeedCommitKeys = noNeedCommitKey
	c.mutations = mutations
	c.lockTTL = txnLockTTL(txn.startTime, size)
	c.priority = getTxnPriority(txn)
	c.syncLog = getTxnSyncLog(txn)
	c.setDetail(commitDetail)
	return nil
}

func (c *twoPhaseCommitter) primary() []byte {
	if len(c.primaryKey) == 0 {
		return c.keys[0]
	}
	return c.primaryKey
}

const bytesPerMiB = 1024 * 1024

func txnLockTTL(startTime time.Time, txnSize int) uint64 {
	// Increase lockTTL for large transactions.
	// The formula is `ttl = ttlFactor * sqrt(sizeInMiB)`.
	// When writeSize is less than 256KB, the base ttl is defaultTTL (3s);
	// When writeSize is 1MiB, 100MiB, or 400MiB, ttl is 6s, 60s, 120s correspondingly;
	lockTTL := defaultLockTTL
	if txnSize >= txnCommitBatchSize {
		sizeMiB := float64(txnSize) / bytesPerMiB
		lockTTL = uint64(float64(ttlFactor) * math.Sqrt(sizeMiB))
		if lockTTL < defaultLockTTL {
			lockTTL = defaultLockTTL
		}
		if lockTTL > maxLockTTL {
			lockTTL = maxLockTTL
		}
	}

	// Increase lockTTL by the transaction's read time.
	// When resolving a lock, we compare current ts and startTS+lockTTL to decide whether to clean up. If a txn
	// takes a long time to read, increasing its TTL will help to prevent it from been aborted soon after prewrite.
	elapsed := time.Since(startTime) / time.Millisecond
	return lockTTL + uint64(elapsed)
}

// doActionOnKeys groups keys into primary batch and secondary batches, if primary batch exists in the key,
// it does action on primary batch first, then on secondary batches. If action is commit, secondary batches
// is done in background goroutine.
func (c *twoPhaseCommitter) doActionOnKeys(bo *Backoffer, action twoPhaseCommitAction, keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	groups, firstRegion, err := c.store.regionCache.GroupKeysByRegion(bo, keys, nil)
	if err != nil {
		return errors.Trace(err)
	}

	metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(metricsTag(action)).Observe(float64(len(groups)))

	var batches []batchKeys
	var sizeFunc = c.keySize
	if _, ok := action.(actionPrewrite); ok {
		// Do not update regionTxnSize on retries. They are not used when building a PrewriteRequest.
		if len(bo.errors) == 0 {
			for region, keys := range groups {
				c.regionTxnSize[region.id] = len(keys)
			}
		}
		sizeFunc = c.keyValueSize
		atomic.AddInt32(&c.getDetail().PrewriteRegionNum, int32(len(groups)))
	}
	// Make sure the group that contains primary key goes first.
	batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], sizeFunc, txnCommitBatchSize)
	delete(groups, firstRegion)
	for id, g := range groups {
		batches = appendBatchBySize(batches, id, g, sizeFunc, txnCommitBatchSize)
	}

	firstIsPrimary := bytes.Equal(keys[0], c.primary())
	_, actionIsCommit := action.(actionCommit)
	_, actionIsCleanup := action.(actionCleanup)
	if firstIsPrimary && (actionIsCommit || actionIsCleanup) {
		// primary should be committed/cleanup first
		err = c.doActionOnBatches(bo, action, batches[:1])
		if err != nil {
			return errors.Trace(err)
		}
		if actionIsCommit && c.testingKnobs.bkAfterCommitPrimary != nil && c.testingKnobs.acAfterCommitPrimary != nil {
			c.testingKnobs.acAfterCommitPrimary <- struct{}{}
			<-c.testingKnobs.bkAfterCommitPrimary
		}
		batches = batches[1:]
	}
	if actionIsCommit {
		// Commit secondary batches in background goroutine to reduce latency.
		// The backoffer instance is created outside of the goroutine to avoid
		// potencial data race in unit test since `CommitMaxBackoff` will be updated
		// by test suites.
		secondaryBo := NewBackoffer(context.Background(), CommitMaxBackoff)
		go func() {
			e := c.doActionOnBatches(secondaryBo, action, batches)
			if e != nil {
				logutil.Logger(context.Background()).Debug("2PC async doActionOnBatches",
					zap.Uint64("conn", c.connID),
					zap.Stringer("action type", action),
					zap.Error(e))
				tikvSecondaryLockCleanupFailureCounterCommit.Inc()
			}
		}()
	} else {
		err = c.doActionOnBatches(bo, action, batches)
	}
	return errors.Trace(err)
}

// doActionOnBatches does action to batches in parallel.
func (c *twoPhaseCommitter) doActionOnBatches(bo *Backoffer, action twoPhaseCommitAction, batches []batchKeys) error {
	if len(batches) == 0 {
		return nil
	}
	if len(batches) == 1 {
		e := action.handleSingleBatch(c, bo, batches[0])
		if e != nil {
			logutil.Logger(bo.ctx).Debug("2PC doActionOnBatches failed",
				zap.Uint64("conn", c.connID),
				zap.Stringer("action type", action),
				zap.Error(e),
				zap.Uint64("txnStartTS", c.startTS))
		}
		return errors.Trace(e)
	}

	// For prewrite, stop sending other requests after receiving first error.
	backoffer := bo
	var cancel context.CancelFunc
	if _, ok := action.(actionPrewrite); ok {
		backoffer, cancel = bo.Fork()
		defer cancel()
	}

	// Concurrently do the work for each batch.
	ch := make(chan error, len(batches))
	for _, batch1 := range batches {

		batch := batch1
		go func() {
			var singleBatchBackoffer *Backoffer
			if _, ok := action.(actionCommit); ok {
				// Because the secondary batches of the commit actions are implemented to be
				// committed asynchronously in background goroutines, we should not
				// fork a child context and call cancel() while the foreground goroutine exits.
				// Otherwise the background goroutines will be canceled execeptionally.
				// Here we makes a new clone of the original backoffer for this goroutine
				// exclusively to avoid the data race when using the same backoffer
				// in concurrent goroutines.
				singleBatchBackoffer = backoffer.Clone()
			} else {
				var singleBatchCancel context.CancelFunc
				singleBatchBackoffer, singleBatchCancel = backoffer.Fork()
				defer singleBatchCancel()
			}
			beforeSleep := singleBatchBackoffer.totalSleep
			ch <- action.handleSingleBatch(c, singleBatchBackoffer, batch)
			commitDetail := c.getDetail()
			if commitDetail != nil { // lock operations of pessimistic-txn will let commitDetail be nil
				if delta := singleBatchBackoffer.totalSleep - beforeSleep; delta > 0 {
					atomic.AddInt64(&commitDetail.CommitBackoffTime, int64(singleBatchBackoffer.totalSleep-beforeSleep)*int64(time.Millisecond))
					commitDetail.Mu.Lock()
					commitDetail.Mu.BackoffTypes = append(commitDetail.Mu.BackoffTypes, singleBatchBackoffer.types...)
					commitDetail.Mu.Unlock()
				}
			}
		}()
	}
	var err error
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			logutil.Logger(context.Background()).Debug("2PC doActionOnBatches failed",
				zap.Uint64("conn", c.connID),
				zap.Stringer("action type", action),
				zap.Error(e),
				zap.Uint64("txnStartTS", c.startTS))
			// Cancel other requests and return the first error.
			if cancel != nil {
				logutil.Logger(context.Background()).Debug("2PC doActionOnBatches to cancel other actions",
					zap.Uint64("conn", c.connID),
					zap.Stringer("action type", action),
					zap.Uint64("txnStartTS", c.startTS))
				cancel()
			}
			if err == nil {
				err = e
			}
		}
	}
	return errors.Trace(err)
}

func (c *twoPhaseCommitter) keyValueSize(key []byte) int {
	size := len(key)
	if mutation := c.mutations[string(key)]; mutation != nil {
		size += len(mutation.Value)
	}
	return size
}

func (c *twoPhaseCommitter) keySize(key []byte) int {
	return len(key)
}

func (c *twoPhaseCommitter) buildPrewriteRequest(batch batchKeys, txnSize uint64) *tikvrpc.Request {
	mutations := make([]*pb.Mutation, len(batch.keys))
	var isPessimisticLock []bool
	if c.isPessimistic {
		isPessimisticLock = make([]bool, len(mutations))
	}
	for i, k := range batch.keys {
		tmp := c.mutations[string(k)]
		mutations[i] = &tmp.Mutation
		if tmp.isPessimisticLock {
			isPessimisticLock[i] = true
		}
	}
	return &tikvrpc.Request{
		Type: tikvrpc.CmdPrewrite,
		Prewrite: &pb.PrewriteRequest{
			Mutations:         mutations,
			PrimaryLock:       c.primary(),
			StartVersion:      c.startTS,
			LockTtl:           c.lockTTL,
			IsPessimisticLock: isPessimisticLock,
			ForUpdateTs:       c.forUpdateTS,
			TxnSize:           txnSize,
		},
		Context: pb.Context{
			Priority: c.priority,
			SyncLog:  c.syncLog,
		},
	}
}

func (actionPrewrite) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchKeys) error {
	txnSize := uint64(c.regionTxnSize[batch.region.id])
	// When we retry because of a region miss, we don't know the transaction size. We set the transaction size here
	// to MaxUint64 to avoid unexpected "resolve lock lite".
	if len(bo.errors) > 0 {
		txnSize = math.MaxUint64
	}

	req := c.buildPrewriteRequest(batch, txnSize)
	for {
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = c.prewriteKeys(bo, batch.keys)
			return errors.Trace(err)
		}
		prewriteResp := resp.Prewrite
		if prewriteResp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		keyErrs := prewriteResp.GetErrors()
		if len(keyErrs) == 0 {
			return nil
		}
		var locks []*Lock
		for _, keyErr := range keyErrs {
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				key := alreadyExist.GetKey()
				conditionPair := c.txn.us.LookupConditionPair(key)
				if conditionPair == nil {
					return errors.Errorf("conn%d, conditionPair for key:%s should not be nil", c.connID, key)
				}
				logutil.Logger(context.Background()).Debug("key already exists",
					zap.Uint64("conn", c.connID),
					zap.Binary("key", key))
				return errors.Trace(conditionPair.Err())
			}

			// Extract lock from key error
			lock, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				return errors.Trace(err1)
			}
			logutil.Logger(context.Background()).Debug("prewrite encounters lock",
				zap.Uint64("conn", c.connID),
				zap.Stringer("lock", lock))
			locks = append(locks, lock)
		}
		start := time.Now()
		msBeforeExpired, err := c.store.lockResolver.ResolveLocks(bo, locks)
		if err != nil {
			return errors.Trace(err)
		}
		atomic.AddInt64(&c.getDetail().ResolveLockTime, int64(time.Since(start)))
		if msBeforeExpired > 0 {
			err = bo.BackoffWithMaxSleep(BoTxnLock, int(msBeforeExpired), errors.Errorf("2PC prewrite lockedKeys: %d", len(locks)))
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

type ttlManagerState uint32

const (
	stateUninitialized ttlManagerState = iota
	stateRunning
	stateClosed
)

type ttlManager struct {
	state  ttlManagerState
	ch     chan struct{}
	killed *uint32
}

func (tm *ttlManager) run(c *twoPhaseCommitter, killed *uint32) {
	// Run only once.
	if !atomic.CompareAndSwapUint32((*uint32)(&tm.state), uint32(stateUninitialized), uint32(stateRunning)) {
		return
	}
	tm.killed = killed
	go tm.keepAlive(c)
}

func (tm *ttlManager) close() {
	if !atomic.CompareAndSwapUint32((*uint32)(&tm.state), uint32(stateRunning), uint32(stateClosed)) {
		return
	}
	close(tm.ch)
}

func (tm *ttlManager) keepAlive(c *twoPhaseCommitter) {
	// Ticker is set to 1/2 of the PessimisticLockTTL.
	ticker := time.NewTicker(time.Duration(PessimisticLockTTL) * time.Millisecond / 2)
	defer ticker.Stop()
	for {
		select {
		case <-tm.ch:
			return
		case <-ticker.C:
			// If kill signal is received, the ttlManager should exit.
			if tm.killed != nil && atomic.LoadUint32(tm.killed) != 0 {
				return
			}
			bo := NewBackoffer(context.Background(), pessimisticLockMaxBackoff)
			now, err := c.store.GetOracle().GetTimestamp(bo.ctx)
			if err != nil {
				err1 := bo.Backoff(BoPDRPC, err)
				if err1 != nil {
					logutil.Logger(context.Background()).Warn("keepAlive get tso fail",
						zap.Error(err))
					return
				}
				continue
			}

			uptime := uint64(oracle.ExtractPhysical(now) - oracle.ExtractPhysical(c.startTS))
			const c10min = 10 * 60 * 1000
			if uptime > c10min {
				// Set a 10min maximum lifetime for the ttlManager, so when something goes wrong
				// the key will not be locked forever.
				logutil.Logger(context.Background()).Info("ttlManager live up to its lifetime",
					zap.Uint64("txnStartTS", c.startTS))
				metrics.TiKVTTLLifeTimeReachCounter.Inc()
				return
			}

			newTTL := uptime + PessimisticLockTTL
			logutil.Logger(context.Background()).Info("send TxnHeartBeat",
				zap.Uint64("startTS", c.startTS), zap.Uint64("newTTL", newTTL))
			startTime := time.Now()
			_, err = sendTxnHeartBeat(bo, c.store, c.primary(), c.startTS, newTTL)
			if err != nil {
				tiKVTxnHeartBeatHistogramError.Observe(time.Since(startTime).Seconds())
				logutil.Logger(context.Background()).Warn("send TxnHeartBeat failed",
					zap.Error(err),
					zap.Uint64("txnStartTS", c.startTS))
				return
			}
			tiKVTxnHeartBeatHistogramOK.Observe(time.Since(startTime).Seconds())
		}
	}
}

func (action actionPessimisticLock) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchKeys) error {
	mutations := make([]*pb.Mutation, len(batch.keys))
	for i, k := range batch.keys {
		mut := &pb.Mutation{
			Op:  pb.Op_PessimisticLock,
			Key: k,
		}
		conditionPair := c.txn.us.LookupConditionPair(k)
		if conditionPair != nil && conditionPair.ShouldNotExist() {
			mut.Assertion = pb.Assertion_NotExist
		}
		mutations[i] = mut
	}

	elapsed := uint64(time.Since(c.txn.startTime) / time.Millisecond)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdPessimisticLock,
		PessimisticLock: &pb.PessimisticLockRequest{
			Mutations:    mutations,
			PrimaryLock:  c.primary(),
			StartVersion: c.startTS,
			ForUpdateTs:  c.forUpdateTS,
			LockTtl:      elapsed + PessimisticLockTTL,
			IsFirstLock:  c.isFirstLock,
			WaitTimeout:  action.LockWaitTime,
		},
		Context: pb.Context{
			Priority: c.priority,
			SyncLog:  c.syncLog,
		},
	}
	lockWaitStartTime := action.WaitStartTime
	for {
		// if lockWaitTime set, refine the request `WaitTimeout` field based on timeout limit
		if action.LockWaitTime > 0 {
			timeLeft := action.LockWaitTime - (time.Since(lockWaitStartTime)).Milliseconds()
			if timeLeft <= 0 {
				req.PessimisticLock.WaitTimeout = kv.LockNoWait
			} else {
				req.PessimisticLock.WaitTimeout = timeLeft
			}
		}
		failpoint.Inject("PessimisticLockErrWriteConflict", func() error {
			time.Sleep(300 * time.Millisecond)
			return kv.ErrWriteConflict
		})
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = c.pessimisticLockKeys(bo, action.LockCtx, batch.keys)
			return errors.Trace(err)
		}
		lockResp := resp.PessimisticLock
		if lockResp == nil {
			return errors.Trace(ErrBodyMissing)
		}
		keyErrs := lockResp.GetErrors()
		if len(keyErrs) == 0 {
			return nil
		}
		var locks []*Lock
		for _, keyErr := range keyErrs {
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				key := alreadyExist.GetKey()
				conditionPair := c.txn.us.LookupConditionPair(key)
				if conditionPair == nil {
					panic(fmt.Sprintf("con:%d, conditionPair for key:%s should not be nil", c.connID, key))
				}
				return errors.Trace(conditionPair.Err())
			}
			if deadlock := keyErr.Deadlock; deadlock != nil {
				return &ErrDeadlock{Deadlock: deadlock}
			}

			// Extract lock from key error
			lock, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				return errors.Trace(err1)
			}
			locks = append(locks, lock)
		}
		// Because we already waited on tikv, no need to Backoff here.
		msBeforeTxnExpired, err := c.store.lockResolver.ResolveLocks(bo, locks)
		if err != nil {
			return errors.Trace(err)
		}

		// If msBeforeTxnExpired is not zero, it means there are still locks blocking us acquiring
		// the pessimistic lock. We should return timeout error if necessary.
		if msBeforeTxnExpired > 0 {
			if action.LockWaitTime == kv.LockNoWait {
				// 3.0 release not supported yet
				return kv.ErrNotImplemented
			} else if action.LockWaitTime == kv.LockAlwaysWait {
				// do nothing but keep wait
			} else {
				// the lockWaitTime is set, we should return wait timeout if we are still blocked by a lock
				if time.Since(lockWaitStartTime).Milliseconds() >= action.LockWaitTime {
					return ErrLockWaitTimeout
				}
			}
			if action.LockCtx.PessimisticLockWaited != nil {
				atomic.StoreInt32(action.LockCtx.PessimisticLockWaited, 1)
			}
		}

		// Handle the killed flag when waiting for the pessimistic lock.
		// When a txn runs into LockKeys() and backoff here, it has no chance to call
		// executor.Next() and check the killed flag.
		if action.Killed != nil {
			// Do not reset the killed flag here!
			// actionPessimisticLock runs on each region parallelly, we have to consider that
			// the error may be dropped.
			if atomic.LoadUint32(action.Killed) == 1 {
				return ErrQueryInterrupted
			}
		}
	}
}

func (actionPessimisticRollback) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchKeys) error {
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdPessimisticRollback,
		PessimisticRollback: &pb.PessimisticRollbackRequest{
			StartVersion: c.startTS,
			ForUpdateTs:  c.forUpdateTS,
			Keys:         batch.keys,
		},
	}
	for {
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = c.pessimisticRollbackKeys(bo, batch.keys)
			return errors.Trace(err)
		}
		return nil
	}
}

func getTxnPriority(txn *tikvTxn) pb.CommandPri {
	if pri := txn.us.GetOption(kv.Priority); pri != nil {
		return kvPriorityToCommandPri(pri.(int))
	}
	return pb.CommandPri_Normal
}

func getTxnSyncLog(txn *tikvTxn) bool {
	if syncOption := txn.us.GetOption(kv.SyncLog); syncOption != nil {
		return syncOption.(bool)
	}
	return false
}

func kvPriorityToCommandPri(pri int) pb.CommandPri {
	switch pri {
	case kv.PriorityLow:
		return pb.CommandPri_Low
	case kv.PriorityHigh:
		return pb.CommandPri_High
	}
	return pb.CommandPri_Normal
}

func (c *twoPhaseCommitter) setDetail(d *execdetails.CommitDetails) {
	atomic.StorePointer(&c.detail, unsafe.Pointer(d))
}

func (c *twoPhaseCommitter) getDetail() *execdetails.CommitDetails {
	return (*execdetails.CommitDetails)(atomic.LoadPointer(&c.detail))
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

func (actionCommit) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchKeys) error {
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdCommit,
		Commit: &pb.CommitRequest{
			StartVersion:  c.startTS,
			Keys:          batch.keys,
			CommitVersion: c.commitTS,
		},
		Context: pb.Context{
			Priority: c.priority,
			SyncLog:  c.syncLog,
		},
	}

	sender := NewRegionRequestSender(c.store.regionCache, c.store.client)
	resp, err := sender.SendReq(bo, req, batch.region, readTimeoutShort)

	// If we fail to receive response for the request that commits primary key, it will be undetermined whether this
	// transaction has been successfully committed.
	// Under this circumstance,  we can not declare the commit is complete (may lead to data lost), nor can we throw
	// an error (may lead to the duplicated key error when upper level restarts the transaction). Currently the best
	// solution is to populate this error and let upper layer drop the connection to the corresponding mysql client.
	isPrimary := bytes.Equal(batch.keys[0], c.primary())
	if isPrimary && sender.rpcError != nil {
		c.setUndeterminedErr(errors.Trace(sender.rpcError))
	}

	if err != nil {
		return errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		// re-split keys and commit again.
		err = c.commitKeys(bo, batch.keys)
		return errors.Trace(err)
	}
	commitResp := resp.Commit
	if commitResp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	// Here we can make sure tikv has processed the commit primary key request. So
	// we can clean undetermined error.
	if isPrimary {
		c.setUndeterminedErr(nil)
	}
	if keyErr := commitResp.GetError(); keyErr != nil {
		c.mu.RLock()
		defer c.mu.RUnlock()
		err = extractKeyErr(keyErr)
		if c.mu.committed {
			// No secondary key could be rolled back after it's primary key is committed.
			// There must be a serious bug somewhere.
			logutil.Logger(context.Background()).Error("2PC failed commit key after primary key committed",
				zap.Error(err),
				zap.Uint64("txnStartTS", c.startTS))
			return errors.Trace(err)
		}
		// The transaction maybe rolled back by concurrent transactions.
		logutil.Logger(context.Background()).Debug("2PC failed commit primary key",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Group that contains primary key is always the first.
	// We mark transaction's status committed when we receive the first success response.
	c.mu.committed = true
	return nil
}

func (actionCleanup) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchKeys) error {
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdBatchRollback,
		BatchRollback: &pb.BatchRollbackRequest{
			Keys:         batch.keys,
			StartVersion: c.startTS,
		},
		Context: pb.Context{
			Priority: c.priority,
			SyncLog:  c.syncLog,
		},
	}
	resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
	if err != nil {
		return errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		err = c.cleanupKeys(bo, batch.keys)
		return errors.Trace(err)
	}
	if keyErr := resp.BatchRollback.GetError(); keyErr != nil {
		err = errors.Errorf("conn%d 2PC cleanup failed: %s", c.connID, keyErr)
		logutil.Logger(context.Background()).Debug("2PC failed cleanup key",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}
	return nil
}

func (c *twoPhaseCommitter) prewriteKeys(bo *Backoffer, keys [][]byte) error {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("twoPhaseCommitter.prewriteKeys", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}

	return c.doActionOnKeys(bo, actionPrewrite{}, keys)
}

func (c *twoPhaseCommitter) commitKeys(bo *Backoffer, keys [][]byte) error {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("twoPhaseCommitter.commitKeys", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}

	return c.doActionOnKeys(bo, actionCommit{}, keys)
}

func (c *twoPhaseCommitter) cleanupKeys(bo *Backoffer, keys [][]byte) error {
	return c.doActionOnKeys(bo, actionCleanup{}, keys)
}

func (c *twoPhaseCommitter) pessimisticLockKeys(bo *Backoffer, lockCtx *kv.LockCtx, keys [][]byte) error {
	return c.doActionOnKeys(bo, actionPessimisticLock{lockCtx}, keys)
}

func (c *twoPhaseCommitter) pessimisticRollbackKeys(bo *Backoffer, keys [][]byte) error {
	return c.doActionOnKeys(bo, actionPessimisticRollback{}, keys)
}

// execute executes the two-phase commit protocol.
func (c *twoPhaseCommitter) execute(ctx context.Context) (err error) {
	var binlogSkipped bool
	defer func() {
		// Always clean up all written keys if the txn does not commit.
		c.mu.RLock()
		committed := c.mu.committed
		undetermined := c.mu.undeterminedErr != nil
		c.mu.RUnlock()
		if !committed && !undetermined {
			c.cleanWg.Add(1)
			go func() {
				cleanupKeysCtx := context.WithValue(context.Background(), txnStartKey, ctx.Value(txnStartKey))
				err := c.cleanupKeys(NewBackoffer(cleanupKeysCtx, cleanupMaxBackoff).WithVars(c.txn.vars), c.keys)
				if err != nil {
					tikvSecondaryLockCleanupFailureCounterRollback.Inc()
					logutil.Logger(ctx).Info("2PC cleanup failed",
						zap.Error(err),
						zap.Uint64("txnStartTS", c.startTS))
				} else {
					logutil.Logger(ctx).Info("2PC clean up done",
						zap.Uint64("txnStartTS", c.startTS))
				}
				c.cleanWg.Done()
			}()
		}
		c.txn.commitTS = c.commitTS
		if binlogSkipped {
			binloginfo.RemoveOneSkippedCommitter()
		} else {
			if err != nil {
				c.writeFinishBinlog(binlog.BinlogType_Rollback, 0)
			} else {
				c.writeFinishBinlog(binlog.BinlogType_Commit, int64(c.commitTS))
			}
		}
	}()

	binlogChan := c.prewriteBinlog()
	prewriteBo := NewBackoffer(ctx, prewriteMaxBackoff).WithVars(c.txn.vars)
	start := time.Now()
	err = c.prewriteKeys(prewriteBo, c.keys)
	commitDetail := c.getDetail()
	commitDetail.PrewriteTime = time.Since(start)
	if prewriteBo.totalSleep > 0 {
		atomic.AddInt64(&commitDetail.CommitBackoffTime, int64(prewriteBo.totalSleep)*int64(time.Millisecond))
		commitDetail.Mu.Lock()
		commitDetail.Mu.BackoffTypes = append(commitDetail.Mu.BackoffTypes, prewriteBo.types...)
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

	start = time.Now()
	commitTS, err := c.store.getTimestampWithRetry(NewBackoffer(ctx, tsoMaxBackoff).WithVars(c.txn.vars))
	if err != nil {
		logutil.Logger(ctx).Warn("2PC get commitTS failed",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}
	commitDetail.GetCommitTsTime = time.Since(start)

	// check commitTS
	if commitTS <= c.startTS {
		err = errors.Errorf("conn%d Invalid transaction tso with txnStartTS=%v while txnCommitTS=%v",
			c.connID, c.startTS, commitTS)
		logutil.Logger(context.Background()).Error("invalid transaction", zap.Error(err))
		return errors.Trace(err)
	}
	c.commitTS = commitTS
	if err = c.checkSchemaValid(); err != nil {
		return errors.Trace(err)
	}

	failpoint.Inject("tmpMaxTxnTime", func(val failpoint.Value) {
		if tmpMaxTxnTime := uint64(val.(int)); tmpMaxTxnTime > 0 {
			c.maxTxnTimeUse = tmpMaxTxnTime
		}
	})

	if c.store.oracle.IsExpired(c.startTS, c.maxTxnTimeUse) {
		err = errors.Errorf("conn%d txn takes too much time, txnStartTS: %d, comm: %d",
			c.connID, c.startTS, c.commitTS)
		return err
	}

	start = time.Now()
	commitBo := NewBackoffer(ctx, CommitMaxBackoff).WithVars(c.txn.vars)
	err = c.commitKeys(commitBo, c.keys)
	commitDetail.CommitTime = time.Since(start)
	if commitBo.totalSleep > 0 {
		atomic.AddInt64(&commitDetail.CommitBackoffTime, int64(commitBo.totalSleep)*int64(time.Millisecond))
		commitDetail.Mu.Lock()
		commitDetail.Mu.BackoffTypes = append(commitDetail.Mu.BackoffTypes, commitBo.types...)
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
		logutil.Logger(ctx).Debug("2PC succeed with error",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
	}
	return nil
}

func (c *twoPhaseCommitter) stripNoNeedCommitKeys() {
	if len(c.noNeedCommitKeys) == 0 {
		return
	}
	var i int
	for _, k := range c.keys {
		if _, ck := c.noNeedCommitKeys[string(k)]; ck {
			continue
		}
		c.keys[i] = k
		i++
	}
	c.keys = c.keys[:i]
}

type schemaLeaseChecker interface {
	Check(txnTS uint64) error
}

func (c *twoPhaseCommitter) checkSchemaValid() error {
	checker, ok := c.txn.us.GetOption(kv.SchemaChecker).(schemaLeaseChecker)
	if ok {
		err := checker.Check(c.commitTS)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *twoPhaseCommitter) prewriteBinlog() chan *binloginfo.WriteResult {
	if !c.shouldWriteBinlog() {
		return nil
	}
	ch := make(chan *binloginfo.WriteResult, 1)
	go func() {
		binInfo := c.txn.us.GetOption(kv.BinlogInfo).(*binloginfo.BinlogInfo)
		bin := binInfo.Data
		bin.StartTs = int64(c.startTS)
		if bin.Tp == binlog.BinlogType_Prewrite {
			bin.PrewriteKey = c.keys[0]
		}
		wr := binInfo.WriteBinlog(c.store.clusterID)
		if wr.Skipped() {
			binInfo.Data.PrewriteValue = nil
			binloginfo.AddOneSkippedCommitter()
		}
		ch <- wr
	}()
	return ch
}

func (c *twoPhaseCommitter) writeFinishBinlog(tp binlog.BinlogType, commitTS int64) {
	if !c.shouldWriteBinlog() {
		return
	}
	binInfo := c.txn.us.GetOption(kv.BinlogInfo).(*binloginfo.BinlogInfo)
	binInfo.Data.Tp = tp
	binInfo.Data.CommitTs = commitTS
	binInfo.Data.PrewriteValue = nil
	go func() {
		binlogWriteResult := binInfo.WriteBinlog(c.store.clusterID)
		err := binlogWriteResult.GetError()
		if err != nil {
			logutil.Logger(context.Background()).Error("failed to write binlog",
				zap.Error(err))
		}
	}()
}

func (c *twoPhaseCommitter) shouldWriteBinlog() bool {
	return c.txn.us.GetOption(kv.BinlogInfo) != nil
}

// TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
// Key+Value size below 16KB.
const txnCommitBatchSize = 16 * 1024

// batchKeys is a batch of keys in the same region.
type batchKeys struct {
	region RegionVerID
	keys   [][]byte
}

// appendBatchBySize appends keys to []batchKeys. It may split the keys to make
// sure each batch's size does not exceed the limit.
func appendBatchBySize(b []batchKeys, region RegionVerID, keys [][]byte, sizeFn func([]byte) int, limit int) []batchKeys {
	var start, end int
	for start = 0; start < len(keys); start = end {
		var size int
		for end = start; end < len(keys) && size < limit; end++ {
			size += sizeFn(keys[end])
		}
		b = append(b, batchKeys{
			region: region,
			keys:   keys[start:end],
		})
	}
	return b
}
