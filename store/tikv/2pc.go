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
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

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
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type twoPhaseCommitAction interface {
	handleSingleBatch(*twoPhaseCommitter, *Backoffer, *batchMutations) (bool, error)
	tiKVTxnRegionsNumHistogram() prometheus.Observer
	String() string
}

var (
	tikvSecondaryLockCleanupFailureCounterRollback = metrics.TiKVSecondaryLockCleanupFailureCounter.WithLabelValues("rollback")
	tiKVTxnHeartBeatHistogramOK                    = metrics.TiKVTxnHeartBeatHistogram.WithLabelValues("ok")
	tiKVTxnHeartBeatHistogramError                 = metrics.TiKVTxnHeartBeatHistogram.WithLabelValues("err")
)

// Global variable set by config file.
var (
	ManagedLockTTL uint64 = 20000 // 20s
)

// metricsTag returns detail tag for metrics.
func metricsTag(action string) string {
	return "2pc_" + action
}

// twoPhaseCommitter executes a two-phase commit protocol.
type twoPhaseCommitter struct {
	store            *tikvStore
	txn              *tikvTxn
	startTS          uint64
	lockTTL          uint64
	commitTS         uint64
	priority         pb.CommandPri
	connID           uint64 // connID is used for log.
	cleanWg          sync.WaitGroup
	detail           unsafe.Pointer
	txnSize          int
	prewriteOnlyKeys int
	ignoredKeys      int

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

	useAsyncCommit uint32
	minCommitTS    uint64
}

// CommitterMutations contains transaction operations.
type CommitterMutations struct {
	ops               []pb.Op
	keys              [][]byte
	values            [][]byte
	isPessimisticLock []bool
}

// NewCommiterMutations creates a CommitterMutations object with sizeHint reserved.
func NewCommiterMutations(sizeHint int) CommitterMutations {
	return CommitterMutations{
		ops:               make([]pb.Op, 0, sizeHint),
		keys:              make([][]byte, 0, sizeHint),
		values:            make([][]byte, 0, sizeHint),
		isPessimisticLock: make([]bool, 0, sizeHint),
	}
}

func (c *CommitterMutations) subRange(from, to int) CommitterMutations {
	var res CommitterMutations
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
	return res
}

// Push another mutation into mutations.
func (c *CommitterMutations) Push(op pb.Op, key []byte, value []byte, isPessimisticLock bool) {
	c.ops = append(c.ops, op)
	c.keys = append(c.keys, key)
	c.values = append(c.values, value)
	c.isPessimisticLock = append(c.isPessimisticLock, isPessimisticLock)
}

func (c *CommitterMutations) len() int {
	return len(c.keys)
}

// GetKeys returns the keys.
func (c *CommitterMutations) GetKeys() [][]byte {
	return c.keys
}

// GetOps returns the key ops.
func (c *CommitterMutations) GetOps() []pb.Op {
	return c.ops
}

// GetValues returns the key values.
func (c *CommitterMutations) GetValues() [][]byte {
	return c.values
}

// GetPessimisticFlags returns the key pessimistic flags.
func (c *CommitterMutations) GetPessimisticFlags() []bool {
	return c.isPessimisticLock
}

// MergeMutations append input mutations into current mutations.
func (c *CommitterMutations) MergeMutations(mutations CommitterMutations) {
	c.ops = append(c.ops, mutations.ops...)
	c.keys = append(c.keys, mutations.keys...)
	c.values = append(c.values, mutations.values...)
	c.isPessimisticLock = append(c.isPessimisticLock, mutations.isPessimisticLock...)
}

func (c *CommitterMutations) reset() {
	c.ops = c.ops[:0]
	c.keys = c.keys[:0]
	c.values = c.values[:0]
	c.isPessimisticLock = c.isPessimisticLock[:0]
}

// newTwoPhaseCommitter creates a twoPhaseCommitter.
func newTwoPhaseCommitter(txn *tikvTxn, connID uint64) (*twoPhaseCommitter, error) {
	return &twoPhaseCommitter{
		store:   txn.store,
		txn:     txn,
		startTS: txn.StartTS(),
		connID:  connID,
		detail:  unsafe.Pointer(new(execdetails.CommitDetails)),
		ttlManager: ttlManager{
			ch: make(chan struct{}),
		},
		isPessimistic: txn.IsPessimistic(),
	}, nil
}

func (c *twoPhaseCommitter) extractKeyExistsErr(key kv.Key) error {
	if !c.txn.us.HasPresumeKeyNotExists(key) {
		return errors.Errorf("conn %d, existErr for key:%s should not be nil", c.connID, key)
	}

	_, handle, err := tablecodec.DecodeRecordKey(key)
	if err == nil {
		if handle.IsInt() {
			return kv.ErrKeyExists.FastGenByArgs(handle.String(), "PRIMARY")
		}
		trimLen := 0
		for i := 0; i < handle.NumCols(); i++ {
			trimLen += len(handle.EncodedCol(i))
		}
		values, err := tablecodec.DecodeValuesBytesToStrings(handle.Encoded()[:trimLen])
		if err == nil {
			return kv.ErrKeyExists.FastGenByArgs(strings.Join(values, "-"), "PRIMARY")
		}
	}

	tableID, indexID, indexValues, err := tablecodec.DecodeIndexKey(key)
	if err == nil {
		return kv.ErrKeyExists.FastGenByArgs(strings.Join(indexValues, "-"), c.txn.us.GetIndexName(tableID, indexID))
	}

	return kv.ErrKeyExists.FastGenByArgs(key.String(), "UNKNOWN")
}

func (c *twoPhaseCommitter) primary() []byte {
	return c.primaryKey
}

// asyncSecondaries returns all keys that must be checked in the recovery phase of an async commit.
func (c *twoPhaseCommitter) asyncSecondaries() [][]byte {
	secondaries := make([][]byte, 0, c.txn.Len())
	it := committerTxnMutations{c, true}.Iter(nil, nil)
	for {
		m := it.Next()
		if m.key == nil {
			break
		}
		if bytes.Equal(m.key, c.primary()) || m.op == pb.Op_CheckNotExists {
			continue
		}
		secondaries = append(secondaries, m.key)
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

func (c *twoPhaseCommitter) keyValueSize(key, value []byte) int {
	return len(key) + len(value)
}

func (c *twoPhaseCommitter) keySize(key, _ []byte) int {
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
	go tm.keepAlive(c)
}

func (tm *ttlManager) close() {
	if !atomic.CompareAndSwapUint32((*uint32)(&tm.state), uint32(stateRunning), uint32(stateClosed)) {
		return
	}
	close(tm.ch)
}

func (tm *ttlManager) keepAlive(c *twoPhaseCommitter) {
	// Ticker is set to 1/2 of the ManagedLockTTL.
	ticker := time.NewTicker(time.Duration(atomic.LoadUint64(&ManagedLockTTL)) * time.Millisecond / 2)
	defer ticker.Stop()
	for {
		select {
		case <-tm.ch:
			return
		case <-ticker.C:
			// If kill signal is received, the ttlManager should exit.
			if tm.lockCtx != nil && tm.lockCtx.Killed != nil && atomic.LoadUint32(tm.lockCtx.Killed) != 0 {
				return
			}
			bo := NewBackofferWithVars(context.Background(), pessimisticLockMaxBackoff, c.txn.vars)
			now, err := c.store.GetOracle().GetTimestamp(bo.ctx)
			if err != nil {
				err1 := bo.Backoff(BoPDRPC, err)
				if err1 != nil {
					logutil.Logger(bo.ctx).Warn("keepAlive get tso fail",
						zap.Error(err))
					return
				}
				continue
			}

			uptime := uint64(oracle.ExtractPhysical(now) - oracle.ExtractPhysical(c.startTS))
			if uptime > config.GetGlobalConfig().Performance.MaxTxnTTL {
				// Checks maximum lifetime for the ttlManager, so when something goes wrong
				// the key will not be locked forever.
				logutil.Logger(bo.ctx).Info("ttlManager live up to its lifetime",
					zap.Uint64("txnStartTS", c.startTS),
					zap.Uint64("uptime", uptime),
					zap.Uint64("maxTxnTTL", config.GetGlobalConfig().Performance.MaxTxnTTL))
				metrics.TiKVTTLLifeTimeReachCounter.Inc()
				// the pessimistic locks may expire if the ttl manager has timed out, set `LockExpired` flag
				// so that this transaction could only commit or rollback with no more statement executions
				if c.isPessimistic && tm.lockCtx != nil && tm.lockCtx.LockExpired != nil {
					atomic.StoreUint32(tm.lockCtx.LockExpired, 1)
				}
				return
			}

			newTTL := uptime + atomic.LoadUint64(&ManagedLockTTL)
			logutil.Logger(bo.ctx).Info("send TxnHeartBeat",
				zap.Uint64("startTS", c.startTS), zap.Uint64("newTTL", newTTL))
			startTime := time.Now()
			_, err = sendTxnHeartBeat(bo, c.store, c.primary(), c.startTS, newTTL)
			if err != nil {
				tiKVTxnHeartBeatHistogramError.Observe(time.Since(startTime).Seconds())
				logutil.Logger(bo.ctx).Warn("send TxnHeartBeat failed",
					zap.Error(err),
					zap.Uint64("txnStartTS", c.startTS))
				return
			}
			tiKVTxnHeartBeatHistogramOK.Observe(time.Since(startTime).Seconds())
		}
	}
}

func sendTxnHeartBeat(bo *Backoffer, store *tikvStore, primary []byte, startTS, ttl uint64) (uint64, error) {
	req := tikvrpc.NewRequest(tikvrpc.CmdTxnHeartBeat, &pb.TxnHeartBeatRequest{
		PrimaryLock:   primary,
		StartVersion:  startTS,
		AdviseLockTtl: ttl,
	})
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
		if resp.Resp == nil {
			return 0, errors.Trace(ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*pb.TxnHeartBeatResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			return 0, errors.Errorf("txn %d heartbeat fail, primary key = %v, err = %s", startTS, primary, keyErr.Abort)
		}
		return cmdResp.GetLockTtl(), nil
	}
}

// checkAsyncCommit checks if async commit protocol is available for current transaction commit, true is returned if possible.
func (c *twoPhaseCommitter) checkAsyncCommit() bool {
	// TODO the keys limit need more tests, this value makes the unit test pass by now.
	const asyncCommitKeysLimit = 256
	// Async commit is not compatible with Binlog because of the non unique timestamp issue.
	if c.connID > 0 && config.GetGlobalConfig().TiKVClient.EnableAsyncCommit &&
		c.txn.GetMemBuffer().Len() <= asyncCommitKeysLimit && !c.shouldWriteBinlog() {
		return true
	}
	return false
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

func (c *twoPhaseCommitter) cleanup(ctx context.Context) {
	c.cleanWg.Add(1)
	c.txn.GetMemBuffer().DiscardValues()
	go func() {
		cleanupKeysCtx := context.WithValue(context.Background(), txnStartKey, ctx.Value(txnStartKey))
		err := c.cleanupTxnMutations(NewBackofferWithVars(cleanupKeysCtx, cleanupMaxBackoff, c.txn.vars))
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

// execute executes the two-phase commit protocol.
func (c *twoPhaseCommitter) execute(ctx context.Context) (err error) {
	var binlogSkipped bool
	defer func() {
		if !c.isAsyncCommit() {
			// Always clean up all written keys if the txn does not commit.
			c.mu.RLock()
			committed := c.mu.committed
			undetermined := c.mu.undeterminedErr != nil
			c.mu.RUnlock()
			if !committed && !undetermined {
				c.cleanup(ctx)
			}
			c.txn.commitTS = c.commitTS
			if binlogSkipped {
				binloginfo.RemoveOneSkippedCommitter()
			} else {
				if err != nil {
					c.writeFinishBinlog(ctx, binlog.BinlogType_Rollback, 0)
				} else {
					c.writeFinishBinlog(ctx, binlog.BinlogType_Commit, int64(c.commitTS))
				}
			}
		} else {
			// The error means the async commit should not succeed.
			if err != nil {
				c.cleanup(ctx)
			}
		}
	}()

	// Check async commit is available or not.
	if c.checkAsyncCommit() {
		c.setAsyncCommit(true)
	}

	binlogChan := c.prewriteBinlog(ctx)
	prewriteBo := NewBackofferWithVars(ctx, PrewriteMaxBackoff, c.txn.vars)
	start := time.Now()
	err = c.prewriteTxnMutations(prewriteBo)
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

	var commitTS uint64
	if c.isAsyncCommit() {
		if c.minCommitTS == 0 {
			err = errors.Errorf("conn %d invalid minCommitTS for async commit protocol after prewrite, startTS=%v", c.connID, c.startTS)
			return errors.Trace(err)
		}
		commitTS = c.minCommitTS
	} else {
		start = time.Now()
		logutil.Event(ctx, "start get commit ts")
		commitTS, err = c.store.getTimestampWithRetry(NewBackofferWithVars(ctx, tsoMaxBackoff, c.txn.vars))
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

	tryAmend := c.isPessimistic && c.connID > 0 && !c.isAsyncCommit() && c.txn.schemaAmender != nil
	if !tryAmend {
		_, _, err = c.checkSchemaValid(ctx, commitTS, c.txn.txnInfoSchema, false)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		relatedSchemaChange, memAmended, err := c.checkSchemaValid(ctx, commitTS, c.txn.txnInfoSchema, true)
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
				return errors.Trace(err)
			}
			commitTS = newCommitTS
		}
	}
	c.commitTS = commitTS

	if c.store.oracle.IsExpired(c.startTS, kv.MaxTxnTimeUse) {
		err = errors.Errorf("conn %d txn takes too much time, txnStartTS: %d, comm: %d",
			c.connID, c.startTS, c.commitTS)
		return err
	}

	if c.connID > 0 {
		failpoint.Inject("beforeCommit", func() {})
	}

	c.txn.GetMemBuffer().DiscardValues()

	if c.isAsyncCommit() {
		// For async commit protocol, the commit is considered success here.
		c.txn.commitTS = c.commitTS
		logutil.Logger(ctx).Info("2PC will use async commit protocol to commit this txn", zap.Uint64("startTS", c.startTS),
			zap.Uint64("commitTS", c.commitTS))
		go func() {
			failpoint.Inject("asyncCommitDoNothing", func() {
				failpoint.Return()
			})
			defer c.ttlManager.close()
			commitBo := NewBackofferWithVars(ctx, int(atomic.LoadUint64(&CommitMaxBackoff)), c.txn.vars)
			err := c.commitTxnMutations(commitBo)
			if err != nil {
				logutil.Logger(ctx).Warn("2PC async commit failed", zap.Uint64("connID", c.connID),
					zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", c.commitTS), zap.Error(err))
			}
		}()
		return nil
	}
	return c.commitTxn(ctx, commitDetail)
}

func (c *twoPhaseCommitter) commitTxn(ctx context.Context, commitDetail *execdetails.CommitDetails) error {
	start := time.Now()

	commitBo := NewBackofferWithVars(ctx, int(atomic.LoadUint64(&CommitMaxBackoff)), c.txn.vars)
	err := c.commitTxnMutations(commitBo)
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
		logutil.Logger(ctx).Debug("got some exceptions, but 2PC was still successful",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
	}
	return nil
}

// SchemaVer is the infoSchema which will return the schema version.
type SchemaVer interface {
	// SchemaMetaVersion returns the meta schema version.
	SchemaMetaVersion() int64
}

type schemaLeaseChecker interface {
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

func (c *twoPhaseCommitter) tryAmendTxn(ctx context.Context, startInfoSchema SchemaVer, change *RelatedSchemaChange) (bool, error) {
	mutations := NewCommiterMutations(c.txn.Len())
	it := committerTxnMutations{c, true}.Iter(nil, nil)
	for {
		m := it.Next()
		if m.key == nil {
			break
		}
		mutations.Push(m.op, m.key, m.value, m.isPessimisticLock)
	}
	addMutations, err := c.txn.schemaAmender.AmendTxn(ctx, startInfoSchema, change, mutations)
	if err != nil {
		return false, err
	}
	// Prewrite new mutations.
	if addMutations != nil && len(addMutations.keys) > 0 {
		prewriteBo := NewBackofferWithVars(ctx, PrewriteMaxBackoff, c.txn.vars)
		err = c.prewriteMutations(prewriteBo, staticMutations{*addMutations})
		if err != nil {
			logutil.Logger(ctx).Warn("amend prewrite has failed", zap.Error(err), zap.Uint64("txnStartTS", c.startTS))
			return false, err
		}
		logutil.Logger(ctx).Info("amend prewrite finished", zap.Uint64("txnStartTS", c.startTS))
		return true, nil
	}
	return false, nil
}

func (c *twoPhaseCommitter) getCommitTS(ctx context.Context, commitDetail *execdetails.CommitDetails) (uint64, error) {
	start := time.Now()
	logutil.Event(ctx, "start get commit ts")
	commitTS, err := c.store.getTimestampWithRetry(NewBackofferWithVars(ctx, tsoMaxBackoff, c.txn.vars))
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
		err = errors.Errorf("conn %d invalid transaction tso with txnStartTS=%v while txnCommitTS=%v",
			c.connID, c.startTS, commitTS)
		logutil.BgLogger().Error("invalid transaction", zap.Error(err))
		return 0, errors.Trace(err)
	}
	return commitTS, nil
}

// checkSchemaValid checks if the schema has changed, if tryAmend is set to true, committer will try to amend
// this transaction using the related schema changes.
func (c *twoPhaseCommitter) checkSchemaValid(ctx context.Context, checkTS uint64, startInfoSchema SchemaVer,
	tryAmend bool) (*RelatedSchemaChange, bool, error) {
	checker, ok := c.txn.us.GetOption(kv.SchemaChecker).(schemaLeaseChecker)
	if !ok {
		if c.connID > 0 {
			logutil.Logger(ctx).Warn("schemaLeaseChecker is not set for this transaction",
				zap.Uint64("connID", c.connID),
				zap.Uint64("startTS", c.startTS),
				zap.Uint64("commitTS", checkTS))
		}
		return nil, false, nil
	}
	relatedChanges, err := checker.CheckBySchemaVer(checkTS, startInfoSchema)
	if err != nil {
		if tryAmend && relatedChanges != nil && relatedChanges.Amendable && c.txn.schemaAmender != nil {
			memAmended, amendErr := c.tryAmendTxn(ctx, startInfoSchema, relatedChanges)
			if amendErr != nil {
				logutil.BgLogger().Info("txn amend has failed", zap.Uint64("connID", c.connID),
					zap.Uint64("startTS", c.startTS), zap.Error(amendErr))
				return nil, false, err
			}
			logutil.Logger(ctx).Info("amend txn successfully for pessimistic commit",
				zap.Uint64("connID", c.connID), zap.Uint64("txn startTS", c.startTS), zap.Bool("memAmended", memAmended),
				zap.Uint64("checkTS", checkTS), zap.Int64("startInfoSchemaVer", startInfoSchema.SchemaMetaVersion()),
				zap.Int64s("table ids", relatedChanges.PhyTblIDS), zap.Uint64s("action types", relatedChanges.ActionTypes))
			return relatedChanges, memAmended, nil
		}
		return nil, false, errors.Trace(err)
	}
	return nil, false, nil
}

func (c *twoPhaseCommitter) prewriteBinlog(ctx context.Context) chan *binloginfo.WriteResult {
	if !c.shouldWriteBinlog() {
		return nil
	}
	ch := make(chan *binloginfo.WriteResult, 1)
	go func() {
		logutil.Eventf(ctx, "start prewrite binlog")
		binInfo := c.txn.us.GetOption(kv.BinlogInfo).(*binloginfo.BinlogInfo)
		bin := binInfo.Data
		bin.StartTs = int64(c.startTS)
		if bin.Tp == binlog.BinlogType_Prewrite {
			bin.PrewriteKey = c.primary()
		}
		wr := binInfo.WriteBinlog(c.store.clusterID)
		if wr.Skipped() {
			binInfo.Data.PrewriteValue = nil
			binloginfo.AddOneSkippedCommitter()
		}
		logutil.Eventf(ctx, "finish prewrite binlog")
		ch <- wr
	}()
	return ch
}

func (c *twoPhaseCommitter) writeFinishBinlog(ctx context.Context, tp binlog.BinlogType, commitTS int64) {
	if !c.shouldWriteBinlog() {
		return
	}
	binInfo := c.txn.us.GetOption(kv.BinlogInfo).(*binloginfo.BinlogInfo)
	binInfo.Data.Tp = tp
	binInfo.Data.CommitTs = commitTS
	binInfo.Data.PrewriteValue = nil

	wg := sync.WaitGroup{}
	mock := false
	failpoint.Inject("mockSyncBinlogCommit", func(val failpoint.Value) {
		if val.(bool) {
			wg.Add(1)
			mock = true
		}
	})
	go func() {
		logutil.Eventf(ctx, "start write finish binlog")
		binlogWriteResult := binInfo.WriteBinlog(c.store.clusterID)
		err := binlogWriteResult.GetError()
		if err != nil {
			logutil.BgLogger().Error("failed to write binlog",
				zap.Error(err))
		}
		logutil.Eventf(ctx, "finish write finish binlog")
		if mock {
			wg.Done()
		}
	}()
	if mock {
		wg.Wait()
	}
}

func (c *twoPhaseCommitter) shouldWriteBinlog() bool {
	return c.txn.us.GetOption(kv.BinlogInfo) != nil
}

// TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
// Key+Value size below 16KB.
const txnCommitBatchSize = 16 * 1024

type batchMutations struct {
	region    RegionVerID
	mutations CommitterMutations
	isPrimary bool
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
	default:
		return pb.CommandPri_Normal
	}
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

func (c *twoPhaseCommitter) prepare(ctx context.Context) error {
	txn := c.txn
	c.isPessimistic = txn.IsPessimistic()
	bo := NewBackofferWithVars(ctx, PrewriteMaxBackoff, c.txn.vars)

	// Sanity check for startTS.
	if txn.StartTS() == math.MaxUint64 {
		err := errors.Errorf("try to commit with invalid txnStartTS: %d", txn.StartTS())
		logutil.BgLogger().Error("commit failed",
			zap.Uint64("conn", c.connID),
			zap.Error(err))
		return errors.Trace(err)
	}

	var (
		firstKey      []byte
		regionSizeCal regionTxnSizeCalculator
		txnDetailsCal txnDetailsCalculator
		preSplitCal   preSplitCalculator
		it            = c.mapWithRegion(bo, committerTxnMutations{c, true}.Iter(nil, nil))
	)
	for {
		m, err := it.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if m.key == nil {
			break
		}
		if firstKey == nil {
			firstKey = m.key
		}

		if len(c.primaryKey) == 0 && m.op != pb.Op_CheckNotExists {
			c.primaryKey = m.key
		}

		regionSizeCal.Process(m)
		txnDetailsCal.Process(m)
		preSplitCal.Process(m)
	}
	if len(c.primaryKey) == 0 {
		c.primaryKey = firstKey
	}

	if err := txnDetailsCal.Finish(c); err != nil {
		return errors.Trace(err)
	}
	regionSizeCal.Finish(c)
	splitKeys, splitRegions := preSplitCal.Finish()

	if c.txnSize == 0 {
		return nil
	}

	if c.trySplitRegions(splitKeys, splitRegions) {
		if err := c.reCalRegionTxnSize(bo); err != nil {
			return err
		}
	}

	mutationsIt := it.src.(*txnMutationsIter)
	c.prewriteOnlyKeys = mutationsIt.prewriteOnlyKeys
	c.ignoredKeys = mutationsIt.ignoredKeys
	c.lockTTL = txnLockTTL(txn.startTime, txn.Size())
	c.priority = getTxnPriority(txn)
	c.syncLog = getTxnSyncLog(txn)
	return nil
}
