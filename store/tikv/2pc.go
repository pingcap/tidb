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

	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

type twoPhaseCommitAction int

const (
	actionPrewrite twoPhaseCommitAction = 1
	actionCommit   twoPhaseCommitAction = 2
	actionCleanup  twoPhaseCommitAction = 3
)

func (ca twoPhaseCommitAction) String() string {
	switch ca {
	case actionPrewrite:
		return "prewrite"
	case actionCommit:
		return "commit"
	case actionCleanup:
		return "cleanup"
	}
	return "unknown"
}

// MetricsTag returns detail tag for metrics.
func (ca twoPhaseCommitAction) MetricsTag() string {
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
	mu        struct {
		sync.RWMutex
		committed       bool
		undeterminedErr error // undeterminedErr saves the rpc error we encounter when commit primary key.
	}
	priority pb.CommandPri
	syncLog  bool
	connID   uint64 // connID is used for log.
	cleanWg  sync.WaitGroup
	// maxTxnTimeUse represents max time a Txn may use (in ms) from its startTS to commitTS.
	// We use it to guarantee GC worker will not influence any active txn. The value
	// should be less than GC life time.
	maxTxnTimeUse uint64
	detail        *execdetails.CommitDetails
}

type mutationEx struct {
	pb.Mutation
	asserted bool
}

// newTwoPhaseCommitter creates a twoPhaseCommitter.
func newTwoPhaseCommitter(txn *tikvTxn, connID uint64) (*twoPhaseCommitter, error) {
	var (
		keys    [][]byte
		size    int
		putCnt  int
		delCnt  int
		lockCnt int
	)
	mutations := make(map[string]*mutationEx)
	err := txn.us.WalkBuffer(func(k kv.Key, v []byte) error {
		if len(v) > 0 {
			op := pb.Op_Put
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
			mutations[string(k)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:  pb.Op_Del,
					Key: k,
				},
			}
			delCnt++
		}
		keys = append(keys, k)
		entrySize := len(k) + len(v)
		if entrySize > kv.TxnEntrySizeLimit {
			return kv.ErrEntryTooLarge.GenWithStackByArgs(kv.TxnEntrySizeLimit, entrySize)
		}
		size += entrySize
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, lockKey := range txn.lockKeys {
		if _, ok := mutations[string(lockKey)]; !ok {
			mutations[string(lockKey)] = &mutationEx{
				Mutation: pb.Mutation{
					Op:  pb.Op_Lock,
					Key: lockKey,
				},
			}
			lockCnt++
			keys = append(keys, lockKey)
			size += len(lockKey)
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}

	for _, pair := range txn.assertions {
		mutation, ok := mutations[string(pair.key)]
		if !ok {
			logutil.Logger(context.Background()).Error("ASSERTION FAIL!!! assertion exists but no mutation?",
				zap.Stringer("assertion", pair))
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
	if len(keys) > int(entrylimit) || size > kv.TxnTotalSizeLimit {
		return nil, kv.ErrTxnTooLarge
	}
	const logEntryCount = 10000
	const logSize = 4 * 1024 * 1024 // 4MB
	if len(keys) > logEntryCount || size > logSize {
		tableID := tablecodec.DecodeTableID(keys[0])
		logutil.Logger(context.Background()).Info("[BIG_TXN]",
			zap.Uint64("con", connID),
			zap.Int64("table ID", tableID),
			zap.Int("size", size),
			zap.Int("keys", len(keys)),
			zap.Int("puts", putCnt),
			zap.Int("dels", delCnt),
			zap.Int("locks", lockCnt),
			zap.Uint64("txnStartTS", txn.startTS))
	}

	// Convert from sec to ms
	maxTxnTimeUse := uint64(config.GetGlobalConfig().TiKVClient.MaxTxnTimeUse) * 1000

	// Sanity check for startTS.
	if txn.StartTS() == math.MaxUint64 {
		err = errors.Errorf("try to commit with invalid txnStartTS: %d", txn.StartTS())
		logutil.Logger(context.Background()).Error("commit failed",
			zap.Uint64("conn", connID),
			zap.Error(err))
		return nil, errors.Trace(err)
	}

	commitDetail := &execdetails.CommitDetails{WriteSize: size, WriteKeys: len(keys)}
	metrics.TiKVTxnWriteKVCountHistogram.Observe(float64(commitDetail.WriteKeys))
	metrics.TiKVTxnWriteSizeHistogram.Observe(float64(commitDetail.WriteSize))
	return &twoPhaseCommitter{
		store:         txn.store,
		txn:           txn,
		startTS:       txn.StartTS(),
		keys:          keys,
		mutations:     mutations,
		lockTTL:       txnLockTTL(txn.startTime, size),
		priority:      getTxnPriority(txn),
		syncLog:       getTxnSyncLog(txn),
		connID:        connID,
		maxTxnTimeUse: maxTxnTimeUse,
		detail:        commitDetail,
	}, nil
}

func (c *twoPhaseCommitter) primary() []byte {
	return c.keys[0]
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
	groups, firstRegion, err := c.store.regionCache.GroupKeysByRegion(bo, keys)
	if err != nil {
		return errors.Trace(err)
	}

	metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(action.MetricsTag()).Observe(float64(len(groups)))

	var batches []batchKeys
	var sizeFunc = c.keySize
	if action == actionPrewrite {
		sizeFunc = c.keyValueSize
		atomic.AddInt32(&c.detail.PrewriteRegionNum, int32(len(groups)))
	}
	// Make sure the group that contains primary key goes first.
	batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], sizeFunc, txnCommitBatchSize)
	delete(groups, firstRegion)
	for id, g := range groups {
		batches = appendBatchBySize(batches, id, g, sizeFunc, txnCommitBatchSize)
	}

	firstIsPrimary := bytes.Equal(keys[0], c.primary())
	if firstIsPrimary && (action == actionCommit || action == actionCleanup) {
		// primary should be committed/cleanup first
		err = c.doActionOnBatches(bo, action, batches[:1])
		if err != nil {
			return errors.Trace(err)
		}
		batches = batches[1:]
	}
	if action == actionCommit {
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
				metrics.TiKVSecondaryLockCleanupFailureCounter.WithLabelValues("commit").Inc()
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
	var singleBatchActionFunc func(bo *Backoffer, batch batchKeys) error
	switch action {
	case actionPrewrite:
		singleBatchActionFunc = c.prewriteSingleBatch
	case actionCommit:
		singleBatchActionFunc = c.commitSingleBatch
	case actionCleanup:
		singleBatchActionFunc = c.cleanupSingleBatch
	}
	if len(batches) == 1 {
		e := singleBatchActionFunc(bo, batches[0])
		if e != nil {
			logutil.Logger(context.Background()).Debug("2PC doActionOnBatches failed",
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
	if action == actionPrewrite {
		backoffer, cancel = bo.Fork()
		defer cancel()
	}

	// Concurrently do the work for each batch.
	ch := make(chan error, len(batches))
	for _, batch1 := range batches {

		batch := batch1
		go func() {
			if action == actionCommit {
				// Because the secondary batches of the commit actions are implemented to be
				// committed asynchronously in background goroutines, we should not
				// fork a child context and call cancel() while the foreground goroutine exits.
				// Otherwise the background goroutines will be canceled execeptionally.
				// Here we makes a new clone of the original backoffer for this goroutine
				// exclusively to avoid the data race when using the same backoffer
				// in concurrent goroutines.
				singleBatchBackoffer := backoffer.Clone()
				ch <- singleBatchActionFunc(singleBatchBackoffer, batch)
			} else {
				singleBatchBackoffer, singleBatchCancel := backoffer.Fork()
				defer singleBatchCancel()
				ch <- singleBatchActionFunc(singleBatchBackoffer, batch)
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

func (c *twoPhaseCommitter) prewriteSingleBatch(bo *Backoffer, batch batchKeys) error {
	mutations := make([]*pb.Mutation, len(batch.keys))
	for i, k := range batch.keys {
		tmp := c.mutations[string(k)]
		mutations[i] = &tmp.Mutation
	}

	req := &tikvrpc.Request{
		Type: tikvrpc.CmdPrewrite,
		Prewrite: &pb.PrewriteRequest{
			Mutations:    mutations,
			PrimaryLock:  c.primary(),
			StartVersion: c.startTS,
			LockTtl:      c.lockTTL,
		},
		Context: pb.Context{
			Priority: c.priority,
			SyncLog:  c.syncLog,
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
					panic(fmt.Sprintf("conn%d, conditionPair for key:%s should not be nil", c.connID, key))
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
		ok, err := c.store.lockResolver.ResolveLocks(bo, locks)
		if err != nil {
			return errors.Trace(err)
		}
		atomic.AddInt64(&c.detail.ResolveLockTime, int64(time.Since(start)))
		if !ok {
			err = bo.Backoff(BoTxnLock, errors.Errorf("2PC prewrite lockedKeys: %d", len(locks)))
			if err != nil {
				return errors.Trace(err)
			}
		}
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

func (c *twoPhaseCommitter) commitSingleBatch(bo *Backoffer, batch batchKeys) error {
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
	req.Context.Priority = c.priority

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
		err = errors.Errorf("conn%d 2PC commit failed: %v", c.connID, keyErr.String())
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
		return errors.Annotate(err, txnRetryableMark)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Group that contains primary key is always the first.
	// We mark transaction's status committed when we receive the first success response.
	c.mu.committed = true
	return nil
}

func (c *twoPhaseCommitter) cleanupSingleBatch(bo *Backoffer, batch batchKeys) error {
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
	return c.doActionOnKeys(bo, actionPrewrite, keys)
}

func (c *twoPhaseCommitter) commitKeys(bo *Backoffer, keys [][]byte) error {
	return c.doActionOnKeys(bo, actionCommit, keys)
}

func (c *twoPhaseCommitter) cleanupKeys(bo *Backoffer, keys [][]byte) error {
	return c.doActionOnKeys(bo, actionCleanup, keys)
}

func (c *twoPhaseCommitter) executeAndWriteFinishBinlog(ctx context.Context) error {
	err := c.execute(ctx)
	if err != nil {
		c.writeFinishBinlog(binlog.BinlogType_Rollback, 0)
	} else {
		c.txn.commitTS = c.commitTS
		c.writeFinishBinlog(binlog.BinlogType_Commit, int64(c.commitTS))
	}
	return errors.Trace(err)
}

// execute executes the two-phase commit protocol.
func (c *twoPhaseCommitter) execute(ctx context.Context) error {
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
					metrics.TiKVSecondaryLockCleanupFailureCounter.WithLabelValues("rollback").Inc()
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
	}()

	binlogChan := c.prewriteBinlog()
	prewriteBo := NewBackoffer(ctx, prewriteMaxBackoff).WithVars(c.txn.vars)
	start := time.Now()
	err := c.prewriteKeys(prewriteBo, c.keys)
	c.detail.PrewriteTime = time.Since(start)
	c.detail.TotalBackoffTime += time.Duration(prewriteBo.totalSleep) * time.Millisecond
	if binlogChan != nil {
		binlogErr := <-binlogChan
		if binlogErr != nil {
			return errors.Trace(binlogErr)
		}
	}
	if err != nil {
		logutil.Logger(ctx).Debug("2PC failed on prewrite",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}

	start = time.Now()
	commitTS, err := c.store.getTimestampWithRetry(NewBackoffer(ctx, tsoMaxBackoff).WithVars(c.txn.vars))
	if err != nil {
		logutil.Logger(ctx).Warn("2PC get commitTS failed",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return errors.Trace(err)
	}
	c.detail.GetCommitTsTime = time.Since(start)

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

	// gofail: var tmpMaxTxnTime uint64
	// if tmpMaxTxnTime > 0 {
	//  c.maxTxnTimeUse = tmpMaxTxnTime
	// }

	if c.store.oracle.IsExpired(c.startTS, c.maxTxnTimeUse) {
		err = errors.Errorf("conn%d txn takes too much time, txnStartTS: %d, comm: %d",
			c.connID, c.startTS, c.commitTS)
		return errors.Annotate(err, txnRetryableMark)
	}

	start = time.Now()
	commitBo := NewBackoffer(ctx, CommitMaxBackoff).WithVars(c.txn.vars)
	err = c.commitKeys(commitBo, c.keys)
	c.detail.CommitTime = time.Since(start)
	c.detail.TotalBackoffTime += time.Duration(commitBo.totalSleep) * time.Millisecond
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

func (c *twoPhaseCommitter) prewriteBinlog() chan error {
	if !c.shouldWriteBinlog() {
		return nil
	}
	ch := make(chan error, 1)
	go func() {
		binInfo := c.txn.us.GetOption(kv.BinlogInfo).(*binloginfo.BinlogInfo)
		bin := binInfo.Data
		bin.StartTs = int64(c.startTS)
		if bin.Tp == binlog.BinlogType_Prewrite {
			bin.PrewriteKey = c.keys[0]
		}
		err := binInfo.WriteBinlog(c.store.clusterID)
		ch <- errors.Trace(err)
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
		err := binInfo.WriteBinlog(c.store.clusterID)
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
