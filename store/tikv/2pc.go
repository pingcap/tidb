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
	"math"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tipb/go-binlog"
	"golang.org/x/net/context"
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

// twoPhaseCommitter executes a two-phase commit protocol.
type twoPhaseCommitter struct {
	store     *tikvStore
	txn       *tikvTxn
	startTS   uint64
	keys      [][]byte
	mutations map[string]*pb.Mutation
	lockTTL   uint64
	commitTS  uint64
	mu        struct {
		sync.RWMutex
		writtenKeys [][]byte
		committed   bool
	}
}

// newTwoPhaseCommitter creates a twoPhaseCommitter.
func newTwoPhaseCommitter(txn *tikvTxn) (*twoPhaseCommitter, error) {
	var keys [][]byte
	var size int
	mutations := make(map[string]*pb.Mutation)
	err := txn.us.WalkBuffer(func(k kv.Key, v []byte) error {
		if len(v) > 0 {
			mutations[string(k)] = &pb.Mutation{
				Op:    pb.Op_Put,
				Key:   k,
				Value: v,
			}
		} else {
			mutations[string(k)] = &pb.Mutation{
				Op:  pb.Op_Del,
				Key: k,
			}
		}
		keys = append(keys, k)
		size += len(k) + len(v)
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Transactions without Put/Del, only Locks are readonly.
	// We can skip commit directly.
	if len(keys) == 0 {
		return nil, nil
	}
	for _, lockKey := range txn.lockKeys {
		if _, ok := mutations[string(lockKey)]; !ok {
			mutations[string(lockKey)] = &pb.Mutation{
				Op:  pb.Op_Lock,
				Key: lockKey,
			}
			keys = append(keys, lockKey)
			size += len(lockKey)
		}
	}
	txnWriteKVCountHistogram.Observe(float64(len(keys)))
	txnWriteSizeHistogram.Observe(float64(size / 1024))

	// Increase lockTTL for large transactions.
	// The formula is `ttl = ttlFactor * sqrt(sizeInMiB)`.
	// When writeSize <= 256K, ttl is defaultTTL (3s);
	// When writeSize is 1MiB, 100MiB, or 400MiB, ttl is 6s, 60s, 120s correspondingly;
	// When writeSize >= 400MiB, ttl is maxTTL (120s).
	var lockTTL uint64
	if size > txnCommitBatchSize {
		sizeMiB := float64(size) / 1024 / 1024
		lockTTL = uint64(float64(ttlFactor) * math.Sqrt(float64(sizeMiB)))
		if lockTTL < defaultLockTTL {
			lockTTL = defaultLockTTL
		}
		if lockTTL > maxLockTTL {
			lockTTL = maxLockTTL
		}
	}

	return &twoPhaseCommitter{
		store:     txn.store,
		txn:       txn,
		startTS:   txn.StartTS(),
		keys:      keys,
		mutations: mutations,
		lockTTL:   lockTTL,
	}, nil
}

func (c *twoPhaseCommitter) primary() []byte {
	return c.keys[0]
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

	var batches []batchKeys
	var sizeFunc = c.keySize
	if action == actionPrewrite {
		sizeFunc = c.keyValueSize
	}
	// Make sure the group that contains primary key goes first.
	batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], sizeFunc, txnCommitBatchSize)
	delete(groups, firstRegion)
	for id, g := range groups {
		batches = appendBatchBySize(batches, id, g, sizeFunc, txnCommitBatchSize)
	}

	firstIsPrimary := bytes.Equal(keys[0], c.primary())
	if firstIsPrimary {
		err = c.doActionOnBatches(bo, action, batches[:1])
		if err != nil {
			return errors.Trace(err)
		}
		batches = batches[1:]
	}
	if action == actionCommit {
		// Commit secondary batches in background goroutine to reduce latency.
		go func() {
			e := c.doActionOnBatches(bo, action, batches)
			if e != nil {
				log.Warnf("2PC async doActionOnBatches %s err: %v", action, e)
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
			log.Warnf("2PC doActionOnBatches %s failed: %v, tid: %d", action, e, c.startTS)
		}
		return errors.Trace(e)
	}

	// For prewrite, stop sending other requests after receiving first error.
	var cancel context.CancelFunc
	if action == actionPrewrite {
		cancel = bo.WithCancel()
	}

	// Concurrently do the work for each batch.
	ch := make(chan error, len(batches))
	for _, batch := range batches {
		go func(batch batchKeys) {
			ch <- singleBatchActionFunc(bo.Fork(), batch)
		}(batch)
	}
	var err error
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			log.Warnf("2PC doActionOnBatches %s failed: %v, tid: %d", action, e, c.startTS)
			if cancel != nil {
				cancel()
			}
			err = e
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
		mutations[i] = c.mutations[string(k)]
	}
	req := &pb.Request{
		Type: pb.MessageType_CmdPrewrite,
		CmdPrewriteReq: &pb.CmdPrewriteRequest{
			Mutations:    mutations,
			PrimaryLock:  c.primary(),
			StartVersion: c.startTS,
			LockTtl:      c.lockTTL,
		},
	}

	for {
		resp, err := c.store.SendKVReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = c.prewriteKeys(bo, batch.keys)
			return errors.Trace(err)
		}
		prewriteResp := resp.GetCmdPrewriteResp()
		if prewriteResp == nil {
			return errors.Trace(errBodyMissing)
		}
		keyErrs := prewriteResp.GetErrors()
		if len(keyErrs) == 0 {
			// We need to cleanup all written keys if transaction aborts.
			c.mu.Lock()
			defer c.mu.Unlock()
			c.mu.writtenKeys = append(c.mu.writtenKeys, batch.keys...)
			return nil
		}
		var locks []*Lock
		for _, keyErr := range keyErrs {
			lock, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				return errors.Trace(err1)
			}
			log.Debugf("2PC prewrite encounters lock: %v", lock)
			locks = append(locks, lock)
		}
		ok, err := c.store.lockResolver.ResolveLocks(bo, locks)
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			err = bo.Backoff(boTxnLock, errors.Errorf("2PC prewrite lockedKeys: %d", len(locks)))
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (c *twoPhaseCommitter) commitSingleBatch(bo *Backoffer, batch batchKeys) error {
	req := &pb.Request{
		Type: pb.MessageType_CmdCommit,
		CmdCommitReq: &pb.CmdCommitRequest{
			StartVersion:  c.startTS,
			Keys:          batch.keys,
			CommitVersion: c.commitTS,
		},
	}

	resp, err := c.store.SendKVReq(bo, req, batch.region, readTimeoutShort)
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr := resp.GetRegionError(); regionErr != nil {
		err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		// re-split keys and commit again.
		err = c.commitKeys(bo, batch.keys)
		return errors.Trace(err)
	}
	commitResp := resp.GetCmdCommitResp()
	if commitResp == nil {
		return errors.Trace(errBodyMissing)
	}
	if keyErr := commitResp.GetError(); keyErr != nil {
		c.mu.RLock()
		defer c.mu.RUnlock()
		err = errors.Errorf("2PC commit failed: %v", keyErr.String())
		if c.mu.committed {
			// No secondary key could be rolled back after it's primary key is committed.
			// There must be a serious bug somewhere.
			log.Errorf("2PC failed commit key after primary key committed: %v, tid: %d", err, c.startTS)
			return errors.Trace(err)
		}
		// The transaction maybe rolled back by concurrent transactions.
		log.Warnf("2PC failed commit primary key: %v, retry later, tid: %d", err, c.startTS)
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
	req := &pb.Request{
		Type: pb.MessageType_CmdBatchRollback,
		CmdBatchRollbackReq: &pb.CmdBatchRollbackRequest{
			Keys:         batch.keys,
			StartVersion: c.startTS,
		},
	}
	resp, err := c.store.SendKVReq(bo, req, batch.region, readTimeoutShort)
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr := resp.GetRegionError(); regionErr != nil {
		err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		err = c.cleanupKeys(bo, batch.keys)
		return errors.Trace(err)
	}
	if keyErr := resp.GetCmdBatchRollbackResp().GetError(); keyErr != nil {
		err = errors.Errorf("2PC cleanup failed: %s", keyErr)
		log.Errorf("2PC failed cleanup key: %v, tid: %d", err, c.startTS)
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

// The max time a Txn may use (in ms) from its startTS to commitTS.
// We use it to guarantee GC worker will not influence any active txn. The value
// should be less than `gcRunInterval`.
const maxTxnTimeUse = 590000

// execute executes the two-phase commit protocol.
func (c *twoPhaseCommitter) execute() error {
	ctx := context.Background()
	defer func() {
		// Always clean up all written keys if the txn does not commit.
		c.mu.RLock()
		writtenKeys := c.mu.writtenKeys
		committed := c.mu.committed
		c.mu.RUnlock()
		if !committed {
			go func() {
				err := c.cleanupKeys(NewBackoffer(cleanupMaxBackoff, ctx), writtenKeys)
				if err != nil {
					log.Infof("2PC cleanup err: %v, tid: %d", err, c.startTS)
				} else {
					log.Infof("2PC clean up done, tid: %d", c.startTS)
				}
			}()
		}
	}()

	binlogChan := c.prewriteBinlog()
	err := c.prewriteKeys(NewBackoffer(prewriteMaxBackoff, ctx), c.keys)
	if binlogChan != nil {
		binlogErr := <-binlogChan
		if binlogErr != nil {
			return errors.Trace(binlogErr)
		}
	}
	if err != nil {
		log.Warnf("2PC failed on prewrite: %v, tid: %d", err, c.startTS)
		return errors.Trace(err)
	}

	commitTS, err := c.store.getTimestampWithRetry(NewBackoffer(tsoMaxBackoff, ctx))
	if err != nil {
		log.Warnf("2PC get commitTS failed: %v, tid: %d", err, c.startTS)
		return errors.Trace(err)
	}
	c.commitTS = commitTS

	if c.store.oracle.IsExpired(c.startTS, maxTxnTimeUse) {
		err = errors.Errorf("txn takes too much time, start: %d, commit: %d", c.startTS, c.commitTS)
		return errors.Annotate(err, txnRetryableMark)
	}

	err = c.commitKeys(NewBackoffer(commitMaxBackoff, ctx), c.keys)
	if err != nil {
		if !c.mu.committed {
			log.Warnf("2PC failed on commit: %v, tid: %d", err, c.startTS)
			return errors.Trace(err)
		}
		log.Warnf("2PC succeed with error: %v, tid: %d", err, c.startTS)
	}
	return nil
}

func (c *twoPhaseCommitter) prewriteBinlog() chan error {
	if !c.shouldWriteBinlog() {
		return nil
	}
	ch := make(chan error, 1)
	go func() {
		bin := c.txn.us.GetOption(kv.BinlogData).(*binlog.Binlog)
		bin.StartTs = int64(c.startTS)
		if bin.Tp == binlog.BinlogType_Prewrite {
			bin.PrewriteKey = c.keys[0]
		}
		err := binloginfo.WriteBinlog(bin, c.store.clusterID)
		ch <- errors.Trace(err)
	}()
	return ch
}

func (c *twoPhaseCommitter) writeFinishBinlog(tp binlog.BinlogType, commitTS int64) {
	if !c.shouldWriteBinlog() {
		return
	}
	bin := c.txn.us.GetOption(kv.BinlogData).(*binlog.Binlog)
	bin.Tp = tp
	bin.CommitTs = commitTS
	go func() {
		err := binloginfo.WriteBinlog(bin, c.store.clusterID)
		if err != nil {
			log.Errorf("failed to write binlog: %v", err)
		}
	}()
}

func (c *twoPhaseCommitter) shouldWriteBinlog() bool {
	if binloginfo.PumpClient == nil {
		return false
	}
	_, ok := c.txn.us.GetOption(kv.BinlogData).(*binlog.Binlog)
	return ok
}

// TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
// Key+Value size below 32KB.
const txnCommitBatchSize = 32 * 1024

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
