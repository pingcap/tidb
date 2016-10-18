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
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tipb/go-binlog"
)

type txnCommitter struct {
	store     *tikvStore
	txn       *tikvTxn
	startTS   uint64
	keys      [][]byte
	mutations map[string]*pb.Mutation
	commitTS  uint64
	mu        struct {
		sync.RWMutex
		writtenKeys [][]byte
		committed   bool
	}
}

func newTxnCommitter(txn *tikvTxn) (*txnCommitter, error) {
	var keys [][]byte
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
		}
	}
	return &txnCommitter{
		store:     txn.store,
		txn:       txn,
		startTS:   txn.StartTS(),
		keys:      keys,
		mutations: mutations,
	}, nil
}

func (c *txnCommitter) primary() []byte {
	return c.keys[0]
}

// iterKeys groups keys into batches, then applies `f` to them. If the flag
// asyncNonPrimary is set, it will return as soon as the primary batch is
// processed.
func (c *txnCommitter) iterKeys(bo *Backoffer, keys [][]byte, f func(*Backoffer, batchKeys) error, sizeFn func([]byte) int, asyncNonPrimary bool) error {
	if len(keys) == 0 {
		return nil
	}
	groups, firstRegion, err := c.store.regionCache.GroupKeysByRegion(bo, keys)
	if err != nil {
		return errors.Trace(err)
	}
	firstIsPrimary := bytes.Equal(keys[0], c.primary())

	var batches []batchKeys
	// Make sure the group that contains primary key goes first.
	if firstIsPrimary {
		batches = appendBatchBySize(batches, firstRegion, groups[firstRegion], sizeFn, txnCommitBatchSize)
		delete(groups, firstRegion)
	}
	for id, g := range groups {
		batches = appendBatchBySize(batches, id, g, sizeFn, txnCommitBatchSize)
	}

	if firstIsPrimary {
		err = c.doBatches(bo, batches[:1], f)
		if err != nil {
			return errors.Trace(err)
		}
		batches = batches[1:]
	}
	if asyncNonPrimary {
		go func() {
			e := c.doBatches(bo, batches, f)
			if e != nil {
				log.Warnf("txnCommitter async doBatches err: %v", e)
			}
		}()
		return nil
	}
	err = c.doBatches(bo, batches, f)
	return errors.Trace(err)
}

// doBatches applies f to batches parallelly.
func (c *txnCommitter) doBatches(bo *Backoffer, batches []batchKeys, f func(*Backoffer, batchKeys) error) error {
	if len(batches) == 0 {
		return nil
	}
	if len(batches) == 1 {
		e := f(bo, batches[0])
		if e != nil {
			log.Warnf("txnCommitter doBatches failed: %v, tid: %d", e, c.startTS)
		}
		return errors.Trace(e)
	}

	// TODO: For prewrite, stop sending other requests after receiving first error.
	ch := make(chan error)
	for _, batch := range batches {
		go func(batch batchKeys) {
			ch <- f(bo.Fork(), batch)
		}(batch)
	}
	var err error
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			log.Warnf("txnCommitter doBatches failed: %v, tid: %d", e, c.startTS)
			err = e
		}
	}
	return errors.Trace(err)
}

func (c *txnCommitter) keyValueSize(key []byte) int {
	size := c.keySize(key)
	if mutation := c.mutations[string(key)]; mutation != nil {
		size += len(mutation.Value)
	}
	return size
}

func (c *txnCommitter) keySize(key []byte) int {
	return len(key)
}

func (c *txnCommitter) prewriteSingleRegion(bo *Backoffer, batch batchKeys) error {
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
			log.Debugf("prewrite encounters lock: %v", lock)
			locks = append(locks, lock)
		}
		ok, err := c.store.lockResolver.ResolveLocks(bo, locks)
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			err = bo.Backoff(boTxnLock, errors.Errorf("prewrite lockedKeys: %d", len(locks)))
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (c *txnCommitter) commitSingleRegion(bo *Backoffer, batch batchKeys) error {
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
		err = errors.Errorf("commit failed: %v", keyErr.String())
		if c.mu.committed {
			// No secondary key could be rolled back after it's primary key is committed.
			// There must be a serious bug somewhere.
			log.Errorf("txn failed commit key after primary key committed: %v, tid: %d", err, c.startTS)
			return errors.Trace(err)
		}
		// The transaction maybe rolled back by concurrent transactions.
		log.Warnf("txn failed commit primary key: %v, retry later, tid: %d", err, c.startTS)
		return errors.Annotate(err, txnRetryableMark)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Group that contains primary key is always the first.
	// We mark transaction's status committed when we receive the first success response.
	c.mu.committed = true
	return nil
}

func (c *txnCommitter) cleanupSingleRegion(bo *Backoffer, batch batchKeys) error {
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
		err = errors.Errorf("cleanup failed: %s", keyErr)
		log.Errorf("txn failed cleanup key: %v, tid: %d", err, c.startTS)
		return errors.Trace(err)
	}
	return nil
}

func (c *txnCommitter) prewriteKeys(bo *Backoffer, keys [][]byte) error {
	return c.iterKeys(bo, keys, c.prewriteSingleRegion, c.keyValueSize, false)
}

func (c *txnCommitter) commitKeys(bo *Backoffer, keys [][]byte) error {
	return c.iterKeys(bo, keys, c.commitSingleRegion, c.keySize, true)
}

func (c *txnCommitter) cleanupKeys(bo *Backoffer, keys [][]byte) error {
	return c.iterKeys(bo, keys, c.cleanupSingleRegion, c.keySize, false)
}

// The max time a Txn may use (in ms) from its startTS to commitTS.
// We use it to guarantee GC worker will not influence any active txn. The value
// should be less than `gcRunInterval`.
const maxTxnTimeUse = 590000

func (c *txnCommitter) Commit() error {
	defer func() {
		// Always clean up all written keys if the txn does not commit.
		c.mu.RLock()
		writtenKeys := c.mu.writtenKeys
		committed := c.mu.committed
		c.mu.RUnlock()
		if !committed {
			go func() {
				err := c.cleanupKeys(NewBackoffer(cleanupMaxBackoff), writtenKeys)
				if err != nil {
					log.Infof("txn cleanup err: %v, tid: %d", err, c.startTS)
				} else {
					log.Infof("txn clean up done, tid: %d", c.startTS)
				}
			}()
		}
	}()

	binlogChan := c.prewriteBinlog()
	err := c.prewriteKeys(NewBackoffer(prewriteMaxBackoff), c.keys)
	if binlogChan != nil {
		binlogErr := <-binlogChan
		if binlogErr != nil {
			return errors.Trace(binlogErr)
		}
	}
	if err != nil {
		log.Warnf("txn commit failed on prewrite: %v, tid: %d", err, c.startTS)
		return errors.Trace(err)
	}

	commitTS, err := c.store.getTimestampWithRetry(NewBackoffer(tsoMaxBackoff))
	if err != nil {
		log.Warnf("txn get commitTS failed: %v, tid: %d", err, c.startTS)
		return errors.Trace(err)
	}
	c.commitTS = commitTS

	if c.store.oracle.IsExpired(c.startTS, maxTxnTimeUse) {
		err = errors.Errorf("txn takes too much time, start: %d, commit: %d", c.startTS, c.commitTS)
		return errors.Annotate(err, txnRetryableMark)
	}

	err = c.commitKeys(NewBackoffer(commitMaxBackoff), c.keys)
	if err != nil {
		if !c.mu.committed {
			log.Warnf("txn commit failed on commit: %v, tid: %d", err, c.startTS)
			return errors.Trace(err)
		}
		log.Warnf("txn commit succeed with error: %v, tid: %d", err, c.startTS)
	}
	return nil
}

func (c *txnCommitter) prewriteBinlog() chan error {
	if !c.shouldWriteBinlog() {
		return nil
	}
	ch := make(chan error, 1)
	go func() {
		prewriteValue := c.txn.us.GetOption(kv.BinlogData)
		binPrewrite := &binlog.Binlog{
			Tp:            binlog.BinlogType_Prewrite,
			StartTs:       int64(c.startTS),
			PrewriteKey:   c.keys[0],
			PrewriteValue: prewriteValue.([]byte),
		}
		err := binloginfo.WriteBinlog(binPrewrite)
		ch <- errors.Trace(err)
	}()
	return ch
}

func (c *txnCommitter) writeFinisheBinlog(tp binlog.BinlogType, commitTS int64) {
	if !c.shouldWriteBinlog() {
		return
	}
	go func() {
		binCommit := &binlog.Binlog{
			Tp:          tp,
			StartTs:     int64(c.startTS),
			CommitTs:    commitTS,
			PrewriteKey: c.keys[0],
		}
		err := binloginfo.WriteBinlog(binCommit)
		if err != nil {
			log.Errorf("failed to write binlog: %v", err)
		}
	}()
}

func (c *txnCommitter) shouldWriteBinlog() bool {
	if binloginfo.PumpClient == nil {
		return false
	}
	prewriteValue := c.txn.us.GetOption(kv.BinlogData)
	return prewriteValue != nil
}

// TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
// Key+Value size below 512KB.
const txnCommitBatchSize = 512 * 1024

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
