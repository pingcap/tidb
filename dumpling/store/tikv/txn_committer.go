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

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/terror"
)

type txnCommitter struct {
	store       *tikvStore
	txn         *tikvTxn
	startTS     uint64
	keys        [][]byte
	mutations   map[string]*pb.Mutation
	commitTS    uint64
	mu          sync.RWMutex
	writtenKeys [][]byte
	committed   bool
	wg          sync.WaitGroup
}

func newTxnCommitter(txn *tikvTxn) (*txnCommitter, error) {
	var keys [][]byte
	mutations := make(map[string]*pb.Mutation)
	err := txn.us.WalkBuffer(func(k kv.Key, v []byte) error {
		if len(v) > 0 {
			mutations[string(k)] = &pb.Mutation{
				Op:    pb.Op_Put.Enum(),
				Key:   k,
				Value: v,
			}
		} else {
			mutations[string(k)] = &pb.Mutation{
				Op:  pb.Op_Del.Enum(),
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
				Op:  pb.Op_Lock.Enum(),
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
func (c *txnCommitter) iterKeys(keys [][]byte, f func(batchKeys) error, sizeFn func([]byte) int, asyncNonPrimary bool) error {
	if len(keys) == 0 {
		return nil
	}
	groups, firstRegion, err := c.store.regionCache.GroupKeysByRegion(keys)
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
		err = c.doBatches(batches[:1], f)
		if err != nil {
			return errors.Trace(err)
		}
		batches = batches[1:]
	}
	if asyncNonPrimary {
		c.wg.Add(1)
		go func() {
			c.doBatches(batches, f)
			c.wg.Done()
		}()
		return nil
	}
	err = c.doBatches(batches, f)
	return errors.Trace(err)
}

// doBatches applies f to batches parallelly.
func (c *txnCommitter) doBatches(batches []batchKeys, f func(batchKeys) error) error {
	if len(batches) == 0 {
		return nil
	}
	if len(batches) == 1 {
		e := f(batches[0])
		if e != nil {
			log.Warnf("txnCommitter doBatches failed: %v, tid: %d", e, c.startTS)
		}
		return errors.Trace(e)
	}

	// TODO: For prewrite, stop sending other requests after receiving first error.
	ch := make(chan error)
	for _, batch := range batches {
		go func(batch batchKeys) {
			ch <- f(batch)
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

func (c *txnCommitter) prewriteSingleRegion(batch batchKeys) error {
	mutations := make([]*pb.Mutation, len(batch.keys))
	for i, k := range batch.keys {
		mutations[i] = c.mutations[string(k)]
	}
	req := &pb.Request{
		Type: pb.MessageType_CmdPrewrite.Enum(),
		CmdPrewriteReq: &pb.CmdPrewriteRequest{
			Mutations:    mutations,
			PrimaryLock:  c.primary(),
			StartVersion: proto.Uint64(c.startTS),
		},
	}

	var backoffErr error
	for backoff := txnLockBackoff(); backoffErr == nil; backoffErr = backoff() {
		resp, err := c.store.SendKVReq(req, batch.region)
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			// re-split keys and prewrite again.
			// TODO: The recursive maybe not able to exit if TiKV &
			// PD are implemented incorrectly. A possible fix is
			// introducing a 'max backoff time'.
			err = c.prewriteKeys(batch.keys)
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
			c.writtenKeys = append(c.writtenKeys, batch.keys...)
			return nil
		}
		for _, keyErr := range keyErrs {
			lockInfo, err := extractLockInfoFromKeyErr(keyErr)
			if err != nil {
				// It could be `Retryable` or `Abort`.
				return errors.Trace(err)
			}
			lock := newLock(c.store, lockInfo.GetPrimaryLock(), lockInfo.GetLockVersion(), lockInfo.GetKey(), c.startTS)
			_, err = lock.cleanup()
			if err != nil && terror.ErrorNotEqual(err, errInnerRetryable) {
				return errors.Trace(err)
			}
		}
	}
	return errors.Annotate(backoffErr, txnRetryableMark)
}

func (c *txnCommitter) commitSingleRegion(batch batchKeys) error {
	req := &pb.Request{
		Type: pb.MessageType_CmdCommit.Enum(),
		CmdCommitReq: &pb.CmdCommitRequest{
			StartVersion:  proto.Uint64(c.startTS),
			Keys:          batch.keys,
			CommitVersion: proto.Uint64(c.commitTS),
		},
	}

	resp, err := c.store.SendKVReq(req, batch.region)
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr := resp.GetRegionError(); regionErr != nil {
		// re-split keys and commit again.
		err = c.commitKeys(batch.keys)
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
		if c.committed {
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
	c.committed = true
	return nil
}

func (c *txnCommitter) cleanupSingleRegion(batch batchKeys) error {
	req := &pb.Request{
		Type: pb.MessageType_CmdBatchRollback.Enum(),
		CmdBatchRollbackReq: &pb.CmdBatchRollbackRequest{
			Keys:         batch.keys,
			StartVersion: proto.Uint64(c.startTS),
		},
	}
	resp, err := c.store.SendKVReq(req, batch.region)
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr := resp.GetRegionError(); regionErr != nil {
		err = c.cleanupKeys(batch.keys)
		return errors.Trace(err)
	}
	if keyErr := resp.GetCmdBatchRollbackResp().GetError(); keyErr != nil {
		err = errors.Errorf("cleanup failed: %s", keyErr)
		log.Errorf("txn failed cleanup key: %v, tid: %d", err, c.startTS)
		return errors.Trace(err)
	}
	return nil
}

func (c *txnCommitter) prewriteKeys(keys [][]byte) error {
	return c.iterKeys(keys, c.prewriteSingleRegion, c.keyValueSize, false)
}

func (c *txnCommitter) commitKeys(keys [][]byte) error {
	return c.iterKeys(keys, c.commitSingleRegion, c.keySize, true)
}

func (c *txnCommitter) cleanupKeys(keys [][]byte) error {
	return c.iterKeys(keys, c.cleanupSingleRegion, c.keySize, false)
}

func (c *txnCommitter) Commit() error {
	c.wg.Add(1)
	defer c.wg.Done()

	// Close the txn after no goroutine uses it.
	go func() {
		c.wg.Wait()
		c.txn.close()
		log.Debugf("txn closed, tid: %d", c.startTS)
	}()

	err := c.prewriteKeys(c.keys)
	if err != nil {
		log.Warnf("txn commit failed on prewrite: %v, tid: %d", err, c.startTS)
		c.wg.Add(1)
		go func() {
			c.cleanupKeys(c.writtenKeys)
			c.wg.Done()
		}()
		return errors.Trace(err)
	}

	commitTS, err := c.store.getTimestampWithRetry()
	if err != nil {
		return errors.Trace(err)
	}
	c.commitTS = commitTS

	err = c.commitKeys(c.keys)
	if err != nil {
		if !c.committed {
			c.wg.Add(1)
			go func() {
				c.cleanupKeys(c.writtenKeys)
				c.wg.Done()
			}()
			return errors.Trace(err)
		}
		log.Warnf("txn commit succeed with error: %v, tid: %d", err, c.startTS)
	}
	return nil
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
