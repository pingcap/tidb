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

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/terror"
)

type txnCommitter struct {
	store       *tikvStore
	startTS     uint64
	keys        [][]byte
	mutations   map[string]*pb.Mutation
	writtenKeys [][]byte
	commitTS    uint64
	committed   bool
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
		startTS:   txn.StartTS(),
		keys:      keys,
		mutations: mutations,
	}, nil
}

func (c *txnCommitter) primary() []byte {
	return c.keys[0]
}

func (c *txnCommitter) iterKeysByRegion(keys [][]byte, f func([][]byte) error) error {
	groups := make(map[uint64][][]byte)
	var primaryRegionID uint64
	for _, k := range keys {
		region, err := c.store.getRegion(k)
		if err != nil {
			return errors.Trace(err)
		}
		id := region.GetID()
		if bytes.Compare(k, c.primary()) == 0 {
			primaryRegionID = id
		}
		groups[id] = append(groups[id], k)
	}

	// Make sure the group that contains primary key goes first.
	if primaryRegionID != 0 {
		if err := f(groups[primaryRegionID]); err != nil {
			return errors.Trace(err)
		}
		delete(groups, primaryRegionID)
	}
	for _, g := range groups {
		if err := f(g); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
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

func (c *txnCommitter) prewriteSingleRegion(keys [][]byte) error {
	mutations := make([]*pb.Mutation, len(keys))
	for i, k := range keys {
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
		region, err := c.store.getRegion(keys[0])
		if err != nil {
			return errors.Trace(err)
		}
		resp, err := c.store.SendKVReq(req, region)
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			// re-split keys and prewrite again.
			// TODO: The recursive maybe not able to exit if TiKV &
			// PD are implemented incorrectly. A possible fix is
			// introducing a 'max backoff time'.
			err = c.prewriteKeys(keys)
			return errors.Trace(err)
		}
		prewriteResp := resp.GetCmdPrewriteResp()
		if prewriteResp == nil {
			return errors.Trace(errBodyMissing)
		}
		keyErrs := prewriteResp.GetErrors()
		if len(keyErrs) == 0 {
			// We need to cleanup all written keys if transaction aborts.
			c.writtenKeys = append(c.writtenKeys, keys...)
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

func (c *txnCommitter) commitSingleRegion(keys [][]byte) error {
	req := &pb.Request{
		Type: pb.MessageType_CmdCommit.Enum(),
		CmdCommitReq: &pb.CmdCommitRequest{
			StartVersion:  proto.Uint64(c.startTS),
			Keys:          keys,
			CommitVersion: proto.Uint64(c.commitTS),
		},
	}

	region, err := c.store.getRegion(keys[0])
	if err != nil {
		return errors.Trace(err)
	}
	resp, err := c.store.SendKVReq(req, region)
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr := resp.GetRegionError(); regionErr != nil {
		// re-split keys and commit again.
		err = c.commitKeys(keys)
		return errors.Trace(err)
	}
	commitResp := resp.GetCmdCommitResp()
	if commitResp == nil {
		return errors.Trace(errBodyMissing)
	}
	keyErrs := commitResp.GetErrors()
	if len(keyErrs) > 0 {
		// TODO: update proto field from repeated to optional. It will never return multiple errors.
		err = errors.Errorf("commit failed: %v", keyErrs[0].String())
		if c.committed {
			// No secondary key could be rolled back after it's primary key is committed.
			// There must be a serious bug somewhere.
			log.Errorf("txn failed commit key after primary key committed: %v", err)
			return errors.Trace(err)
		}
		// The transaction maybe rolled back by concurrent transactions.
		log.Warnf("txn failed commit primary key: %v, retry later", err)
		return errors.Annotate(err, txnRetryableMark)
	}

	// Group that contains primary key is always the first.
	// We mark transaction's status committed when we receive the first success response.
	c.committed = true
	return nil
}

func (c *txnCommitter) cleanupSingleRegion(keys [][]byte) error {
	// TODO: add batch RPC call.
	for _, k := range keys {
		req := &pb.Request{
			Type: pb.MessageType_CmdCleanup.Enum(),
			CmdCleanupReq: &pb.CmdCleanupRequest{
				Key:          k,
				StartVersion: proto.Uint64(c.startTS),
			},
		}
		region, err := c.store.getRegion(k)
		if err != nil {
			return errors.Trace(err)
		}
		c.store.SendKVReq(req, region)
		// TODO: check response and retry
	}
	return nil
}

func (c *txnCommitter) prewriteKeys(keys [][]byte) error {
	return c.iterKeysByRegion(keys, batchIterFn(c.prewriteSingleRegion, c.keyValueSize))
}

func (c *txnCommitter) commitKeys(keys [][]byte) error {
	return c.iterKeysByRegion(keys, batchIterFn(c.commitSingleRegion, c.keySize))
}

func (c *txnCommitter) cleanupKeys(keys [][]byte) error {
	return c.iterKeysByRegion(keys, batchIterFn(c.cleanupSingleRegion, c.keySize))
}

func (c *txnCommitter) Commit() error {
	err := c.prewriteKeys(c.keys)
	if err != nil {
		log.Warnf("txn commit failed on prewrite: %v", err)
		c.cleanupKeys(c.writtenKeys)
		return errors.Trace(err)
	}

	commitTS, err := c.store.oracle.GetTimestamp()
	if err != nil {
		return errors.Trace(err)
	}
	c.commitTS = commitTS

	err = c.commitKeys(c.keys)
	if err != nil {
		if !c.committed {
			c.cleanupKeys(c.writtenKeys)
			return errors.Trace(err)
		}
		log.Warnf("txn commit succeed with error: %v", err)
	}
	return nil
}

// TiKV recommends each RPC packet should be less than ~1MB. We keep each packet's
// Key+Value size below 512KB.
const txnCommitBatchSize = 512 * 1024

// batchIterfn wraps an iteration function and returns a new one that iterates
// keys by batch size.
func batchIterFn(f func([][]byte) error, sizeFn func([]byte) int) func([][]byte) error {
	return func(keys [][]byte) error {
		var start, end int
		for start = 0; start < len(keys); start = end {
			var size int
			for end = start; end < len(keys) && size < txnCommitBatchSize; end++ {
				size += sizeFn(keys[end])
			}
			if err := f(keys[start:end]); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}
}
