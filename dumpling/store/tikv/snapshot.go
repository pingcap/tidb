// Copyright 2015 PingCAP, Inc.
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
	"sync"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/terror"
)

var (
	_ kv.Snapshot = (*tikvSnapshot)(nil)
)

const (
	scanBatchSize = 100
	batchGetSize  = 5120
)

// tikvSnapshot implements MvccSnapshot interface.
type tikvSnapshot struct {
	store   *tikvStore
	version kv.Version
}

// newTiKVSnapshot creates a snapshot of an TiKV store.
func newTiKVSnapshot(store *tikvStore, ver kv.Version) *tikvSnapshot {
	return &tikvSnapshot{
		store:   store,
		version: ver,
	}
}

// BatchGet gets all the keys' value from kv-server and returns a map contains key/value pairs.
// The map will not contain nonexistent keys.
func (s *tikvSnapshot) BatchGet(keys []kv.Key) (map[string][]byte, error) {
	// We want [][]byte instead of []kv.Key, use some magic to save memory.
	bytesKeys := *(*[][]byte)(unsafe.Pointer(&keys))

	// Create a map to collect key-values from region servers.
	var mu sync.Mutex
	m := make(map[string][]byte)
	err := s.batchGetKeysByRegions(bytesKeys, func(k, v []byte) {
		if len(v) == 0 {
			return
		}
		mu.Lock()
		m[string(k)] = v
		mu.Unlock()
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return m, nil
}

func (s *tikvSnapshot) batchGetKeysByRegions(keys [][]byte, collectF func(k, v []byte)) error {
	groups, _, err := s.store.regionCache.GroupKeysByRegion(keys)
	if err != nil {
		return errors.Trace(err)
	}

	var batches []batchKeys
	for id, g := range groups {
		batches = appendBatchBySize(batches, id, g, func([]byte) int { return 1 }, batchGetSize)
	}

	if len(batches) == 0 {
		return nil
	}
	if len(batches) == 1 {
		return errors.Trace(s.batchGetSingleRegion(batches[0], collectF))
	}
	ch := make(chan error)
	for _, batch := range batches {
		go func(batch batchKeys) {
			ch <- s.batchGetSingleRegion(batch, collectF)
		}(batch)
	}
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			log.Warnf("snapshot batchGet failed: %v, tid: %d", e, s.version.Ver)
			err = e
		}
	}
	return errors.Trace(err)
}

func (s *tikvSnapshot) batchGetSingleRegion(batch batchKeys, collectF func(k, v []byte)) error {
	pending := batch.keys
	var backoffErr error
	for backoff := txnLockBackoff(); backoffErr == nil; backoffErr = backoff() {
		req := &pb.Request{
			Type: pb.MessageType_CmdBatchGet.Enum(),
			CmdBatchGetReq: &pb.CmdBatchGetRequest{
				Keys:    pending,
				Version: proto.Uint64(s.version.Ver),
			},
		}
		resp, err := s.store.SendKVReq(req, batch.region)
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			err = s.batchGetKeysByRegions(pending, collectF)
			return errors.Trace(err)
		}
		batchGetResp := resp.GetCmdBatchGetResp()
		if batchGetResp == nil {
			return errors.Trace(errBodyMissing)
		}
		var lockedKeys [][]byte
		for _, pair := range batchGetResp.Pairs {
			keyErr := pair.GetError()
			if keyErr == nil {
				collectF(pair.GetKey(), pair.GetValue())
				continue
			}
			// This could be slow if we meet many expired locks.
			// TODO: Find a way to do quick unlock.
			val, err := s.handleKeyError(keyErr)
			if err != nil {
				if terror.ErrorNotEqual(err, errInnerRetryable) {
					return errors.Trace(err)
				}
				lockedKeys = append(lockedKeys, pair.GetKey())
				continue
			}
			collectF(pair.GetKey(), val)
		}
		if len(lockedKeys) > 0 {
			pending = lockedKeys
			continue
		}
		return nil
	}
	return errors.Annotate(backoffErr, txnRetryableMark)
}

// Get gets the value for key k from snapshot.
func (s *tikvSnapshot) Get(k kv.Key) ([]byte, error) {
	req := &pb.Request{
		Type: pb.MessageType_CmdGet.Enum(),
		CmdGetReq: &pb.CmdGetRequest{
			Key:     k,
			Version: proto.Uint64(s.version.Ver),
		},
	}

	var (
		backoffErr    error
		regionBackoff = regionMissBackoff()
		txnBackoff    = txnLockBackoff()
	)
	for backoffErr == nil {
		region, err := s.store.regionCache.GetRegion(k)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp, err := s.store.SendKVReq(req, region.VerID())
		if err != nil {
			return nil, errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			backoffErr = regionBackoff()
			continue
		}
		cmdGetResp := resp.GetCmdGetResp()
		if cmdGetResp == nil {
			return nil, errors.Trace(errBodyMissing)
		}
		val := cmdGetResp.GetValue()
		if keyErr := cmdGetResp.GetError(); keyErr != nil {
			val, err = s.handleKeyError(keyErr)
			if err != nil {
				if terror.ErrorEqual(err, errInnerRetryable) {
					backoffErr = txnBackoff()
					continue
				}
				return nil, errors.Trace(err)
			}
		}
		if len(val) == 0 {
			return nil, kv.ErrNotExist
		}
		return val, nil
	}
	return nil, errors.Annotate(backoffErr, txnRetryableMark)
}

// Seek return a list of key-value pair after `k`.
func (s *tikvSnapshot) Seek(k kv.Key) (kv.Iterator, error) {
	scanner, err := newScanner(s, k, scanBatchSize)
	return scanner, errors.Trace(err)
}

// SeekReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (s *tikvSnapshot) SeekReverse(k kv.Key) (kv.Iterator, error) {
	return nil, kv.ErrNotImplemented
}

// Release unimplement.
func (s *tikvSnapshot) Release() {
}

func extractLockInfoFromKeyErr(keyErr *pb.KeyError) (*pb.LockInfo, error) {
	if locked := keyErr.GetLocked(); locked != nil {
		return locked, nil
	}
	if keyErr.Retryable != nil {
		err := errors.Errorf("tikv restarts txn: %s", keyErr.GetRetryable())
		log.Warn(err)
		return nil, errors.Annotate(err, txnRetryableMark)
	}
	if keyErr.Abort != nil {
		err := errors.Errorf("tikv aborts txn: %s", keyErr.GetAbort())
		log.Warn(err)
		return nil, errors.Trace(err)
	}
	return nil, errors.Errorf("unexpected KeyError: %s", keyErr.String())
}

// handleKeyError tries to resolve locks then retry to get value.
func (s *tikvSnapshot) handleKeyError(keyErr *pb.KeyError) ([]byte, error) {
	lockInfo, err := extractLockInfoFromKeyErr(keyErr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	lock := newLock(s.store, lockInfo.GetPrimaryLock(), lockInfo.GetLockVersion(), lockInfo.GetKey(), s.version.Ver)
	val, err := lock.cleanup()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return val, nil
}
