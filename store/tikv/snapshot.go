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
	bo := NewBackoffer(batchGetMaxBackoff)

	// Create a map to collect key-values from region servers.
	var mu sync.Mutex
	m := make(map[string][]byte)
	err := s.batchGetKeysByRegions(bo, bytesKeys, func(k, v []byte) {
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

func (s *tikvSnapshot) batchGetKeysByRegions(bo *Backoffer, keys [][]byte, collectF func(k, v []byte)) error {
	groups, _, err := s.store.regionCache.GroupKeysByRegion(bo, keys)
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
		return errors.Trace(s.batchGetSingleRegion(bo, batches[0], collectF))
	}
	ch := make(chan error)
	for _, batch := range batches {
		go func(batch batchKeys) {
			ch <- s.batchGetSingleRegion(bo, batch, collectF)
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

func (s *tikvSnapshot) batchGetSingleRegion(bo *Backoffer, batch batchKeys, collectF func(k, v []byte)) error {
	pending := batch.keys
	for {
		req := &pb.Request{
			Type: pb.MessageType_CmdBatchGet.Enum(),
			CmdBatchGetReq: &pb.CmdBatchGetRequest{
				Keys:    pending,
				Version: proto.Uint64(s.version.Ver),
			},
		}
		resp, err := s.store.SendKVReq(bo, req, batch.region)
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = s.batchGetKeysByRegions(bo, pending, collectF)
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
			var val []byte
			val, err = s.handleKeyError(bo, keyErr)
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
			err = bo.Backoff(boTxnLock, errors.Errorf("batchGet lockedKeys: %d", len(lockedKeys)))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		return nil
	}
}

// Get gets the value for key k from snapshot.
func (s *tikvSnapshot) Get(k kv.Key) ([]byte, error) {
	bo := NewBackoffer(getMaxBackoff)

	req := &pb.Request{
		Type: pb.MessageType_CmdGet.Enum(),
		CmdGetReq: &pb.CmdGetRequest{
			Key:     k,
			Version: proto.Uint64(s.version.Ver),
		},
	}
	for {
		region, err := s.store.regionCache.GetRegion(bo, k)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp, err := s.store.SendKVReq(bo, req, region.VerID())
		if err != nil {
			return nil, errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}
		cmdGetResp := resp.GetCmdGetResp()
		if cmdGetResp == nil {
			return nil, errors.Trace(errBodyMissing)
		}
		val := cmdGetResp.GetValue()
		if keyErr := cmdGetResp.GetError(); keyErr != nil {
			val, err = s.handleKeyError(bo, keyErr)
			if err != nil {
				if terror.ErrorEqual(err, errInnerRetryable) {
					err = bo.Backoff(boTxnLock, err)
					if err != nil {
						return nil, errors.Trace(err)
					}
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
func (s *tikvSnapshot) handleKeyError(bo *Backoffer, keyErr *pb.KeyError) ([]byte, error) {
	lockInfo, err := extractLockInfoFromKeyErr(keyErr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	lock := newLock(s.store, lockInfo.GetPrimaryLock(), lockInfo.GetLockVersion(), lockInfo.GetKey(), s.version.Ver)
	val, err := lock.cleanup(bo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return val, nil
}
