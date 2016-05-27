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
	maxGetCount   = 3
	batchGetSize  = 100
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

// makeBatchGetReqs splits each key into corresponding region.
func (s *tikvSnapshot) makeBatchGetReqs(keys []kv.Key) (map[RegionVerID]*batchGetRegion, error) {
	startTS := s.version.Ver
	multiBatchGet := map[RegionVerID]*batchGetRegion{}
	for _, k := range keys {
		region, err := s.store.regionCache.GetRegion(k)
		if err != nil {
			return nil, errors.Trace(err)
		}
		regionID := region.VerID()
		singleBatchGet, ok := multiBatchGet[regionID]
		if !ok {
			singleBatchGet = &batchGetRegion{
				CmdBatchGetRequest: &pb.CmdBatchGetRequest{
					Version: proto.Uint64(startTS),
				},
				region: regionID,
			}
			multiBatchGet[regionID] = singleBatchGet
		}
		cmdBatchGetReq := singleBatchGet.CmdBatchGetRequest
		cmdBatchGetReq.Keys = append(cmdBatchGetReq.Keys, k)
	}
	return multiBatchGet, nil
}

// doBatchGet sends BatchGet RPC request. If any key is locked, use tikvSnapshot.Get() to retry.
func (s *tikvSnapshot) doBatchGet(singleBatchGet *batchGetRegion) (map[string][]byte, error) {
	cmdBatchGetReq := singleBatchGet.CmdBatchGetRequest
	keys := cmdBatchGetReq.GetKeys()
	if len(keys) == 0 {
		return nil, nil
	}
	req := &pb.Request{
		Type:           pb.MessageType_CmdBatchGet.Enum(),
		CmdBatchGetReq: cmdBatchGetReq,
	}
	resp, err := s.store.SendKVReq(req, singleBatchGet.region)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if regionErr := resp.GetRegionError(); regionErr != nil {
		//TODO: retry internally
		return nil, errors.Annotate(errors.New(regionErr.String()), txnRetryableMark)
	}
	cmdBatchGetResp := resp.GetCmdBatchGetResp()
	if cmdBatchGetResp == nil {
		return nil, errors.Trace(errBodyMissing)
	}
	pairs := cmdBatchGetResp.GetPairs()
	m := make(map[string][]byte, len(pairs))
	for _, pair := range pairs {
		keyErr := pair.GetError()
		if keyErr == nil {
			if val := pair.GetValue(); len(val) > 0 {
				m[string(pair.GetKey())] = val
			}
			continue
		}
		lockInfo, err := extractLockInfoFromKeyErr(keyErr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		val, err := s.Get(lockInfo.GetKey())
		if err != nil {
			if terror.ErrorEqual(err, kv.ErrNotExist) {
				continue
			}
			return nil, errors.Trace(err)
		}
		m[string(lockInfo.GetKey())] = val
	}
	return m, nil
}

// BatchGet gets all the keys' value from kv-server and returns a map contains key/value pairs.
// The map will not contain nonexistent keys.
func (s *tikvSnapshot) BatchGet(keys []kv.Key) (map[string][]byte, error) {
	m := make(map[string][]byte, len(keys))

	multiBatchGet, err := s.makeBatchGetReqs(keys)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, singleBatchGet := range multiBatchGet {
		keys := singleBatchGet.GetKeys()
		for startIdx := 0; startIdx < len(keys); startIdx += batchGetSize {
			endIdx := startIdx + batchGetSize
			if endIdx > len(keys) {
				endIdx = len(keys)
			}
			newSingleBatchGet := &batchGetRegion{
				CmdBatchGetRequest: &pb.CmdBatchGetRequest{
					Keys:    keys[startIdx:endIdx],
					Version: proto.Uint64(singleBatchGet.GetVersion()),
				},
				region: singleBatchGet.region,
			}
			res, err := s.doBatchGet(newSingleBatchGet)
			if err != nil {
				return nil, errors.Trace(err)
			}
			m, err = mergeResult(m, res)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	return m, nil
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

// mergeResult Merge d2 into d1. If d1 and d2 are overlap, it returns error.
func mergeResult(d1, d2 map[string][]byte) (map[string][]byte, error) {
	if d1 == nil {
		d1 = make(map[string][]byte)
	}
	for k2, v2 := range d2 {
		if v1, ok := d1[k2]; ok {
			// Because compare []byte takes too much time,
			// if conflict return error directly even their values are same.
			return nil, errors.Errorf("add dict conflict key[%s] v1[%q] v2[%q]",
				k2, v1, v2)
		}
		d1[k2] = v2
	}
	return d1, nil
}

type batchGetRegion struct {
	*pb.CmdBatchGetRequest
	region RegionVerID
}
