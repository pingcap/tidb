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
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/terror"
)

// Scanner support tikv scan
type Scanner struct {
	StartKey     []byte // len(StartKey) == 0 stands for minimum key.
	Version      uint64
	nextStartKey []byte
	valid        bool
	snapshot     tikvSnapshot
	cache        []*resultRow
	idx          int
	batchSize    int
	region       *requestRegion
	skipFirst    bool // Skip first row when get next data from same region.
}

func newScanner(region *requestRegion, startKey []byte, ver uint64, snapshot tikvSnapshot, batchSize int) *Scanner {
	// It must be > 1. Otherwise scanner won't skipFirst.
	if batchSize <= 1 {
		batchSize = scanBatchSize
	}
	if startKey == nil {
		startKey = []byte("")
	}
	return &Scanner{
		StartKey:     startKey,
		Version:      ver,
		nextStartKey: startKey,
		valid:        true,
		snapshot:     snapshot,
		batchSize:    batchSize,
		region:       region,
		skipFirst:    false,
	}
}

// Valid return valid.
func (s *Scanner) Valid() bool {
	_ = s.getCurrent()
	return s.valid
}

// Key return key.
func (s *Scanner) Key() kv.Key {
	ret := s.getCurrent()
	return kv.Key(ret.key)
}

// Value return value.
func (s *Scanner) Value() []byte {
	ret := s.getCurrent()
	return ret.value
}

// Next return next element.
func (s *Scanner) Next() error {
	if !s.valid {
		return errors.New("scanner iterator is invalid")
	}
	s.idx++
	for s.idx >= len(s.cache) {
		if len(s.nextStartKey) == 0 {
			break
		}
		ret, err := s.getData()
		if err != nil {
			log.Errorf("getData failed: %s", err)
			s.valid = false
			return errors.Trace(err)
		}
		s.cache = ret
		if s.skipFirst {
			s.idx = 1
		} else {
			s.idx = 0
		}
		if s.mayHasMoreRegionData() {
			s.nextStartKey, s.skipFirst = s.cache[len(s.cache)-1].key, true
		} else {
			// There is no more data in this region, switch next.
			s.nextStartKey, s.skipFirst = s.region.meta.GetEndKey(), false
			break
		}
	}
	if s.idx >= len(s.cache) {
		s.Close()
		return kv.ErrNotExist
	}
	return nil
}

// Close close iterator.
func (s *Scanner) Close() {
	s.valid = false
}

func (s *Scanner) getCurrent() *resultRow {
	ret := &resultRow{key: nil, value: nil}
	if !s.valid {
		return ret
	}
	if len(s.cache) == 0 {
		if err := s.Next(); err != nil {
			return ret
		}
	}
	if s.idx >= len(s.cache) {
		return ret
	}
	if s.cache[s.idx] == nil {
		return ret
	}
	ret = s.cache[s.idx]
	if ret.lock == nil {
		return ret
	}

	var err error
	for tryCount := 1; tryCount <= maxGetCount; tryCount++ {
		var val []byte
		// This key is lock, try to cleanup and get it again.
		if val, err = s.snapshot.handleKeyError(&pb.KeyError{Locked: ret.lock}); err != nil {
			if terror.ErrorNotEqual(err, errInnerRetryable) {
				log.Errorf("Get error[%s]", errors.ErrorStack(err))
			}
		} else {
			ret.value = val
			return ret
		}
	}
	if err != nil {
		log.Errorf("Get current locked key failed: %s", err)
		return ret
	}
	return ret
}

func (s *Scanner) getData() ([]*resultRow, error) {
	region, err := s.snapshot.store.getRegion(s.nextStartKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.region = region
	req := &pb.Request{
		Type: pb.MessageType_CmdScan.Enum(),
		CmdScanReq: &pb.CmdScanRequest{
			StartKey: []byte(s.nextStartKey),
			Limit:    proto.Uint32(uint32(s.batchSize)),
			Version:  proto.Uint64(s.Version),
		},
	}

	log.Debugf("Seek nextStartKey[%q]", s.nextStartKey)
	resp, err := s.snapshot.store.SendKVReq(req, region)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if regionErr := resp.GetRegionError(); regionErr != nil {
		//TODO: retry internally
		return nil, errors.Annotate(errors.New(regionErr.String()), txnRetryableMark)
	}
	cmdScanResp := resp.GetCmdScanResp()
	if cmdScanResp == nil {
		return nil, errors.Trace(errBodyMissing)
	}
	pairs := cmdScanResp.GetPairs()
	ret := make([]*resultRow, len(pairs))
	for i, pair := range pairs {
		keyErr := pair.GetError()
		if keyErr == nil {
			ret[i] = &resultRow{
				key:   pair.GetKey(),
				value: pair.GetValue(),
			}
			continue
		}
		lockInfo, err := extractLockInfoFromKeyErr(keyErr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret[i] = &resultRow{
			key:  lockInfo.GetKey(),
			lock: lockInfo,
		}
	}
	return ret, nil
}

// mayHasMoreRegionData whether current region has more data.
// If region has multiple of batchSize exactly, it return true but
// it will return false(only one row) next time.
func (s *Scanner) mayHasMoreRegionData() bool {
	return len(s.cache) == s.batchSize
}

type resultRow struct {
	key   []byte
	value []byte
	lock  *pb.LockInfo
}
