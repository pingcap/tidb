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
)

// Scanner support tikv scan
type Scanner struct {
	snapshot     *tikvSnapshot
	batchSize    int
	valid        bool
	cache        []*pb.KvPair
	idx          int
	nextStartKey []byte
	eof          bool
}

func newScanner(snapshot *tikvSnapshot, startKey []byte, batchSize int) (*Scanner, error) {
	// It must be > 1. Otherwise scanner won't skipFirst.
	if batchSize <= 1 {
		batchSize = scanBatchSize
	}
	scanner := &Scanner{
		snapshot:     snapshot,
		batchSize:    batchSize,
		valid:        true,
		nextStartKey: startKey,
	}
	err := scanner.Next()
	if kv.IsErrNotFound(err) {
		return scanner, nil
	}
	return scanner, errors.Trace(err)
}

// Valid return valid.
func (s *Scanner) Valid() bool {
	return s.valid
}

// Key return key.
func (s *Scanner) Key() kv.Key {
	if s.valid {
		return s.cache[s.idx].Key
	}
	return nil
}

// Value return value.
func (s *Scanner) Value() []byte {
	if s.valid {
		return s.cache[s.idx].Value
	}
	return nil
}

// Next return next element.
func (s *Scanner) Next() error {
	bo := NewBackoffer(scannerNextMaxBackoff)
	if !s.valid {
		return errors.New("scanner iterator is invalid")
	}
	for {
		s.idx++
		if s.idx >= len(s.cache) {
			if s.eof {
				s.Close()
				return kv.ErrNotExist
			}
			err := s.getData(bo)
			if err != nil {
				s.Close()
				return errors.Trace(err)
			}
			if s.idx >= len(s.cache) {
				continue
			}
		}
		if err := s.resolveCurrentLock(bo); err != nil {
			s.Close()
			return errors.Trace(err)
		}
		if len(s.Value()) == 0 {
			// nil stands for NotExist, go to next KV pair.
			continue
		}
		return nil
	}
}

// Close close iterator.
func (s *Scanner) Close() {
	s.valid = false
}

func (s *Scanner) startTS() uint64 {
	return s.snapshot.version.Ver
}

func (s *Scanner) resolveCurrentLock(bo *Backoffer) error {
	current := s.cache[s.idx]
	if current.GetError() == nil {
		return nil
	}
	val, err := s.snapshot.get(bo, kv.Key(current.Key))
	if err != nil {
		return errors.Trace(err)
	}
	current.Error = nil
	current.Value = val
	return nil
}

func (s *Scanner) getData(bo *Backoffer) error {
	log.Debugf("txn getData nextStartKey[%q], txn %d", s.nextStartKey, s.startTS())
	for {
		region, err := s.snapshot.store.regionCache.GetRegion(bo, s.nextStartKey)
		if err != nil {
			return errors.Trace(err)
		}
		req := &pb.Request{
			Type: pb.MessageType_CmdScan.Enum(),
			CmdScanReq: &pb.CmdScanRequest{
				StartKey: []byte(s.nextStartKey),
				Limit:    proto.Uint32(uint32(s.batchSize)),
				Version:  proto.Uint64(s.startTS()),
			},
		}
		resp, err := s.snapshot.store.SendKVReq(bo, req, region.VerID())
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			log.Warnf("scanner getData failed: %s", regionErr)
			err = bo.Backoff(boRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		cmdScanResp := resp.GetCmdScanResp()
		if cmdScanResp == nil {
			return errors.Trace(errBodyMissing)
		}

		kvPairs := cmdScanResp.Pairs
		// Check if kvPair contains error, it should be a Lock.
		for _, pair := range kvPairs {
			if keyErr := pair.GetError(); keyErr != nil {
				lock, err := extractLockFromKeyErr(keyErr)
				if err != nil {
					return errors.Trace(err)
				}
				pair.Key = lock.Key
			}
		}

		s.cache, s.idx = kvPairs, 0
		if len(kvPairs) < s.batchSize {
			// No more data in current Region. Next getData() starts
			// from current Region's endKey.
			s.nextStartKey = region.EndKey()
			if len(region.EndKey()) == 0 {
				// Current Region is the last one.
				s.eof = true
			}
			return nil
		}
		// next getData() starts from the last key in kvPairs (but skip
		// it by appending a '\x00' to the key). Note that next getData()
		// may get an empty response if the Region in fact does not have
		// more data.
		lastKey := kvPairs[len(kvPairs)-1].GetKey()
		s.nextStartKey = kv.Key(lastKey).Next()
		return nil
	}
}
