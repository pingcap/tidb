// Copyright 2019-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"bytes"
	"slices"
	"sort"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/mvcc"
)
// Get implements the MVCCStore interface.
func (store *MVCCStore) Get(reqCtx *requestCtx, key []byte, version uint64) ([]byte, error) {
	pair, err := store.GetPair(reqCtx, key, version)
	if err != nil {
		return nil, err
	}
	return pair.Value, nil
}

// GetPair gets the KvPair
func (store *MVCCStore) GetPair(reqCtx *requestCtx, key []byte, version uint64) (*kvrpcpb.KvPair, error) {
	if reqCtx.isSnapshotIsolation() {
		committedLocks := reqCtx.rpcCtx.CommittedLocks
		if reqCtx.returnCommitTS {
			// set committedLocks to nil if commitTS is needed to make sure all KvPair has CommitTS
			committedLocks = nil
		}
		lockPairs, err := store.CheckKeysLock(version, reqCtx.rpcCtx.ResolvedLocks, committedLocks, key)
		if err != nil {
			return nil, err
		}
		if len(lockPairs) != 0 {
			return &kvrpcpb.KvPair{
				Key:   safeCopy(key),
				Value: safeCopy(getValueFromLock(lockPairs[0].lock)),
			}, nil
		}
	} else if reqCtx.isRcCheckTSIsolationLevel() {
		err := store.CheckKeysLockForRcCheckTS(version, reqCtx.rpcCtx.ResolvedLocks, key)
		if err != nil {
			return nil, err
		}
	}
	val, userMeta, err := reqCtx.getDBReader().Get(key, version)
	if err != nil {
		return nil, err
	}

	var commitTS uint64
	if reqCtx.returnCommitTS && len(userMeta) > 0 {
		commitTS = userMeta.CommitTS()
	}
	return &kvrpcpb.KvPair{
		Key:      safeCopy(key),
		Value:    safeCopy(val),
		CommitTs: commitTS,
	}, err
}

// BatchGet implements the MVCCStore interface.
func (store *MVCCStore) BatchGet(reqCtx *requestCtx, keys [][]byte, version uint64) []*kvrpcpb.KvPair {
	pairs := make([]*kvrpcpb.KvPair, 0, len(keys))
	var remain [][]byte
	if reqCtx.isSnapshotIsolation() {
		remain = make([][]byte, 0, len(keys))
		committedLocks := reqCtx.rpcCtx.CommittedLocks
		if reqCtx.returnCommitTS {
			// set committedLocks to nil if commitTS is needed to make sure all KvPair has CommitTS
			committedLocks = nil
		}
		for _, key := range keys {
			lockPairs, err := store.CheckKeysLock(version, reqCtx.rpcCtx.ResolvedLocks, committedLocks, key)
			if err != nil {
				pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Error: convertToKeyError(err)})
			} else if len(lockPairs) != 0 {
				value := getValueFromLock(lockPairs[0].lock)
				if value != nil {
					pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: value})
				}
			} else {
				remain = append(remain, key)
			}
		}
	} else if reqCtx.isRcCheckTSIsolationLevel() {
		remain = make([][]byte, 0, len(keys))
		for _, key := range keys {
			err := store.CheckKeysLockForRcCheckTS(version, reqCtx.rpcCtx.ResolvedLocks, key)
			if err != nil {
				pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Error: convertToKeyError(err)})
			} else {
				remain = append(remain, key)
			}
		}
	} else {
		remain = keys
	}
	batchGetFunc := func(key, value []byte, userMeta mvcc.DBUserMeta, err error) {
		if len(value) != 0 {
			var commitTS uint64
			if reqCtx.returnCommitTS && err == nil {
				commitTS = userMeta.CommitTS()
			}
			pairs = append(pairs, &kvrpcpb.KvPair{
				Key:      safeCopy(key),
				Value:    safeCopy(value),
				CommitTs: commitTS,
				Error:    convertToKeyError(err),
			})
		}
	}
	reqCtx.getDBReader().BatchGet(remain, version, batchGetFunc)
	return pairs
}

func (store *MVCCStore) collectRangeLock(startTS uint64, startKey, endKey []byte, resolved, committed []uint64,
	isolationLEvel kvrpcpb.IsolationLevel) []*kvrpcpb.KvPair {
	var pairs []*kvrpcpb.KvPair
	it := store.lockStore.NewIterator()
	for it.Seek(startKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), endKey) {
			break
		}
		lock := mvcc.DecodeLock(it.Value())
		if isolationLEvel == kvrpcpb.IsolationLevel_SI {
			lockPair, err := checkLock(lock, it.Key(), startTS, resolved, committed)
			if lockPair != nil {
				pairs = append(pairs, &kvrpcpb.KvPair{
					Key: lockPair.key,
					// deleted key's value is nil
					Value: getValueFromLock(lockPair.lock),
				})
			} else if err != nil {
				pairs = append(pairs, &kvrpcpb.KvPair{
					Error: convertToKeyError(err),
					Key:   safeCopy(it.Key()),
				})
			}
		} else if isolationLEvel == kvrpcpb.IsolationLevel_RCCheckTS {
			err := checkLockForRcCheckTS(lock, it.Key(), startTS, resolved)
			if err != nil {
				pairs = append(pairs, &kvrpcpb.KvPair{
					Error: convertToKeyError(err),
					Key:   safeCopy(it.Key()),
				})
			}
		}
	}
	return pairs
}

func inTSSet(startTS uint64, tsSet []uint64) bool {
	return slices.Contains(tsSet, startTS)
}

type kvScanProcessor struct {
	pairs      []*kvrpcpb.KvPair
	sampleStep uint32
	scanCnt    uint32
}

func (p *kvScanProcessor) Process(key, value []byte, _ uint64) (err error) {
	if p.sampleStep > 0 {
		p.scanCnt++
		if (p.scanCnt-1)%p.sampleStep != 0 {
			return nil
		}
	}
	p.pairs = append(p.pairs, &kvrpcpb.KvPair{
		Key:   safeCopy(key),
		Value: safeCopy(value),
	})
	return nil
}

func (p *kvScanProcessor) SkipValue() bool {
	return false
}

// Scan implements the MVCCStore interface.
func (store *MVCCStore) Scan(reqCtx *requestCtx, req *kvrpcpb.ScanRequest) []*kvrpcpb.KvPair {
	var startKey, endKey []byte
	if req.Reverse {
		startKey = req.EndKey
		if len(startKey) == 0 {
			startKey = reqCtx.regCtx.RawStart()
		}
		endKey = req.StartKey
	} else {
		startKey = req.StartKey
		endKey = req.EndKey
		if len(endKey) == 0 {
			endKey = reqCtx.regCtx.RawEnd()
		}
		if len(endKey) == 0 {
			// Don't scan internal keys.
			endKey = InternalKeyPrefix
		}
	}
	var lockPairs []*kvrpcpb.KvPair
	limit := req.GetLimit()
	if req.SampleStep == 0 {
		if reqCtx.isSnapshotIsolation() || reqCtx.isRcCheckTSIsolationLevel() {
			if bytes.Compare(startKey, endKey) <= 0 {
				lockPairs = store.collectRangeLock(req.GetVersion(), startKey, endKey, reqCtx.rpcCtx.ResolvedLocks,
					reqCtx.rpcCtx.CommittedLocks, reqCtx.rpcCtx.IsolationLevel)
			} else {
				lockPairs = store.collectRangeLock(req.GetVersion(), endKey, startKey, reqCtx.rpcCtx.ResolvedLocks,
					reqCtx.rpcCtx.CommittedLocks, reqCtx.rpcCtx.IsolationLevel)
			}
		}
	} else {
		limit = req.SampleStep * limit
	}
	var scanProc = &kvScanProcessor{
		sampleStep: req.SampleStep,
	}
	reader := reqCtx.getDBReader()
	var err error
	if req.Reverse {
		err = reader.ReverseScan(startKey, endKey, int(limit), req.GetVersion(), scanProc)
	} else {
		err = reader.Scan(startKey, endKey, int(limit), req.GetVersion(), scanProc)
	}
	if err != nil {
		scanProc.pairs = append(scanProc.pairs[:0], &kvrpcpb.KvPair{
			Error: convertToKeyError(err),
		})
		return scanProc.pairs
	}
	pairs := append(lockPairs, scanProc.pairs...)
	sort.SliceStable(pairs, func(i, j int) bool {
		cmp := bytes.Compare(pairs[i].Key, pairs[j].Key)
		if req.Reverse {
			cmp = -cmp
		}
		return cmp < 0
	})
	validPairs := pairs[:0]
	var prev *kvrpcpb.KvPair
	for _, pair := range pairs {
		if prev != nil && bytes.Equal(prev.Key, pair.Key) {
			continue
		}
		prev = pair
		if pair.Error != nil || len(pair.Value) != 0 {
			validPairs = append(validPairs, pair)
			if len(validPairs) >= int(limit) {
				break
			}
		}
	}
	return validPairs
}
