// Copyright 2021 PingCAP, Inc.
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

package txn

import (
	"context"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/kv"
	derr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/store/driver/options"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"github.com/tikv/client-go/v2/txnkv/txnutil"
)

type tikvSnapshot struct {
	*txnsnapshot.KVSnapshot
	// customRetrievers stores all custom retrievers, it is sorted
	customRetrievers sortedRetrievers
}

// NewSnapshot creates a kv.Snapshot with txnsnapshot.KVSnapshot.
func NewSnapshot(snapshot *txnsnapshot.KVSnapshot) kv.Snapshot {
	return &tikvSnapshot{snapshot, nil}
}

// BatchGet gets all the keys' value from kv-server and returns a map contains key/value pairs.
// The map will not contain nonexistent keys.
func (s *tikvSnapshot) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	var result map[string][]byte
	if len(s.customRetrievers) > 0 {
		snapKeys, err := s.customRetrievers.TryBatchGet(ctx, keys, func(k kv.Key, v []byte) {
			if result == nil {
				result = make(map[string][]byte)
			}

			if len(v) > 0 {
				result[string(k)] = v
			}
		})

		if err != nil {
			return nil, err
		}

		keys = snapKeys
	}

	if len(keys) > 0 {
		data, err := s.KVSnapshot.BatchGet(ctx, toTiKVKeys(keys))
		if err != nil {
			return nil, extractKeyErr(err)
		}

		if len(result) == 0 {
			result = data
		} else {
			for k, v := range data {
				result[k] = v
			}
		}
	}

	if result == nil {
		// make sure to return an empty map instead of nil
		result = make(map[string][]byte)
	}

	return result, nil
}

// Get gets the value for key k from snapshot.
func (s *tikvSnapshot) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	if custom, val, err := s.customRetrievers.TryGet(ctx, k); custom {
		if len(val) == 0 {
			return nil, kv.ErrNotExist
		}
		return val, err
	}

	data, err := s.KVSnapshot.Get(ctx, k)
	return data, extractKeyErr(err)
}

// Iter return a list of key-value pair after `k`.
func (s *tikvSnapshot) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	if len(s.customRetrievers) > 0 {
		return s.scanWithCustomRetrievers(k, upperBound, false)
	}

	scanner, err := s.KVSnapshot.Iter(k, upperBound)
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}
	return &tikvScanner{scanner.(*txnsnapshot.Scanner)}, err
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (s *tikvSnapshot) IterReverse(k kv.Key) (kv.Iterator, error) {
	if len(s.customRetrievers) > 0 {
		return s.scanWithCustomRetrievers(nil, k, true)
	}

	scanner, err := s.KVSnapshot.IterReverse(k)
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}
	return &tikvScanner{scanner.(*txnsnapshot.Scanner)}, err
}

func (s *tikvSnapshot) scanWithCustomRetrievers(startKey, endKey kv.Key, reverse bool) (kv.Iterator, error) {
	snapshotRetriever := NewSnapshot(s.KVSnapshot)
	scans := s.customRetrievers.GetScanRetrievers(startKey, endKey, snapshotRetriever)
	if len(scans) == 0 {
		return &kv.EmptyIterator{}, nil
	}

	iters := make([]kv.Iterator, len(scans))
	for i, retriever := range scans {
		iter, err := retriever.ScanCurrentRange(reverse)
		if err != nil {
			return nil, err
		}

		if retriever.Retriever != snapshotRetriever {
			if iter, err = filterEmptyValue(iter); err != nil {
				return nil, err
			}
		}

		loc := i
		if reverse {
			loc = len(iters) - i - 1
		}
		iters[loc] = iter
	}

	if len(iters) == 1 {
		return iters[0], nil
	}

	return newOneByOneIter(iters), nil
}

func (s *tikvSnapshot) SetOption(opt int, val interface{}) {
	switch opt {
	case kv.IsolationLevel:
		level := getTiKVIsolationLevel(val.(kv.IsoLevel))
		s.KVSnapshot.SetIsolationLevel(level)
	case kv.Priority:
		s.KVSnapshot.SetPriority(getTiKVPriority(val.(int)))
	case kv.NotFillCache:
		s.KVSnapshot.SetNotFillCache(val.(bool))
	case kv.SnapshotTS:
		s.KVSnapshot.SetSnapshotTS(val.(uint64))
	case kv.ReplicaRead:
		t := options.GetTiKVReplicaReadType(val.(kv.ReplicaReadType))
		s.KVSnapshot.SetReplicaRead(t)
	case kv.SampleStep:
		s.KVSnapshot.SetSampleStep(val.(uint32))
	case kv.TaskID:
		s.KVSnapshot.SetTaskID(val.(uint64))
	case kv.CollectRuntimeStats:
		if val == nil {
			s.KVSnapshot.SetRuntimeStats(nil)
		} else {
			s.KVSnapshot.SetRuntimeStats(val.(*txnsnapshot.SnapshotRuntimeStats))
		}
	case kv.IsStalenessReadOnly:
		s.KVSnapshot.SetIsStatenessReadOnly(val.(bool))
	case kv.MatchStoreLabels:
		s.KVSnapshot.SetMatchStoreLabels(val.([]*metapb.StoreLabel))
	case kv.ResourceGroupTag:
		s.KVSnapshot.SetResourceGroupTag(val.([]byte))
	case kv.TxnScope:
		s.KVSnapshot.SetTxnScope(val.(string))
	case kv.SortedCustomRetrievers:
		s.customRetrievers = val.([]*RangedKVRetriever)
	}
}

func toTiKVKeys(keys []kv.Key) [][]byte {
	bytesKeys := *(*[][]byte)(unsafe.Pointer(&keys))
	return bytesKeys
}

func getTiKVIsolationLevel(level kv.IsoLevel) txnsnapshot.IsoLevel {
	switch level {
	case kv.SI:
		return txnsnapshot.SI
	case kv.RC:
		return txnsnapshot.RC
	default:
		return txnsnapshot.SI
	}
}

func getTiKVPriority(pri int) txnutil.Priority {
	switch pri {
	case kv.PriorityHigh:
		return txnutil.PriorityHigh
	case kv.PriorityLow:
		return txnutil.PriorityLow
	default:
		return txnutil.PriorityNormal
	}
}
