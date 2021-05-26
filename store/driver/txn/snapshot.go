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
	"github.com/pingcap/tidb/store/tikv"
)

type tikvSnapshot struct {
	*tikv.KVSnapshot
}

// NewSnapshot creates a kv.Snapshot with tikv.KVSnapshot.
func NewSnapshot(snapshot *tikv.KVSnapshot) kv.Snapshot {
	return &tikvSnapshot{snapshot}
}

// BatchGet gets all the keys' value from kv-server and returns a map contains key/value pairs.
// The map will not contain nonexistent keys.
func (s *tikvSnapshot) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	data, err := s.KVSnapshot.BatchGet(ctx, toTiKVKeys(keys))
	return data, extractKeyErr(err)
}

// Get gets the value for key k from snapshot.
func (s *tikvSnapshot) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	data, err := s.KVSnapshot.Get(ctx, k)
	return data, extractKeyErr(err)
}

// Iter return a list of key-value pair after `k`.
func (s *tikvSnapshot) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	scanner, err := s.KVSnapshot.Iter(k, upperBound)
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}
	return &tikvScanner{scanner.(*tikv.Scanner)}, err
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (s *tikvSnapshot) IterReverse(k kv.Key) (kv.Iterator, error) {
	scanner, err := s.KVSnapshot.IterReverse(k)
	if err != nil {
		return nil, derr.ToTiDBErr(err)
	}
	return &tikvScanner{scanner.(*tikv.Scanner)}, err
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
			s.KVSnapshot.SetRuntimeStats(val.(*tikv.SnapshotRuntimeStats))
		}
	case kv.IsStalenessReadOnly:
		s.KVSnapshot.SetIsStatenessReadOnly(val.(bool))
	case kv.MatchStoreLabels:
		s.KVSnapshot.SetMatchStoreLabels(val.([]*metapb.StoreLabel))
	case kv.ResourceGroupTag:
		s.KVSnapshot.SetResourceGroupTag(val.([]byte))
	}
}

func toTiKVKeys(keys []kv.Key) [][]byte {
	bytesKeys := *(*[][]byte)(unsafe.Pointer(&keys))
	return bytesKeys
}

func getTiKVIsolationLevel(level kv.IsoLevel) tikv.IsoLevel {
	switch level {
	case kv.SI:
		return tikv.SI
	case kv.RC:
		return tikv.RC
	default:
		return tikv.SI
	}
}

func getTiKVPriority(pri int) tikv.Priority {
	switch pri {
	case kv.PriorityHigh:
		return tikv.PriorityHigh
	case kv.PriorityLow:
		return tikv.PriorityLow
	default:
		return tikv.PriorityNormal
	}
}
