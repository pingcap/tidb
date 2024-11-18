// Copyright 2019 PingCAP, Inc.
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

package verification_test

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/stretchr/testify/require"
)

func TestChecksum(t *testing.T) {
	checksum := verification.NewKVChecksum()
	require.Equal(t, uint64(0), checksum.Sum())

	// checksum on nothing
	checksum.Update([]common.KvPair{})
	require.Equal(t, uint64(0), checksum.Sum())

	checksum.Update(nil)
	require.Equal(t, uint64(0), checksum.Sum())

	// checksum on real data
	excpectChecksum := uint64(4850203904608948940)

	kvs := []common.KvPair{
		{
			Key: []byte("Cop"),
			Val: []byte("PingCAP"),
		},
		{
			Key: []byte("Introduction"),
			Val: []byte("Inspired by Google Spanner/F1, PingCAP develops TiDB."),
		},
	}

	checksum.Update(kvs)

	var kvBytes uint64
	for _, kv := range kvs {
		kvBytes += uint64(len(kv.Key) + len(kv.Val))
	}
	require.Equal(t, kvBytes, checksum.SumSize())
	require.Equal(t, uint64(len(kvs)), checksum.SumKVS())
	require.Equal(t, excpectChecksum, checksum.Sum())

	// recompute on same key-value
	checksum.Update(kvs)
	require.Equal(t, kvBytes<<1, checksum.SumSize())
	require.Equal(t, uint64(len(kvs))<<1, checksum.SumKVS())
	require.NotEqual(t, excpectChecksum, checksum.Sum())
}

func TestChecksumJSON(t *testing.T) {
	testStruct := &struct {
		Checksum verification.KVChecksum
	}{
		Checksum: verification.MakeKVChecksum(123, 456, 7890),
	}

	res, err := json.Marshal(testStruct)

	require.NoError(t, err)
	require.Equal(t, []byte(`{"Checksum":{"checksum":7890,"size":123,"kvs":456}}`), res)
}

func TestGroupChecksum(t *testing.T) {
	kvPair := common.KvPair{Key: []byte("key"), Val: []byte("val")}
	kvPair2 := common.KvPair{Key: []byte("key2"), Val: []byte("val2")}

	c := verification.NewKVGroupChecksumWithKeyspace([]byte(""))
	c.UpdateOneDataKV(kvPair)
	c.UpdateOneIndexKV(1, kvPair2)
	inner := c.GetInnerChecksums()
	require.Equal(t, 2, len(inner))
	require.Equal(t, uint64(1), inner[1].SumKVS())
	require.Equal(t, uint64(1), inner[verification.DataKVGroupID].SumKVS())

	keyspaceC := verification.NewKVGroupChecksumWithKeyspace([]byte("keyspace"))
	keyspaceC.UpdateOneDataKV(kvPair)
	keyspaceC.UpdateOneIndexKV(1, kvPair2)
	keyspaceInner := keyspaceC.GetInnerChecksums()
	require.NotEqual(t, inner, keyspaceInner)

	c2 := verification.NewKVGroupChecksumWithKeyspace(nil)
	c2.UpdateOneIndexKV(1, kvPair)
	c2.UpdateOneIndexKV(2, kvPair2)
	c.Add(c2)
	inner = c.GetInnerChecksums()
	require.Equal(t, 3, len(inner))
	require.Equal(t, uint64(2), inner[1].SumKVS())
	require.Equal(t, uint64(1), inner[2].SumKVS())
	require.Equal(t, uint64(1), inner[verification.DataKVGroupID].SumKVS())

	dataKVCnt, indexKVCnt := c.DataAndIndexSumKVS()
	require.Equal(t, uint64(1), dataKVCnt)
	require.Equal(t, uint64(3), indexKVCnt)

	dataSize, indexSize := c.DataAndIndexSumSize()
	require.Equal(t, uint64(6), dataSize)
	require.Equal(t, uint64(22), indexSize)

	merged := c.MergedChecksum()
	require.Equal(t, uint64(4), merged.SumKVS())
	require.Equal(t, uint64(28), merged.SumSize())
}
