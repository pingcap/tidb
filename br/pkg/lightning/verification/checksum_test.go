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

	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/stretchr/testify/require"
)

func uint64NotEqual(a uint64, b uint64) bool { return a != b }

func TestChcksum(t *testing.T) {
	checksum := verification.NewKVChecksum(0)
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
	require.True(t, uint64NotEqual(checksum.Sum(), excpectChecksum))
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
