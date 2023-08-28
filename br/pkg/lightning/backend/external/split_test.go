// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/require"
)

func TestRangeSplitterStrictCase(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	subDir := "/mock-test"

	writer1 := NewWriterBuilder().
		SetMemorySizeLimit(15). // slightly larger than len("key01") + len("value01")
		SetPropSizeDistance(1).
		SetPropKeysDistance(1).
		Build(memStore, subDir, 1)
	keys1 := [][]byte{
		[]byte("key01"), []byte("key11"), []byte("key21"),
	}
	values1 := [][]byte{
		[]byte("value01"), []byte("value11"), []byte("value21"),
	}
	dataFiles1, statFiles1, err := MockExternalEngineWithWriter(memStore, writer1, subDir, keys1, values1)
	require.NoError(t, err)
	require.Len(t, dataFiles1, 2)
	require.Len(t, statFiles1, 2)

	writer2 := NewWriterBuilder().
		SetMemorySizeLimit(15).
		SetPropSizeDistance(1).
		SetPropKeysDistance(1).
		Build(memStore, subDir, 2)
	keys2 := [][]byte{
		[]byte("key02"), []byte("key12"), []byte("key22"),
	}
	values2 := [][]byte{
		[]byte("value02"), []byte("value12"), []byte("value22"),
	}
	dataFiles12, statFiles12, err := MockExternalEngineWithWriter(memStore, writer2, subDir, keys2, values2)
	require.NoError(t, err)
	require.Len(t, dataFiles12, 4)
	require.Len(t, statFiles12, 4)

	writer3 := NewWriterBuilder().
		SetMemorySizeLimit(15).
		SetPropSizeDistance(1).
		SetPropKeysDistance(1).
		Build(memStore, subDir, 3)
	keys3 := [][]byte{
		[]byte("key03"), []byte("key13"), []byte("key23"),
	}
	values3 := [][]byte{
		[]byte("value03"), []byte("value13"), []byte("value23"),
	}
	dataFiles123, statFiles123, err := MockExternalEngineWithWriter(memStore, writer3, subDir, keys3, values3)
	require.NoError(t, err)
	require.Len(t, dataFiles123, 6)
	require.Len(t, statFiles123, 6)

	require.Equal(t, []string{
		"/mock-test/1/0", "/mock-test/1/1",
		"/mock-test/2/0", "/mock-test/2/1",
		"/mock-test/3/0", "/mock-test/3/1",
	}, dataFiles123)

	splitter, err := NewRangeSplitter(
		ctx, dataFiles123, statFiles123, memStore, 1000, 2, 1000, 1,
	)
	require.NoError(t, err)
	endKey, dataFiles, statFiles, splitKeys, err := splitter.SplitOneRangesGroup()
	require.NoError(t, err)
	require.EqualValues(t, kv.Key("key03"), endKey)
	require.Equal(t, []string{"/mock-test/1/0", "/mock-test/2/0"}, dataFiles)
	require.Equal(t, []string{"/mock-test/1_stat/0", "/mock-test/2_stat/0"}, statFiles)
	require.Equal(t, [][]byte{[]byte("key02")}, splitKeys)
}
