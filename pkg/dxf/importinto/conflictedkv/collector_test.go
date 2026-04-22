// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package conflictedkv

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/verification"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

func TestCollectResultMerge(t *testing.T) {
	keyspace := []byte("test")

	checkResultFn := func(expected, in *CollectResult) {
		require.EqualValues(t, expected.RowCount, in.RowCount)
		require.EqualValues(t, expected.TotalFileSize, in.TotalFileSize)
		require.EqualValues(t, expected.RowRecordingCapped, in.RowRecordingCapped)
		require.EqualValues(t, expected.Checksum.Sum(), in.Checksum.Sum())
		require.EqualValues(t, expected.Checksum.SumKVS(), in.Checksum.SumKVS())
		require.EqualValues(t, expected.Checksum.SumSize(), in.Checksum.SumSize())
		require.EqualValues(t, expected.Filenames, in.Filenames)
	}
	// Test merging a populated result into an empty one
	r1 := NewCollectResult(keyspace)
	otherSum := verification.MakeKVChecksumWithKeyspace(keyspace, 10, 1, 1)
	r2 := &CollectResult{
		RowCount:           10,
		TotalFileSize:      100,
		RowRecordingCapped: true,
		Checksum:           &otherSum,
		Filenames:          []string{"file1"},
	}
	r1.Merge(r2)
	checkResultFn(r2, r1)

	// Test merging an empty result into a populated one
	r3 := NewCollectResult(keyspace)
	r1.Merge(r3)
	checkResultFn(r2, r1)

	// Test merging a nil result
	r1.Merge(nil)
	checkResultFn(r2, r1)

	// Test merging another populated result
	otherSum = verification.MakeKVChecksumWithKeyspace(keyspace, 20, 2, 2)
	r4 := &CollectResult{
		RowCount:      20,
		TotalFileSize: 200,
		Checksum:      &otherSum,
		Filenames:     []string{"file2"},
	}
	r1.Merge(r4)
	require.EqualValues(t, int64(30), r1.RowCount)
	require.EqualValues(t, int64(300), r1.TotalFileSize)
	require.True(t, r1.RowRecordingCapped)
	require.EqualValues(t, uint64(3), r1.Checksum.Sum())
	require.EqualValues(t, uint64(3), r1.Checksum.SumKVS())
	require.EqualValues(t, uint64(30), r1.Checksum.SumSize())
	require.EqualValues(t, []string{"file1", "file2"}, r1.Filenames)
}

type mockKVStore struct {
	tidbkv.Storage
}

type mockWriter struct {
	closeErr   error
	closeCount int
}

func (w *mockWriter) Write(_ context.Context, p []byte) (int, error) {
	return len(p), nil
}

func (w *mockWriter) Close(context.Context) error {
	w.closeCount++
	return w.closeErr
}

func (s *mockKVStore) GetCodec() tikv.Codec {
	if kerneltype.IsClassic() {
		return tikv.NewCodecV1(tikv.ModeTxn)
	}
	codec, _ := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{Id: 1})
	return codec
}

func TestCollectorHandleEncodedRow(t *testing.T) {
	logger := zap.Must(zap.NewDevelopment())
	ctx := context.Background()

	doTestFn := func(t *testing.T, kvGroup string, maxSize int64, outFileCnt int) {
		objStore := objstore.NewMemStorage()
		t.Cleanup(func() {
			objStore.Close()
		})
		bak := MaxConflictRowFileSize
		MaxConflictRowFileSize = maxSize
		t.Cleanup(func() {
			MaxConflictRowFileSize = bak
		})
		store := &mockKVStore{}
		var sharedSize atomic.Int64
		var sharedTotalFileSize atomic.Int64
		coll := NewCollector(
			nil, logger, objStore, store, "test",
			kvGroup, nil, nil, NewBoundedHandleSet(logger, &sharedSize, units.MiB), &sharedTotalFileSize, nil, nil,
		)
		rowCount := 48
		for i := range rowCount {
			// when write to the conflicted row file, it will be formated as `("id", "value")`
			row := []types.Datum{types.NewStringDatum("id"), types.NewStringDatum("value")}
			pairs := &kv.Pairs{Pairs: []common.KvPair{{Key: []byte(fmt.Sprint(123 * (i + 1)))}}}
			require.NoError(t, coll.HandleEncodedRow(ctx, tidbkv.IntHandle(i), row, pairs))
		}
		require.NoError(t, coll.Close(ctx))
		if kvGroup == external.DataKVGroup {
			require.Empty(t, coll.hdlSet.handles)
		} else {
			require.Equal(t, rowCount, len(coll.hdlSet.handles))
		}
		kvBytes := 184 + rowCount*len(store.GetCodec().GetKeyspace())
		// for nextgen, the crc sum of keyspace is 0 since the row number is even.
		crcSum := uint64(14672641476652606594)
		chkSum := verification.MakeKVChecksumWithKeyspace(store.GetCodec().GetKeyspace(), uint64(kvBytes), uint64(rowCount), crcSum)
		fileNames := make([]string, 0, outFileCnt)
		for i := range outFileCnt {
			fileNames = append(fileNames, path.Join("test", fmt.Sprintf("data-%04d.txt", i+1)))
		}
		require.EqualValues(t, &CollectResult{
			RowCount:      int64(rowCount),
			TotalFileSize: 768,
			Checksum:      &chkSum,
			Filenames:     fileNames,
		}, coll.result)

		var lines int
		for _, f := range fileNames {
			content, err := objStore.ReadFile(ctx, f)
			require.NoError(t, err)
			lines += len(strings.Split(strings.TrimSpace(string(content)), "\n"))
		}
		require.Equal(t, rowCount, lines)
	}

	for _, c := range [][2]int{{90, 8}, {300, 3}, {800, 1}} {
		maxSize, outFileCnt := c[0], c[1]
		t.Run(fmt.Sprintf("data kv, max file size %d bytes", maxSize), func(t *testing.T) {
			doTestFn(t, external.DataKVGroup, int64(maxSize), outFileCnt)
		})
	}

	for _, c := range [][2]int{{90, 8}, {300, 3}, {800, 1}} {
		maxSize, outFileCnt := c[0], c[1]
		t.Run(fmt.Sprintf("index kv, max file size %d bytes", maxSize), func(t *testing.T) {
			doTestFn(t, external.IndexID2KVGroup(1), int64(maxSize), outFileCnt)
		})
	}
}

func TestCollectorHandleEncodedRowMaxTotalFileSize(t *testing.T) {
	logger := zap.Must(zap.NewDevelopment())
	ctx := context.Background()

	objStore := objstore.NewMemStorage()
	t.Cleanup(func() {
		objStore.Close()
	})
	bakFileSize := MaxConflictRowFileSize
	MaxConflictRowFileSize = units.MiB
	t.Cleanup(func() {
		MaxConflictRowFileSize = bakFileSize
	})
	bakTotalFileSize := maxTotalConflictRowFileSize
	maxTotalConflictRowFileSize = 32
	t.Cleanup(func() {
		maxTotalConflictRowFileSize = bakTotalFileSize
	})

	store := &mockKVStore{}
	var sharedSize atomic.Int64
	var sharedTotalFileSize atomic.Int64
	coll := NewCollector(
		nil, logger, objStore, store, "test",
		external.DataKVGroup, nil, nil, NewBoundedHandleSet(logger, &sharedSize, units.MiB), &sharedTotalFileSize, nil, nil,
	)

	rowCount := 5
	expectedSum := verification.NewKVChecksumWithKeyspace(store.GetCodec().GetKeyspace())
	for i := range rowCount {
		row := []types.Datum{types.NewStringDatum("id"), types.NewStringDatum("value")}
		pairs := &kv.Pairs{Pairs: []common.KvPair{{Key: []byte(fmt.Sprint(123 * (i + 1)))}}}
		require.NoError(t, coll.HandleEncodedRow(ctx, tidbkv.IntHandle(i), row, pairs))
		expectedSum.Update(pairs.Pairs)
	}
	require.NoError(t, coll.Close(ctx))

	require.EqualValues(t, 48, sharedTotalFileSize.Load())
	require.EqualValues(t, rowCount, coll.result.RowCount)
	require.EqualValues(t, 32, coll.result.TotalFileSize)
	require.True(t, coll.result.RowRecordingCapped)
	require.True(t, coll.stopRecording)
	require.Nil(t, coll.writer)
	require.EqualValues(t, expectedSum.Sum(), coll.result.Checksum.Sum())
	require.EqualValues(t, expectedSum.SumKVS(), coll.result.Checksum.SumKVS())
	require.EqualValues(t, expectedSum.SumSize(), coll.result.Checksum.SumSize())
	require.Equal(t, []string{path.Join("test", "data-0001.txt")}, coll.result.Filenames)

	content, err := objStore.ReadFile(ctx, coll.result.Filenames[0])
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	require.Len(t, lines, 2)

	t.Run("close failure on size-limit path still clears writer", func(t *testing.T) {
		writer := &mockWriter{closeErr: fmt.Errorf("close failed")}
		testCollector := &Collector{
			logger:       logger,
			handler:      &BaseHandler{},
			result:       NewCollectResult(nil),
			writer:       writer,
			currFileSize: 16,
		}

		err := testCollector.onTotalSizeLimitExceeded(ctx, 64, 32, 32)
		require.ErrorContains(t, err, "close failed")
		require.True(t, testCollector.stopRecording)
		require.True(t, testCollector.result.RowRecordingCapped)
		require.Nil(t, testCollector.writer)
		require.Zero(t, testCollector.currFileSize)
		require.Equal(t, 1, writer.closeCount)
		require.NoError(t, testCollector.Close(ctx))
		require.Equal(t, 1, writer.closeCount)
	})

	t.Run("close failure on switch file path still clears writer", func(t *testing.T) {
		writer := &mockWriter{closeErr: fmt.Errorf("close failed")}
		testCollector := &Collector{
			logger:       logger,
			handler:      &BaseHandler{},
			result:       NewCollectResult(nil),
			writer:       writer,
			currFileSize: 16,
		}

		err := testCollector.switchFile(ctx)
		require.ErrorContains(t, err, "close failed")
		require.Nil(t, testCollector.writer)
		require.Equal(t, int64(16), testCollector.currFileSize)
		require.Equal(t, 1, writer.closeCount)
		require.NoError(t, testCollector.Close(ctx))
		require.Equal(t, 1, writer.closeCount)
	})
}

func TestCollectorHandleEncodedRowMaxTotalFileSizeSharedByCollectors(t *testing.T) {
	logger := zap.Must(zap.NewDevelopment())
	ctx := context.Background()

	objStore := objstore.NewMemStorage()
	t.Cleanup(func() {
		objStore.Close()
	})
	bakFileSize := MaxConflictRowFileSize
	MaxConflictRowFileSize = units.MiB
	t.Cleanup(func() {
		MaxConflictRowFileSize = bakFileSize
	})
	bakTotalFileSize := maxTotalConflictRowFileSize
	maxTotalConflictRowFileSize = 48
	t.Cleanup(func() {
		maxTotalConflictRowFileSize = bakTotalFileSize
	})

	store := &mockKVStore{}
	var sharedSize atomic.Int64
	var sharedTotalFileSize atomic.Int64
	coll1 := NewCollector(
		nil, logger, objStore, store, "test1",
		external.DataKVGroup, nil, nil, NewBoundedHandleSet(logger, &sharedSize, units.MiB), &sharedTotalFileSize, nil, nil,
	)
	coll2 := NewCollector(
		nil, logger, objStore, store, "test2",
		external.DataKVGroup, nil, nil, NewBoundedHandleSet(logger, &sharedSize, units.MiB), &sharedTotalFileSize, nil, nil,
	)

	row := []types.Datum{types.NewStringDatum("id"), types.NewStringDatum("value")}
	for i := range 3 {
		pairs := &kv.Pairs{Pairs: []common.KvPair{{Key: []byte(fmt.Sprintf("a-%d", i))}}}
		require.NoError(t, coll1.HandleEncodedRow(ctx, tidbkv.IntHandle(i), row, pairs))
	}
	for i := range 3 {
		pairs := &kv.Pairs{Pairs: []common.KvPair{{Key: []byte(fmt.Sprintf("b-%d", i))}}}
		require.NoError(t, coll2.HandleEncodedRow(ctx, tidbkv.IntHandle(i), row, pairs))
	}
	require.NoError(t, coll1.Close(ctx))
	require.NoError(t, coll2.Close(ctx))

	require.EqualValues(t, 64, sharedTotalFileSize.Load())
	require.EqualValues(t, 3, coll1.result.RowCount)
	require.EqualValues(t, 48, coll1.result.TotalFileSize)
	require.False(t, coll1.result.RowRecordingCapped)
	require.EqualValues(t, 3, coll2.result.RowCount)
	require.EqualValues(t, 0, coll2.result.TotalFileSize)
	require.True(t, coll2.result.RowRecordingCapped)
	require.False(t, coll1.stopRecording)
	require.True(t, coll2.stopRecording)
	require.Equal(t, []string{path.Join("test1", "data-0001.txt")}, coll1.result.Filenames)
	require.Empty(t, coll2.result.Filenames)

	content, err := objStore.ReadFile(ctx, coll1.result.Filenames[0])
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	require.Len(t, lines, 3)
}
