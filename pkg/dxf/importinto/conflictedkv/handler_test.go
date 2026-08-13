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

package conflictedkv_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/dxf/importinto/conflictedkv"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/ingestor/globalsort"
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockHandleEncodedRowFn func(ctx context.Context, rowKey tidbkv.Key, row []types.Datum, kvPairs *kv.Pairs) error

func (h mockHandleEncodedRowFn) HandleEncodedRow(ctx context.Context, rowKey tidbkv.Key, row []types.Datum, kvPairs *kv.Pairs) error {
	return h(ctx, rowKey, row, kvPairs)
}

type mockTrafficRecorder struct {
	readBytes  atomic.Uint64
	writeBytes atomic.Uint64
}

func (r *mockTrafficRecorder) IncClusterReadBytes(n uint64) {
	r.readBytes.Add(n)
}

func (r *mockTrafficRecorder) IncClusterWriteBytes(n uint64) {
	r.writeBytes.Add(n)
}

func getEncoder(t *testing.T, tbl table.Table) *importer.TableKVEncoder {
	t.Helper()
	encodeCfg := &encode.EncodingConfig{
		Table:                tbl,
		UseIdentityAutoRowID: true,
	}
	controller := &importer.LoadDataController{
		ASTArgs: &importer.ASTArgs{},
		Plan:    &importer.Plan{},
		Table:   tbl,
	}
	localEncoder, err := importer.NewTableKVEncoderForDupResolve(encodeCfg, controller)
	require.NoError(t, err)
	return localEncoder
}

func TestHandler(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	ctx := context.Background()
	logger := zap.Must(zap.NewDevelopment())

	cleanupEnvFn := func(t *testing.T, tableName string) table.Table {
		t.Helper()
		tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
		if tableName == "tc" {
			tk.MustExec("create table tc(a bigint primary key clustered, b int, c int, index(b), unique(c))")
		} else {
			tk.MustExec("create table tn(a bigint primary key nonclustered, b int, c int, index(b), unique(c))")
		}
		tbl, err := do.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr(tableName))
		require.NoError(t, err)
		return tbl
	}

	t.Run("test data kv handler", func(t *testing.T) {
		doTestFn := func(t *testing.T, tableName string, expectedKVs int) {
			tbl := cleanupEnvFn(t, tableName)
			encoder := getEncoder(t, tbl)
			var rowCnt, kvPairCnt int64
			mockEncodedKVHdl := mockHandleEncodedRowFn(func(ctx context.Context, rowKey tidbkv.Key, row []types.Datum, kvPairs *kv.Pairs) error {
				handle, err := tablecodec.DecodeRowKey(rowKey)
				require.NoError(t, err)
				require.Equal(t, tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, handle), rowKey)
				rowCnt++
				kvPairCnt += int64(len(kvPairs.Pairs))
				return nil
			})
			progressCollector := &execute.TestCollector{}
			baseHdl := conflictedkv.NewBaseHandler(
				tbl, globalsort.DataKVGroup, store.GetCodec(), encoder, mockEncodedKVHdl, progressCollector, logger,
			)
			dataKVHdl := conflictedkv.NewDataKVHandler(baseHdl)
			t.Cleanup(func() {
				require.NoError(t, dataKVHdl.Close(ctx))
			})
			require.NoError(t, dataKVHdl.PreRun())
			var ch = make(chan *simplesst.KVPair, 10)
			eg := util.NewErrorGroupWithRecover()
			eg.Go(func() error {
				return dataKVHdl.Run(ctx, ch)
			})

			eg.Go(func() error {
				dupID := 100
				row := []types.Datum{types.NewDatum(dupID), types.NewDatum(dupID), types.NewDatum(dupID)}
				localEncoder := getEncoder(t, tbl)
				dupPairs, err2 := localEncoder.Encode(row, int64(dupID))
				require.NoError(t, err2)
				for _, pair := range dupPairs.Pairs {
					if !tablecodec.IsRecordKey(pair.Key) {
						continue
					}
					// completely same row repeat 10 times
					for range 10 {
						ch <- &simplesst.KVPair{Key: store.GetCodec().EncodeKey(pair.Key), Value: pair.Val}
					}
				}
				close(ch)
				return nil
			})
			require.NoError(t, eg.Wait())
			require.EqualValues(t, 10, rowCnt)
			require.EqualValues(t, expectedKVs, kvPairCnt)
			require.EqualValues(t, 10, progressCollector.ProcessedCnt.Load())
		}

		t.Run("clustered pk table", func(t *testing.T) {
			doTestFn(t, "tc", 30)
		})
		t.Run("non-clustered pk table", func(t *testing.T) {
			doTestFn(t, "tn", 40)
		})
	})

	t.Run("data kv handler re-encodes visible columns after functional index", func(t *testing.T) {
		tk.MustExec("drop table if exists tf")
		tk.MustExec(`create table tf(
			id bigint primary key clustered,
			a int,
			unique key uk_expr ((a + 1))
		)`)
		tk.MustExec("alter table tf add column tail int")
		tk.MustExec("alter table tf add unique key uk_tail (tail)")
		tbl, err := do.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("tf"))
		require.NoError(t, err)

		fixtureEncoder := getEncoder(t, tbl)
		t.Cleanup(func() {
			require.NoError(t, fixtureEncoder.Close())
		})
		fixturePairs, err := fixtureEncoder.Encode(
			[]types.Datum{types.NewIntDatum(1), types.NewIntDatum(10), types.NewIntDatum(100)},
			1,
		)
		require.NoError(t, err)

		var tailIndexID int64
		for _, idx := range tbl.Meta().Indices {
			if idx.Name.L == "uk_tail" {
				tailIndexID = idx.ID
				break
			}
		}
		require.NotZero(t, tailIndexID)

		expectedKVs := make(map[string]string, len(fixturePairs.Pairs))
		var (
			recordKV        *simplesst.KVPair
			expectedTailKey string
		)
		for _, pair := range fixturePairs.Pairs {
			expectedKVs[string(pair.Key)] = string(pair.Val)
			if tablecodec.IsRecordKey(pair.Key) {
				recordKV = &simplesst.KVPair{
					Key:   store.GetCodec().EncodeKey(append(tidbkv.Key(nil), pair.Key...)),
					Value: append([]byte(nil), pair.Val...),
				}
				continue
			}
			indexID, err := tablecodec.DecodeIndexID(pair.Key)
			require.NoError(t, err)
			if indexID == tailIndexID {
				expectedTailKey = string(pair.Key)
			}
		}
		require.NotNil(t, recordKV)
		require.NotEmpty(t, expectedTailKey)
		fixturePairs.Clear()

		handled := false
		encodedRowHandler := mockHandleEncodedRowFn(func(
			_ context.Context, _ tidbkv.Key, row []types.Datum, kvPairs *kv.Pairs,
		) error {
			handled = true
			actualKVs := make(map[string]string, len(kvPairs.Pairs))
			for _, pair := range kvPairs.Pairs {
				actualKVs[string(pair.Key)] = string(pair.Val)
			}
			_, ok := actualKVs[expectedTailKey]
			require.True(t, ok, "re-encoded KVs must contain uk_tail=100")
			require.Equal(t, expectedKVs, actualKVs)
			require.Len(t, row, 3)
			require.Equal(t, int64(100), row[2].GetInt64())
			return nil
		})
		dataKVHandler := conflictedkv.NewDataKVHandler(conflictedkv.NewBaseHandler(
			tbl,
			globalsort.DataKVGroup,
			store.GetCodec(),
			getEncoder(t, tbl),
			encodedRowHandler,
			nil,
			logger,
		))
		t.Cleanup(func() {
			require.NoError(t, dataKVHandler.Close(ctx))
		})
		require.NoError(t, dataKVHandler.Handle(ctx, recordKV))
		require.True(t, handled)
	})

	t.Run("test index kv handler", func(t *testing.T) {
		doTestFn := func(t *testing.T, tableName string, expectedKVs int) {
			tbl := cleanupEnvFn(t, tableName)
			bak := conflictedkv.BufferedHandleLimit
			conflictedkv.BufferedHandleLimit = 2
			t.Cleanup(func() {
				conflictedkv.BufferedHandleLimit = bak
			})
			// we insert those row to make sure the index kv handler can get the data
			// KV from TiKV, it's not possible in real world conflicted KV case.
			tk.MustExec(fmt.Sprintf("insert into %s values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)", tbl.Meta().Name.L))
			encoder := getEncoder(t, tbl)
			require.NoError(t, err)
			var sharedSize atomic.Int64
			alreadyProcessedRowKeys := conflictedkv.NewBoundedKeySet(logger, &sharedSize, 1<<20)
			locallyProcessedRowKeys := conflictedkv.NewBoundedKeySet(logger, &sharedSize, 1<<20)
			alreadyProcessedRowKeys.Add(tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, tidbkv.IntHandle(1)))
			alreadyProcessedRowKeys.Add(tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, tidbkv.IntHandle(3)))

			var (
				rowCnt, kvPairCnt int64
				handledHandles    = make(map[string]struct{})
			)
			mockEncodedKVHdl := mockHandleEncodedRowFn(func(ctx context.Context, rowKey tidbkv.Key, row []types.Datum, kvPairs *kv.Pairs) error {
				handle, err := tablecodec.DecodeRowKey(rowKey)
				require.NoError(t, err)
				require.Equal(t, tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, handle), rowKey)
				require.False(t, alreadyProcessedRowKeys.Contains(rowKey), "should not handle the row keys in the filter set")
				rowCnt++
				kvPairCnt += int64(len(kvPairs.Pairs))
				handledHandles[handle.String()] = struct{}{}
				return nil
			})
			var targetIndexID int64 = 2
			progressCollector := &execute.TestCollector{}
			baseHdl := conflictedkv.NewBaseHandler(
				tbl, globalsort.IndexID2KVGroup(targetIndexID), store.GetCodec(),
				encoder, mockEncodedKVHdl, progressCollector, logger,
			)
			trafficRec := &mockTrafficRecorder{}
			indexKVHdl := conflictedkv.NewIndexKVHandler(
				baseHdl,
				conflictedkv.NewLazyRefreshedSnapshot(store, trafficRec),
				conflictedkv.NewKeyFilter(alreadyProcessedRowKeys, locallyProcessedRowKeys),
			)
			require.NoError(t, indexKVHdl.PreRun())
			var ch = make(chan *simplesst.KVPair, 10)
			eg := util.NewErrorGroupWithRecover()
			eg.Go(func() error {
				defer func() {
					// index kv handler buffer handles, the last batch will be processed
					// on close
					require.NoError(t, indexKVHdl.Close(ctx))
				}()
				return indexKVHdl.Run(ctx, ch)
			})
			eg.Go(func() error {
				// id: 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16
				// uk: 1 1 2 2 3 3 4 4 5  5  6  6  7  7  8  8
				// the first 10 rows have conflicts with existing data, but only the
				// first 5 have corresponding data kvs in TiKV, and only 2/4/5 will
				// be handled in this handler, 1/3 are filtered out.
				for i := range 16 {
					id := i + 1
					ukVal := (id + 1) / 2
					row := []types.Datum{types.NewDatum(id), types.NewDatum(id), types.NewDatum(ukVal)}
					localEncoder := getEncoder(t, tbl)
					dupPairs, err2 := localEncoder.Encode(row, int64(id))
					require.NoError(t, err2)
					for _, pair := range dupPairs.Pairs {
						if tablecodec.IsRecordKey(pair.Key) {
							continue
						}
						indexID, err := tablecodec.DecodeIndexID(pair.Key)
						require.NoError(t, err)
						// only send unique index kv pairs
						if indexID == targetIndexID {
							ch <- &simplesst.KVPair{Key: store.GetCodec().EncodeKey(pair.Key), Value: pair.Val}
						}
					}
				}
				close(ch)
				return nil
			})
			require.NoError(t, eg.Wait())
			require.Greater(t, trafficRec.readBytes.Load(), uint64(0))
			require.EqualValues(t, 3, rowCnt)
			require.EqualValues(t, expectedKVs, kvPairCnt)
			require.EqualValues(t, map[string]struct{}{"2": {}, "4": {}, "5": {}}, handledHandles)
			for _, handle := range []int64{2, 4, 5} {
				rowKey := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, tidbkv.IntHandle(handle))
				require.True(t, locallyProcessedRowKeys.Contains(rowKey))
			}
			for _, handle := range []int64{6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16} {
				rowKey := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, tidbkv.IntHandle(handle))
				require.False(t, locallyProcessedRowKeys.Contains(rowKey))
			}
			require.EqualValues(t, 16, progressCollector.ProcessedCnt.Load())
		}

		t.Run("clustered pk table", func(t *testing.T) {
			doTestFn(t, "tc", 9)
		})
		t.Run("non-clustered pk table", func(t *testing.T) {
			doTestFn(t, "tn", 12)
		})
	})

	t.Run("multi-valued index deduplicates a row across batches", func(t *testing.T) {
		// Regression test for https://github.com/pingcap/tidb/issues/69799.
		tk.MustExec("drop table if exists tmv")
		tk.MustExec(`create table tmv(
			pk bigint primary key clustered,
			a json not null,
			unique key uk_a ((cast(a->'$' as unsigned array)))
		)`)
		tk.MustExec(`insert into tmv values (1, '[1000, 2000]')`)
		tbl, err := do.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("tmv"))
		require.NoError(t, err)

		bak := conflictedkv.BufferedHandleLimit
		conflictedkv.BufferedHandleLimit = 2
		t.Cleanup(func() {
			conflictedkv.BufferedHandleLimit = bak
		})

		var sharedSize atomic.Int64
		globalSet := conflictedkv.NewBoundedKeySet(logger, &sharedSize, 1<<20)
		localSet := conflictedkv.NewBoundedKeySet(logger, &sharedSize, 1<<20)
		var handledRowKeys []tidbkv.Key
		mockEncodedKVHdl := mockHandleEncodedRowFn(func(_ context.Context, rowKey tidbkv.Key, _ []types.Datum, _ *kv.Pairs) error {
			handledRowKeys = append(handledRowKeys, rowKey.Clone())
			return nil
		})

		targetIdx := tbl.Meta().Indices[0]
		progressCollector := &execute.TestCollector{}
		indexKVHdl := conflictedkv.NewIndexKVHandler(
			conflictedkv.NewBaseHandler(
				tbl,
				globalsort.IndexID2KVGroup(targetIdx.ID),
				store.GetCodec(),
				getEncoder(t, tbl),
				mockEncodedKVHdl,
				progressCollector,
				logger,
			),
			conflictedkv.NewLazyRefreshedSnapshot(store, nil),
			conflictedkv.NewKeyFilter(globalSet, localSet),
		)
		require.NoError(t, indexKVHdl.PreRun())

		fixtureEncoder := getEncoder(t, tbl)
		t.Cleanup(func() {
			require.NoError(t, fixtureEncoder.Close())
		})
		indexKVsForRow := func(handle int64, jsonText string) []*simplesst.KVPair {
			jsonValue, err := types.ParseBinaryJSONFromString(jsonText)
			require.NoError(t, err)
			pairs, err := fixtureEncoder.Encode(
				[]types.Datum{types.NewIntDatum(handle), types.NewJSONDatum(jsonValue)},
				handle,
			)
			require.NoError(t, err)
			indexKVs := make([]*simplesst.KVPair, 0, 2)
			for _, pair := range pairs.Pairs {
				if tablecodec.IsRecordKey(pair.Key) {
					continue
				}
				indexID, err := tablecodec.DecodeIndexID(pair.Key)
				require.NoError(t, err)
				if indexID == targetIdx.ID {
					indexKVs = append(indexKVs, &simplesst.KVPair{
						Key:   store.GetCodec().EncodeKey(pair.Key),
						Value: pair.Val,
					})
				}
			}
			return indexKVs
		}

		row1KVs := indexKVsForRow(1, "[1000, 2000]")
		row2KVs := indexKVsForRow(2, "[1000]")
		row3KVs := indexKVsForRow(3, "[2000]")
		require.Len(t, row1KVs, 2)
		require.Len(t, row2KVs, 1)
		require.Len(t, row3KVs, 1)

		ch := make(chan *simplesst.KVPair, 4)
		// Handle 1 appears in two different flushes:
		// [handle 1, handle 2], then [handle 3, handle 1].
		ch <- row1KVs[0]
		ch <- row2KVs[0]
		ch <- row3KVs[0]
		ch <- row1KVs[1]
		close(ch)

		require.NoError(t, indexKVHdl.Run(ctx, ch))
		require.NoError(t, indexKVHdl.Close(ctx))
		expectedRowKey := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, tidbkv.IntHandle(1))
		require.Equal(t, []tidbkv.Key{expectedRowKey}, handledRowKeys)
		require.True(t, localSet.Contains(expectedRowKey))
		require.EqualValues(t, 4, progressCollector.ProcessedCnt.Load())
	})

	t.Run("index row is marked local only after successful handling", func(t *testing.T) {
		tbl := cleanupEnvFn(t, "tc")
		tk.MustExec("insert into tc values (1,1,1)")

		bak := conflictedkv.BufferedHandleLimit
		conflictedkv.BufferedHandleLimit = 1
		t.Cleanup(func() {
			conflictedkv.BufferedHandleLimit = bak
		})

		var sharedSize atomic.Int64
		globalSet := conflictedkv.NewBoundedKeySet(logger, &sharedSize, 1<<20)
		localSet := conflictedkv.NewBoundedKeySet(logger, &sharedSize, 1<<20)
		handleErr := errors.New("handle row")
		handleAttempts := 0
		mockEncodedKVHdl := mockHandleEncodedRowFn(func(_ context.Context, _ tidbkv.Key, _ []types.Datum, _ *kv.Pairs) error {
			handleAttempts++
			if handleAttempts == 1 {
				return handleErr
			}
			return nil
		})

		targetIdx := tbl.Meta().Indices[1]
		indexKVHdl := conflictedkv.NewIndexKVHandler(
			conflictedkv.NewBaseHandler(
				tbl,
				globalsort.IndexID2KVGroup(targetIdx.ID),
				store.GetCodec(),
				getEncoder(t, tbl),
				mockEncodedKVHdl,
				nil,
				logger,
			),
			conflictedkv.NewLazyRefreshedSnapshot(store, nil),
			conflictedkv.NewKeyFilter(globalSet, localSet),
		)
		require.NoError(t, indexKVHdl.PreRun())

		fixtureEncoder := getEncoder(t, tbl)
		t.Cleanup(func() {
			require.NoError(t, fixtureEncoder.Close())
		})
		pairs, err := fixtureEncoder.Encode(
			[]types.Datum{types.NewIntDatum(1), types.NewIntDatum(1), types.NewIntDatum(1)},
			1,
		)
		require.NoError(t, err)
		var indexKV *simplesst.KVPair
		for _, pair := range pairs.Pairs {
			if tablecodec.IsRecordKey(pair.Key) {
				continue
			}
			indexID, err := tablecodec.DecodeIndexID(pair.Key)
			require.NoError(t, err)
			if indexID == targetIdx.ID {
				indexKV = &simplesst.KVPair{
					Key:   store.GetCodec().EncodeKey(pair.Key),
					Value: pair.Val,
				}
				break
			}
		}
		require.NotNil(t, indexKV)

		expectedRowKey := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, tidbkv.IntHandle(1))
		err = indexKVHdl.Handle(ctx, indexKV)
		require.ErrorIs(t, err, handleErr)
		require.False(t, localSet.Contains(expectedRowKey))

		// The failed row remains buffered and Close retries it.
		require.NoError(t, indexKVHdl.Close(ctx))
		require.Equal(t, 2, handleAttempts)
		require.True(t, localSet.Contains(expectedRowKey))
	})
}
