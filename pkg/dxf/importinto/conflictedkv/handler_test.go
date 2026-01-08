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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/importinto/conflictedkv"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
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

type mockHandleEncodedRowFn func(ctx context.Context, handle tidbkv.Handle, row []types.Datum, kvPairs *kv.Pairs) error

func (h mockHandleEncodedRowFn) HandleEncodedRow(ctx context.Context, handle tidbkv.Handle, row []types.Datum, kvPairs *kv.Pairs) error {
	return h(ctx, handle, row, kvPairs)
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
	if kerneltype.IsNextGen() {
		t.Skip("skip test for next-gen kernel temporarily, we need to adapt the test later")
	}
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
			mockEncodedKVHdl := mockHandleEncodedRowFn(func(ctx context.Context, handle tidbkv.Handle, row []types.Datum, kvPairs *kv.Pairs) error {
				rowCnt++
				kvPairCnt += int64(len(kvPairs.Pairs))
				return nil
			})
			baseHdl := conflictedkv.NewBaseHandler(tbl, external.DataKVGroup, encoder, mockEncodedKVHdl, logger)
			dataKVHdl := conflictedkv.NewDataKVHandler(baseHdl)
			t.Cleanup(func() {
				require.NoError(t, dataKVHdl.Close(ctx))
			})
			require.NoError(t, dataKVHdl.PreRun())
			var ch = make(chan *external.KVPair, 10)
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
						ch <- &external.KVPair{Key: pair.Key, Value: pair.Val}
					}
				}
				close(ch)
				return nil
			})
			require.NoError(t, eg.Wait())
			require.EqualValues(t, 10, rowCnt)
			require.EqualValues(t, expectedKVs, kvPairCnt)
		}

		t.Run("clustered pk table", func(t *testing.T) {
			doTestFn(t, "tc", 30)
		})
		t.Run("non-clustered pk table", func(t *testing.T) {
			doTestFn(t, "tn", 40)
		})
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
			alreadyProcessedHandles := conflictedkv.NewBoundedHandleSet(logger, &sharedSize, 1<<20)
			alreadyProcessedHandles.Add(tidbkv.IntHandle(1))
			alreadyProcessedHandles.Add(tidbkv.IntHandle(3))

			var (
				rowCnt, kvPairCnt int64
				handledHandles    = make(map[string]struct{})
			)
			mockEncodedKVHdl := mockHandleEncodedRowFn(func(ctx context.Context, handle tidbkv.Handle, row []types.Datum, kvPairs *kv.Pairs) error {
				require.False(t, alreadyProcessedHandles.Contains(handle), "should not handle the handles in the filter set")
				rowCnt++
				kvPairCnt += int64(len(kvPairs.Pairs))
				handledHandles[handle.String()] = struct{}{}
				return nil
			})
			var targetIndexID int64 = 2
			baseHdl := conflictedkv.NewBaseHandler(tbl, external.IndexID2KVGroup(targetIndexID), encoder, mockEncodedKVHdl, logger)
			indexKVHdl := conflictedkv.NewIndexKVHandler(
				baseHdl,
				conflictedkv.NewLazyRefreshedSnapshot(store),
				conflictedkv.NewHandleFilter(alreadyProcessedHandles),
			)
			require.NoError(t, indexKVHdl.PreRun())
			var ch = make(chan *external.KVPair, 10)
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
							ch <- &external.KVPair{Key: pair.Key, Value: pair.Val}
						}
					}
				}
				close(ch)
				return nil
			})
			require.NoError(t, eg.Wait())
			require.EqualValues(t, 3, rowCnt)
			require.EqualValues(t, expectedKVs, kvPairCnt)
			require.EqualValues(t, map[string]struct{}{"2": {}, "4": {}, "5": {}}, handledHandles)
		}

		t.Run("clustered pk table", func(t *testing.T) {
			doTestFn(t, "tc", 9)
		})
		t.Run("non-clustered pk table", func(t *testing.T) {
			doTestFn(t, "tn", 12)
		})
	})
}
