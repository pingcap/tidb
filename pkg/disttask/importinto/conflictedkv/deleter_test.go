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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/importinto/conflictedkv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
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

func TestDeleter(t *testing.T) {
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
	tableName := "tc"

	cleanUpEnvFn := func(t *testing.T) table.Table {
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		tk2.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
		tk2.MustExec("create table tc(a bigint primary key clustered, b int, c int, index(b), unique(c))")
		tk2.MustExec("insert into tc values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10)")
		tk2.MustQuery("select * from tc").Sort().Equal(testkit.Rows(
			"1 1 1", "2 2 2", "3 3 3", "4 4 4", "5 5 5",
			"6 6 6", "7 7 7", "8 8 8", "9 9 9", "10 10 10",
		))
		tk2.MustExec("admin check table tc")
		tbl, err := do.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr(tableName))
		require.NoError(t, err)
		return tbl
	}

	gatherTargetKVFn := func(t *testing.T, tbl table.Table, kvGroup string, endID int) []external.KVPair {
		targetKVs := make([]external.KVPair, 0, endID)
		localEncoder := getEncoder(t, tbl)
		for i := range endID {
			dupID := i + 1
			row := []types.Datum{types.NewDatum(dupID), types.NewDatum(dupID), types.NewDatum(dupID)}
			dupPairs, err2 := localEncoder.Encode(row, int64(dupID))
			require.NoError(t, err2)
			for _, pair := range dupPairs.Pairs {
				if kvGroup == external.DataKVGroup {
					if tablecodec.IsRecordKey(pair.Key) {
						targetKVs = append(targetKVs, external.KVPair{Key: bytes.Clone(pair.Key), Value: bytes.Clone(pair.Val)})
					}
				} else {
					indexID, err2 := external.KVGroup2IndexID(kvGroup)
					require.NoError(t, err2)
					if !tablecodec.IsRecordKey(pair.Key) {
						gotID, err2 := tablecodec.DecodeIndexID(pair.Key)
						require.NoError(t, err2)
						if gotID == indexID {
							targetKVs = append(targetKVs, external.KVPair{Key: bytes.Clone(pair.Key), Value: bytes.Clone(pair.Val)})
						}
					}
				}
			}
			dupPairs.Clear()
		}
		return targetKVs
	}
	simulateConflictedKVFn := func(t *testing.T, tbl table.Table, kvGroup string, endID int) {
		kvsToDelete := gatherTargetKVFn(t, tbl, kvGroup, endID)
		// remove some KVs to simulate the conflicted KV scenario
		txn, err := store.Begin()
		require.NoError(t, err)
		for _, kv := range kvsToDelete {
			require.NoError(t, txn.Delete(kv.Key))
		}
		require.NoError(t, txn.Commit(ctx))
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		require.ErrorContains(t, tk2.ExecToErr("admin check table tc"), "data inconsistency in table")
	}

	runDeleterFn := func(t *testing.T, kvGroup string, conflictedRowCnt int) {
		tbl := cleanUpEnvFn(t)
		simulateConflictedKVFn(t, tbl, kvGroup, conflictedRowCnt)
		conflictedKVs := gatherTargetKVFn(t, tbl, kvGroup, conflictedRowCnt)

		encoder := getEncoder(t, tbl)
		deleter := conflictedkv.NewDeleter(tbl, logger, store, kvGroup, encoder)
		eg := util.NewErrorGroupWithRecover()
		ch := make(chan *external.KVPair)
		eg.Go(func() error {
			return deleter.Run(ctx, ch)
		})

		eg.Go(func() error {
			rand.Shuffle(len(conflictedKVs), func(i, j int) {
				conflictedKVs[i], conflictedKVs[j] = conflictedKVs[j], conflictedKVs[i]
			})
			for _, kv := range conflictedKVs {
				// sending the conflicted KV twice
				for range 2 {
					ch <- &kv
				}
			}
			close(ch)
			return nil
		})
		require.NoError(t, eg.Wait())
	}

	bak := conflictedkv.BufferedKeyCountLimit
	t.Cleanup(func() {
		conflictedkv.BufferedKeyCountLimit = bak
	})
	conflictedkv.BufferedKeyCountLimit = 2

	t.Run("data kv conflicts", func(t *testing.T) {
		runDeleterFn(t, external.DataKVGroup, 7)
		tk.MustQuery("select * from tc").Sort().Equal(testkit.Rows(
			"8 8 8", "9 9 9", "10 10 10",
		))
		tk.MustExec("admin check table tc")
	})

	t.Run("index kv conflicts", func(t *testing.T) {
		// 2 is the unique index ID for index c
		kvGroup := external.IndexID2KVGroup(2)
		runDeleterFn(t, kvGroup, 5)
		tk.MustQuery("select * from tc").Sort().Equal(testkit.Rows(
			"6 6 6", "7 7 7", "8 8 8", "9 9 9", "10 10 10",
		))
		tk.MustExec("admin check table tc")
	})
}
