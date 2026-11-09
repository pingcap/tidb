// Copyright 2024 PingCAP, Inc.
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

package util_test

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

type partitionItemLookupForbiddenInfoSchema struct {
	infoschema.InfoSchema
	t *testing.T
}

func (is partitionItemLookupForbiddenInfoSchema) TableItemByPartitionID(partitionID int64) (infoschema.TableItem, bool) {
	is.t.Fatalf("TableItemByPartitionID should not be called for partition ID %d", partitionID)
	return infoschema.TableItem{}, false
}

func (is partitionItemLookupForbiddenInfoSchema) TableIDByPartitionID(partitionID int64) (int64, bool) {
	is.t.Fatalf("TableIDByPartitionID should not be called for partition ID %d", partitionID)
	return 0, false
}

func TestIsSpecialGlobalIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(
		"create table t(" +
			"	a int," +
			"	b int," +
			"	c int," +
			"	d varchar(20)," +
			"	unique index b(b) global," +
			"	index c(c)," +
			"	unique index ub_s((b+1)) global," +
			"	unique index ud_s(d(3)) global," +
			"	index b_s((b+1))," +
			"	index d_s(d(3))" +
			") partition by hash(a) partitions 5")

	tblInfo := dom.MustGetTableInfo(t, "test", "t")
	cnt := 0
	for _, idx := range tblInfo.Indices {
		switch idx.Name.O {
		case "b", "c", "b_s", "d_s":
			cnt++
			require.False(t, util.IsSpecialGlobalIndex(idx, tblInfo))
		case "ub_s", "ud_s":
			cnt++
			require.True(t, util.IsSpecialGlobalIndex(idx, tblInfo))
		}
	}
	require.Equal(t, cnt, len(tblInfo.Indices))
}

func TestCallSCtxFailed(t *testing.T) {
	_, dom := testkit.CreateMockStoreAndDomain(t)

	var sctxWithFailure sessionctx.Context
	err := util.CallWithSCtx(dom.StatsHandle().SPool(), func(sctx sessionctx.Context) error {
		sctxWithFailure = sctx
		return errors.New("simulated error")
	})
	require.Error(t, err)
	require.Equal(t, "simulated error", err.Error())
	notReleased := infosync.ContainsInternalSessionForTest(sctxWithFailure)
	require.False(t, notReleased)
}

func TestSnapshotTableInfoGetterAvoidsV1PartitionScan(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table normal (a int)")
	tk.MustExec("create table partitioned (a int) partition by hash(a) partitions 2")

	normalInfo := dom.MustGetTableInfo(t, "test", "normal")
	partitionedInfo := dom.MustGetTableInfo(t, "test", "partitioned")
	partitionInfo := partitionedInfo.GetPartitionInfo()
	require.NotNil(t, partitionInfo)
	require.NotEmpty(t, partitionInfo.Definitions)
	partitionID := partitionInfo.Definitions[0].ID

	is := partitionItemLookupForbiddenInfoSchema{
		InfoSchema: infoschema.MockInfoSchemaWithSchemaVer([]*model.TableInfo{normalInfo, partitionedInfo}, 1),
		t:          t,
	}
	snapshotGetter := util.NewSnapshotTableInfoGetter(is)
	tbl, ok := snapshotGetter.TableInfoByID(normalInfo.ID)
	require.True(t, ok)
	require.Equal(t, normalInfo.ID, tbl.Meta().ID)
	tbl, ok = snapshotGetter.TableInfoByID(partitionID)
	require.True(t, ok)
	require.Equal(t, partitionedInfo.ID, tbl.Meta().ID)
	_, ok = snapshotGetter.TableInfoByID(1 << 60)
	require.False(t, ok)
	item, ok := snapshotGetter.TableItemByID(normalInfo.ID)
	require.True(t, ok)
	require.Equal(t, "normal", item.TableName.L)
	item, ok = snapshotGetter.TableItemByID(partitionID)
	require.True(t, ok)
	require.Equal(t, "partitioned", item.TableName.L)
	_, ok = snapshotGetter.TableItemByID(1 << 60)
	require.False(t, ok)
	_, ok = snapshotGetter.ParentTableID(normalInfo.ID)
	require.False(t, ok)
	tableID, ok := snapshotGetter.ParentTableID(partitionID)
	require.True(t, ok)
	require.Equal(t, partitionedInfo.ID, tableID)
	_, ok = snapshotGetter.ParentTableID(1 << 60)
	require.False(t, ok)
}
