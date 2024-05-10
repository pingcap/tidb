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

package addindextest

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestDDLTestEstimateTableRowSize(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1, 1);")

	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "estimate_row_size")
	tkSess := tk.Session()
	exec := tkSess.GetRestrictedSQLExecutor()
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	size := ddl.EstimateTableRowSizeForTest(ctx, store, exec, tbl)
	require.Equal(t, 0, size) // No data in information_schema.columns.
	tk.MustExec("analyze table t;")
	size = ddl.EstimateTableRowSizeForTest(ctx, store, exec, tbl)
	require.Equal(t, 16, size)

	tk.MustExec("alter table t add column c varchar(255);")
	tk.MustExec("update t set c = repeat('a', 50) where a = 1;")
	tk.MustExec("analyze table t;")
	size = ddl.EstimateTableRowSizeForTest(ctx, store, exec, tbl)
	require.Equal(t, 67, size)

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (id bigint primary key, b text) partition by hash(id) partitions 4;")
	for i := 1; i < 10; i++ {
		insertSQL := fmt.Sprintf("insert into t values (%d, repeat('a', 10))", i*10000)
		tk.MustExec(insertSQL)
	}
	tk.MustQuery("split table t between (0) and (1000000) regions 2;").Check(testkit.Rows("4 1"))
	tk.MustExec("set global tidb_analyze_skip_column_types=`json,blob,mediumblob,longblob`")
	tk.MustExec("analyze table t;")
	tbl, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	size = ddl.EstimateTableRowSizeForTest(ctx, store, exec, tbl)
	require.Equal(t, 19, size)
	ptbl := tbl.GetPartitionedTable()
	pids := ptbl.GetAllPartitionIDs()
	for _, pid := range pids {
		partition := ptbl.GetPartition(pid)
		size = ddl.EstimateTableRowSizeForTest(ctx, store, exec, partition)
		require.Equal(t, 19, size)
	}
}
