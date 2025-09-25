// Copyright 2016 PingCAP, Inc.
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

package executor_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestRefreshTableStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("insert into t1 values (1,1), (2,2), (3,3)")
	tk.MustExec("create table t2 (a int, b int)")
	tk.MustExec("insert into t2 values (1,1), (2,2), (3,3)")
	tk.MustExec("analyze table t1, t2 all columns")

	is := dom.InfoSchema()
	handle := dom.StatsHandle()
	ctx := context.Background()
	tbl1, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tbl2, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	tbl1Meta := tbl1.Meta()
	tbl1Stats := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	tbl2Meta := tbl2.Meta()
	tbl2Stats := handle.GetPhysicalTableStats(tbl2Meta.ID, tbl2Meta)
	tk.MustExec("refresh stats test.t1")
	tbl1StatsUpdated := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	tbl2StatsUpdated := handle.GetPhysicalTableStats(tbl2Meta.ID, tbl2Meta)
	require.NotSame(t, tbl1Stats, tbl1StatsUpdated)
	require.Same(t, tbl2Stats, tbl2StatsUpdated)
	tk.MustExec("REFRESH STATS *.* FULL CLUSTER")
	tbl2StatsUpdated = handle.GetPhysicalTableStats(tbl2Meta.ID, tbl2Meta)
	require.NotSame(t, tbl2Stats, tbl2StatsUpdated)
}
