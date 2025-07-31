// Copyright 2025 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/table/tables/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestPartialIndexDDL(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("create table t (a int, b int, c int, d int as (c + 1) virtual, e int as (c + 1) stored, primary key (a))")
	tk.MustExec("insert into t(a, b, c) values (1, 2, 3), (2, 3, 4), (3, 4, 5)")

	// Disable fast-reorg
	tk.MustExec("set global tidb_ddl_enable_fast_reorg = 0")
	tk.MustGetDBError("alter table t add index idx1(a) where c > 3", dbterror.ErrUnsupportedAddPartialIndex)
	tk.MustExec("set global tidb_ddl_enable_fast_reorg = 1")

	tk.MustExec("alter table t add index idx0(a)")
	testutil.CheckIndexKVCount(t, tk, dom, "t", "idx0", 3)
	jobs := tk.MustQuery("admin show ddl jobs 1").Rows()
	require.Equal(t, jobs[0][7], "3")

	// Partial index with proper condition
	tk.MustExec("alter table t add index idx1(a) where c >= 5")
	testutil.CheckIndexKVCount(t, tk, dom, "t", "idx1", 1)
	jobs = tk.MustQuery("admin show ddl jobs 1").Rows()
	require.Equal(t, jobs[0][7], "3")

	// Partial index with condition that no row meets
	tk.MustExec("alter table t add index idx2(a) where c >= 6")
	testutil.CheckIndexKVCount(t, tk, dom, "t", "idx2", 0)
	jobs = tk.MustQuery("admin show ddl jobs 1").Rows()
	require.Equal(t, jobs[0][7], "3")

	// Partial index with virtual generated columns
	tk.MustExec("alter table t add index idx3(a) where d >= 5")
	testutil.CheckIndexKVCount(t, tk, dom, "t", "idx3", 2)
	jobs = tk.MustQuery("admin show ddl jobs 1").Rows()
	require.Equal(t, jobs[0][7], "3")

	// Partial index with stored generated columns
	tk.MustExec("alter table t add index idx4(a) where e >= 4")
	testutil.CheckIndexKVCount(t, tk, dom, "t", "idx4", 3)
	jobs = tk.MustQuery("admin show ddl jobs 1").Rows()
	require.Equal(t, jobs[0][7], "3")

	// Multi-schema change add multiple partial indexes
	tk.MustExec("alter table t add index idx5(a) where c >= 4, add index idx6(a) where c >= 5")
	testutil.CheckIndexKVCount(t, tk, dom, "t", "idx5", 2)
	testutil.CheckIndexKVCount(t, tk, dom, "t", "idx6", 1)
	jobs = tk.MustQuery("admin show ddl jobs 1").Rows()
	require.Equal(t, jobs[1][7], "3")

	// Pushdown is disabled for `not` function, so the DDL cannot pushdown
	tk.MustExec("INSERT INTO mysql.expr_pushdown_blacklist VALUES('not','tikv','');")
	tk.MustExec("ADMIN reload expr_pushdown_blacklist;")
	tk.MustExec("alter table t add index idx7(a) where b is not null;")
	testutil.CheckIndexKVCount(t, tk, dom, "t", "idx7", 3)
	tk.MustExec("DELETE FROM mysql.expr_pushdown_blacklist WHERE name='not' AND store_type='tikv';")

	// Create index on table with `_tidb_rowid` column
	tk.MustExec("create table t1 (a int, b int, c int)")
	tk.MustExec("insert into t1(a, b, c) values (1, 2, 3), (2, 3, 4), (3, 4, 5)")
	tk.MustExec("alter table t1 add index idx1(a) where a > 1")
	testutil.CheckIndexKVCount(t, tk, dom, "t1", "idx1", 2)

	tk.MustExec("drop table t")
}
