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
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table/tables/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func findIndex(t *testing.T, dom *domain.Domain, tableName string, indexName string) *model.IndexInfo {
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr(tableName))
	require.NoError(t, err)

	idx := tbl.Meta().FindIndexByName(indexName)
	return idx
}

func validatePartialIndexExists(t *testing.T, dom *domain.Domain, tableName string, indexName string) {
	idx := findIndex(t, dom, tableName, indexName)
	require.NotNil(t, idx)
	require.NotEmpty(t, idx.ConditionExprString)
}

func TestPartialIndexDDL(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)

	t.Run("TestCreatePartialIndex", func(t *testing.T) {
		tk.MustExec("use test")
		tk.MustExec("create table t1 (col1 int primary key, col2 int, key idx(col2) where col1 > 100);")
		defer tk.MustExec("drop table if exists t1;")
		validatePartialIndexExists(t, dom, "t1", "idx")

		tk.MustExec("create table t2 (col1 int primary key, col2 int);")
		defer tk.MustExec("drop table if exists t2;")
		tk.MustExec("create index idx on t2(col2) where col1 > 100;")
		validatePartialIndexExists(t, dom, "t2", "idx")

		tk.MustExec("create table t3 (col1 int primary key, col2 int);")
		defer tk.MustExec("drop table if exists t3;")
		tk.MustExec("alter table t3 add index idx(col2) where col1 > 100;")
		validatePartialIndexExists(t, dom, "t3", "idx")

		tk.MustExec("create table t4 like t1")
		defer tk.MustExec("drop table if exists t4;")
		validatePartialIndexExists(t, dom, "t4", "idx")
	})

	t.Run("TestValidationInPartialIndex", func(t *testing.T) {
		tk.MustExec("use test")
		tk.MustExec("create table t(col1 int primary key, col2 int, col3 varchar(255), col4 int as (col2 + 1));")
		defer tk.MustExec("drop table if exists t;")

		type testCase struct {
			idxDef    string
			errorCode int
		}
		testCases := []testCase{
			{"t(col2) where col2 = 1;", 0},
			{"t(col2) where col1 != 1;", 0},
			{"t(col2) where col1 > 1;", 0},
			{"t(col2) where col1 IS NULL;", 0},
			{"t(col2) where col1 IS NOT NULL;", 0},
			{"t(col2) where col1 IN (1,2,3,4,5);", 8200},
			{"t(col2) where col1 LIKE '1%';", 8200},
			{"t(col2) where col1 > col2;", 8200},
			{"t(col2) where col1 = NOW();", 8200},
			{"t(col2) where col1 = (select 1);", 8200},
			{"t(col2) where col4 = 4;", 8200},
		}
		for _, tc := range testCases {
			t.Run(tc.idxDef, func(t *testing.T) {
				sql := "create index idx on " + tc.idxDef

				if tc.errorCode != 0 {
					tk.MustGetErrCode(sql, tc.errorCode)
					return
				}

				tk.MustExec(sql)
				validatePartialIndexExists(t, dom, "t", "idx")
				tk.MustExec("drop index idx on t;")
			})
		}
	})

	t.Run("TestIndexManagementForPartialIndex", func(t *testing.T) {
		tk.MustExec("use test")
		tk.MustExec("create table t(col1 int primary key, col2 int, col3 int);")
		defer tk.MustExec("drop table if exists t;")

		tk.MustExec("create index idx on t(col3) where col2 = 1;")
		validatePartialIndexExists(t, dom, "t", "idx")
		tk.MustExec("alter table t rename index idx to idx2;")
		validatePartialIndexExists(t, dom, "t", "idx2")

		tk.MustExec("alter table t change column col3 col4 int;")
		validatePartialIndexExists(t, dom, "t", "idx2")
		idx := findIndex(t, dom, "t", "idx2")
		require.Equal(t, "col4", idx.Columns[0].Name.O)
		tk.MustExec("alter table t modify column col4 int unsigned;")
		validatePartialIndexExists(t, dom, "t", "idx2")
		idx = findIndex(t, dom, "t", "idx2")
		require.Equal(t, "col4", idx.Columns[0].Name.O)
		tk.MustExec("alter table t drop column col4;")
		idx = findIndex(t, dom, "t", "idx2")
		require.Nil(t, idx)

		tk.MustExec("create index idx on t(col2) where col2 = 1;")
		validatePartialIndexExists(t, dom, "t", "idx")
		tk.MustExec("drop index idx on t;")
		idx = findIndex(t, dom, "t", "idx")
		require.Nil(t, idx)
	})

	t.Run("TestManipulateColumnReferencedByPartialIndex", func(t *testing.T) {
		tk.MustExec("use test")
		tk.MustExec("create table t(col1 int primary key, col2 int, col3 int);")

		tk.MustExec("create index idx on t(col3) where col2 = 1;")
		validatePartialIndexExists(t, dom, "t", "idx")

		tk.MustGetDBError("alter table t drop column col2;", dbterror.ErrModifyColumnReferencedByPartialCondition)
		tk.MustGetDBError("alter table t change column col2 col4 int;", dbterror.ErrModifyColumnReferencedByPartialCondition)
		tk.MustGetDBError("alter table t modify column col2 int unsigned;", dbterror.ErrModifyColumnReferencedByPartialCondition)

		tk.MustExec("drop table t;")
		tk.MustExec("create table t(col1 int primary key, col2 int, col3 int, key t(col3) where col2 = 1);")

		tk.MustGetDBError("alter table t drop column col2;", dbterror.ErrModifyColumnReferencedByPartialCondition)
		tk.MustGetDBError("alter table t change column col2 col4 int;", dbterror.ErrModifyColumnReferencedByPartialCondition)
		tk.MustGetDBError("alter table t modify column col2 int unsigned;", dbterror.ErrModifyColumnReferencedByPartialCondition)
		tk.MustExec("drop table t;")
	})

	t.Run("TestPartialIndexCanOnlyBeCreatedWithFastReorg", func(t *testing.T) {
		tk.MustExec("use test;")
		tk.MustExec("create table t (a int, b int, c int, primary key (a))")
		defer tk.MustExec("drop table if exists t;")
		tk.MustExec("insert into t(a, b, c) values (1, 2, 3), (2, 3, 4), (3, 4, 5)")

		// Disable fast-reorg
		tk.MustExec("set global tidb_ddl_enable_fast_reorg = 0")
		tk.MustGetDBError("alter table t add index idx1(a) where c > 3", dbterror.ErrUnsupportedAddPartialIndex)
		tk.MustExec("set global tidb_ddl_enable_fast_reorg = 1")

		tk.MustExec("alter table t add index idx0(a)")
		testutil.CheckIndexKVCount(t, tk, dom, "t", "idx0", 3)
		jobs := tk.MustQuery("admin show ddl jobs 1").Rows()
		require.Equal(t, jobs[0][7], "3")
	})

	t.Run("TestAddPartialIndex", func(t *testing.T) {
		tk.MustExec("use test;")
		tk.MustExec("create table t (a int, b int, c int, primary key (a))")
		defer tk.MustExec("drop table if exists t;")
		tk.MustExec("insert into t(a, b, c) values (1, 2, 3), (2, 3, 4), (3, 4, 5)")

		// Partial index with proper condition
		tk.MustExec("alter table t add index idx1(a) where c >= 5")
		testutil.CheckIndexKVCount(t, tk, dom, "t", "idx1", 1)
		jobs := tk.MustQuery("admin show ddl jobs 1").Rows()
		require.Equal(t, jobs[0][7], "3")

		// Partial index with condition that no row meets
		tk.MustExec("alter table t add index idx2(a) where c >= 6")
		testutil.CheckIndexKVCount(t, tk, dom, "t", "idx2", 0)
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

		// Multi-schema change add multiple indexes, including partial index and normal index
		tk.MustExec("alter table t add index idx8(a), add index idx9(a) where c >= 5")
		testutil.CheckIndexKVCount(t, tk, dom, "t", "idx8", 3)
		testutil.CheckIndexKVCount(t, tk, dom, "t", "idx9", 1)
		jobs = tk.MustQuery("admin show ddl jobs 1").Rows()
		require.Equal(t, jobs[1][7], "3")

		// Create index on table with `_tidb_rowid` column
		tk.MustExec("create table t1 (a int, b int, c int)")
		tk.MustExec("insert into t1(a, b, c) values (1, 2, 3), (2, 3, 4), (3, 4, 5)")
		tk.MustExec("alter table t1 add index idx1(a) where a > 1")
		testutil.CheckIndexKVCount(t, tk, dom, "t1", "idx1", 2)

		// Create normal index, the row count is still correct.
		tk.MustExec("alter table t1 add index idx2(a)")
		testutil.CheckIndexKVCount(t, tk, dom, "t1", "idx2", 3)
		jobs = tk.MustQuery("admin show ddl jobs 1").Rows()
		require.Equal(t, jobs[0][7], "3")
	})

	t.Run("TestValidateColumnExistsInAddIndex", func(t *testing.T) {
		tk.MustExec("use test;")
		tk.MustExec("create table t (a int, b int);")
		defer tk.MustExec("drop table if exists t;")
		tk.MustExec("alter table t add index idx_b(b) where a = 1;")
		tk.MustGetDBError("alter table t add index idx_b_2(b) where c = 1;",
			dbterror.ErrUnsupportedAddPartialIndex)
	})
}
