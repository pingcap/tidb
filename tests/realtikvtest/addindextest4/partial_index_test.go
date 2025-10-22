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
	"github.com/pingcap/tidb/pkg/testkit"
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
}
