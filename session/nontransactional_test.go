// Copyright 2022 PingCAP, Inc.
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

package session_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestNonTransactionalDelete(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")

	tables := []string{
		"create table t(a int, b int, primary key(a, b) clustered)",
		"create table t(a int, b int, primary key(a, b) nonclustered)",
		"create table t(a int, b int, primary key(a) clustered)",
		"create table t(a int, b int, primary key(a) nonclustered)",
		"create table t(a varchar(30), b int, primary key(a, b) clustered)",
		"create table t(a varchar(30), b int, primary key(a, b) nonclustered)",
		"create table t(a varchar(30), b int, primary key(a) clustered)",
		"create table t(a varchar(30), b int, primary key(a) nonclustered)",
	}
	for _, table := range tables {
		tk.MustExec("drop table if exists t")
		tk.MustExec(table)
		for i := 0; i < 100; i++ {
			tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		}
		tk.MustExec("split on a limit 3 delete from t")
		tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))

		for i := 0; i < 100; i++ {
			tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		}
		if strings.Contains(table, "a int") {
			rows := tk.MustQuery("split on a limit 3 dry run delete from t").Rows()
			for _, row := range rows {
				require.True(t, strings.HasPrefix(row[0].(string), "DELETE FROM `test`.`t` WHERE `a` BETWEEN"))
			}
		}
		tk.MustQuery("split on a limit 3 dry run query delete from t").Check(testkit.Rows(
			"select `a` from `test`.`t` where true order by IF(ISNULL(`a`),0,1),`a`"))
	}
}

func TestNonTransactionalDeleteErrorMessage(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, primary key(a, b) clustered)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	failpoint.Enable("github.com/pingcap/tidb/session/splitDeleteError", `return`)
	defer failpoint.Disable("github.com/pingcap/tidb/session/splitDeleteError")
	rows := tk.MustQuery("split on a limit 3 delete from t").Rows()
	require.Equal(t, 1, len(rows))
	require.Equal(t, rows[0][2].(string), "Early return: error occurred in the first job: injected split delete error")
}

func TestNonTransactionalDeleteSplitOnTiDBRowID(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	tk.MustExec("split on _tidb_rowid limit 3 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteNull(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		tk.MustExec("insert into t values (null, null)")
	}

	tk.MustExec("split on a limit 3 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))

	// all values are null
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (null, null)")
	}
	tk.MustExec("split on a limit 3 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteSmallBatch(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=1024")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, key(a))")
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		tk.MustExec("insert into t values (null, null)")
	}
	require.Equal(t, 1, len(tk.MustQuery("split on a limit 1000 dry run delete from t").Rows()))
	tk.MustExec("split on a limit 1000 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteShardOnGeneratedColumn(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_max_chunk_size=35")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c double as (sqrt(a * a + b * b)), key(c))")
	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d, default)", i, i*2))
	}
	tk.MustExec("split on c limit 10 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}
