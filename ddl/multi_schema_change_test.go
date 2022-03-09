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

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestMultiSchemaChangeAddColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("alter table t add column b int default 2, add column c int default 3;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))
}

func TestMultiSchemaDropColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	tk.MustExec("create table t (a int, b int)")
	_, err := tk.Exec("alter table t drop column a, drop column b")
	require.Equal(t, err.Error(), "[ddl:1113]A table must have at least 1 column")
}

func TestMultiSchemaChangeOperateSameColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	tk.MustExec("create table t (a int, c int)")
	_, err := tk.Exec("alter table t add column b int default 2, add column b int default 3")
	require.Equal(t, err.Error(), "[ddl:8200]Unsupported operate same column 'b'")
	_, err = tk.Exec("alter table t drop column a, drop column a")
	require.Equal(t, err.Error(), "[ddl:8200]Unsupported operate same column 'a'")
}
