// Copyright 2021 PingCAP, Inc.
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

package join_test

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestIssue18068(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/testIssue18068", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/testIssue18068"))
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, s")
	tk.MustExec("create table t (a int, index idx(a))")
	tk.MustExec("create table s (a int, index idx(a))")
	tk.MustExec("insert into t values(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1);")
	tk.MustExec("insert into s values(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1);")
	tk.MustExec("set @@tidb_index_join_batch_size=1")
	tk.MustExec("set @@tidb_max_chunk_size=32")
	tk.MustExec("set @@tidb_init_chunk_size=1")
	tk.MustExec("set @@tidb_index_lookup_join_concurrency=2")

	tk.MustExec("select  /*+ inl_merge_join(s)*/ 1 from t join s on t.a = s.a limit 1")
	// Do not hang in index merge join when the second and third execute.
	tk.MustExec("select  /*+ inl_merge_join(s)*/ 1 from t join s on t.a = s.a limit 1")
	tk.MustExec("select  /*+ inl_merge_join(s)*/ 1 from t join s on t.a = s.a limit 1")
}

func TestIssue54064(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists A, B")
	tk.MustExec("create table A\n(id int primary key nonclustered auto_increment,\nx varchar(32) not null,\ny char(5) not null,\nz varchar(25) not null,\nkey idx_sub_tsk(z,x,y)\n)")
	tk.MustExec("create table B\n( y char(5) not null,\nz varchar(25) not null,\nx varchar(32) not null,\nprimary key(z, x, y) nonclustered\n)\n")
	tk.MustExec("insert into A (y, z, x) values\n('CN000', '123', 'RW '),\n('CN000', '456', '123');")
	tk.MustExec("insert into B values\n('CN000', '123', 'RW '),\n('CN000', '456', '123');")
	tk.MustQuery("select /*+ inl_merge_join(a, b) */\na.*\nfrom a join b on a.y=b.y and a.z=b.z and a.x = b.x\nwhere a.y='CN000' order by 1,2;").Check(
		testkit.Rows("1 RW  CN000 123", "2 123 CN000 456"))
	res := tk.MustQuery("explain format='brief' select /*+ inl_merge_join(a, b) */\na.*\nfrom a join b on a.y=b.y and a.z=b.z and a.x = b.x\nwhere a.y='CN000' order by 1,2;")
	require.NotRegexp(t, "IndexMergeJoin_.*", res.Rows()[0][0])
}
