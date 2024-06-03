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

func TestIndexLookupMergeJoinHang(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/IndexMergeJoinMockOOM", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/IndexMergeJoinMockOOM"))
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int,b int,index idx(a))")
	tk.MustExec("create table t2 (a int,b int,index idx(a))")
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3),(2000,2000)")
	tk.MustExec("insert into t2 values (1,1),(2,2),(3,3),(2000,2000)")
	// Do not hang in index merge join when OOM occurs.
	err := tk.QueryToErr("select /*+ INL_MERGE_JOIN(t1, t2) */ * from t1, t2 where t1.a = t2.a")
	require.Error(t, err)
	require.Equal(t, "OOM test index merge join doesn't hang here.", err.Error())
}

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
