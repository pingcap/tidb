// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package explain

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestIssue53867(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t_xf1at0 (c_weg int ,c_u text ,c_bu double ,c__icnfdo_ tinyint ,c_micv4b_95 text ,c_ngdu3 int not null ,c_curc3h double ,c_menn1dk double ,c_pv int unique ,c_u3zry tinyint ,primary key(c_ngdu3, c_pv) NONCLUSTERED) shard_row_id_bits=4 pre_split_regions=2;")
	tk.MustExec("create index t_kp1_idx_1 on t_xf1at0 (c_weg, c_ngdu3, c_pv);")
	tk.MustExec("insert into t_xf1at0 (c_weg, c_ngdu3, c_pv) values (0, 0, 0), (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 6, 6), (7, 7, 7), (8, 8, 8), (9, 9, 9), (10, 10, 10);")
	tk.MustExec("create table t_bhze93f (c_x393ej_ int ,c_k3kss19 double not null ,c_q6qt9_ int not null ,c_wp7o_0sstj text ,c_gus881_9 double unique ,c_wzmb0 text ,primary key(c_q6qt9_) NONCLUSTERED) shard_row_id_bits=4 pre_split_regions=2;")
	tk.MustExec("insert into t_bhze93f (c_x393ej_, c_k3kss19, c_q6qt9_, c_wp7o_0sstj, c_gus881_9, c_wzmb0) values(-1458322912, 5.32, -811171723, cast(null as char), 96.15, 'r0exs4umz5'),(cast(null as signed), 57.87, -1624364959, 't968', 35.92, 'f'),(1237193156, 81.49, -744800718, 'pfcascv16e', cast(null as double), 'pv34_'),(1955355436, 16.26, -1560468978, cast(null as char), 65537.1, 'bps');")
	tk.MustExec("create table t_b0t (c_g int ,c_ci4cf5ns tinyint ,c_at double ,c_xb int ,c_uyu4fop36b double ,c_zhouc text ,c_m9g6b tinyint not null unique ,c_k7rlw47ob tinyint unique ,primary key(c_xb) CLUSTERED) pre_split_regions=2;")

	// Need no panic
	tk.MustQuery("select /*+ STREAM_AGG() */ (ref_4.c_k3kss19 / ref_4.c_k3kss19) as c2 from t_bhze93f as ref_4 where (EXISTS (select ref_5.c_wp7o_0sstj as c0 from t_bhze93f as ref_5 where (207007502 < (select distinct ref_6.c_weg as c0 from t_xf1at0 as ref_6 union all (select ref_7.c_xb as c0 from t_b0t as ref_7 where (-16090 != ref_4.c_x393ej_)) limit 1)) limit 1));")
}

func TestIssue60926(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (col0 int, col1 int);")
	tk.MustExec("create table t2 (col0 int, col1 int);")
	tk.MustExec("insert into t1 values (0, 10), (1, 10), (2, 10), (3, 10), (4, 10), (5, 10), (6, 10), (7, 10), (8, 10), (9, 10), (10, 10);")
	tk.MustExec("insert into t2 values (0, 5), (0, 5), (1, 5), (2, 5), (2, 5), (3, 5), (4, 5), (5, 5), (5, 5), (6, 5), (7, 5), (8, 5), (8, 5), (9, 5), (9, 5), (10, 5);")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/issue60926", "panic"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/issue60926"))
	}()
	executor.IsChildCloseCalledForTest.Store(false)
	tk.MustQuery("select * from t1 join (select col0, sum(col1) from t2 group by col0) as r on t1.col0 = r.col0;")
	require.True(t, executor.IsChildCloseCalledForTest.Load())
}
