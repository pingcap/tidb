// Copyright 2026 PingCAP, Inc.
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

package core_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestHF1045ApplyColumnPruningRegression(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, pc, code")
	tk.MustExec(`create table t1 (
		id bigint primary key auto_increment,
		k varchar(32),
		key idx_k(k)
	)`)
	tk.MustExec(`create table t2 (
		id bigint primary key auto_increment,
		k varchar(32),
		sub_id varchar(32),
		extra varchar(32),
		key idx_k(k)
	)`)
	tk.MustExec(`create table pc (
		id bigint primary key auto_increment,
		pc_id varchar(32),
		status varchar(8),
		unique key uk(pc_id)
	)`)
	tk.MustExec(`create table code (
		id bigint primary key auto_increment,
		ct varchar(64),
		cv varchar(32),
		cn varchar(64),
		unique key uk(ct, cv)
	)`)

	tk.MustExec("insert into t1 (k) values ('k1'),('k2'),('k3')")
	tk.MustExec("insert into t2 (k, sub_id, extra) values ('k1','spc1','e1'),('k2','spc2','e2'),('k3','spc3','e3')")
	tk.MustExec("insert into pc (pc_id, status) values ('spc1','01'),('spc2','02'),('spc3','03')")
	tk.MustExec("insert into code (ct, cv, cn) values ('RC','01','S1'),('RC','02','S2'),('RC','03','S3')")
	tk.MustExec("analyze table t1, t2, pc, code")

	sql := `select count(1) from (
		select
			(
				select /*+ NO_DECORRELATE() */ x.cn
				from code x
				where x.ct = 'RC'
				  and x.cv = (
					select /*+ NO_DECORRELATE() */ y.status
					from pc y
					where y.pc_id = c.sub_id
					limit 1
				  )
				limit 1
			) as caseStatus
		from t1 a
		join t2 c on a.k = c.k
		limit 100
	) m`

	plan := fmt.Sprint(tk.MustQuery("explain format = 'brief' " + sql).Rows())
	require.True(t, strings.Contains(plan, "Apply"), "expected Apply in plan, got %s", plan)

	tk.MustQuery(sql).Check(testkit.Rows("3"))
}
