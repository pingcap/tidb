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

package executor_test

import (
	"os"
	"testing"

	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestIndexAdvise(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustGetErrMsg("index advise infile '/tmp/nonexistence.sql'", "Index Advise: don't support load file without local field")
	tk.MustGetErrMsg("index advise local infile ''", "Index Advise: infile path is empty")
	tk.MustGetErrMsg("index advise local infile '/tmp/nonexistence.sql' lines terminated by ''", "Index Advise: don't support advise index for SQL terminated by nil")

	path := "/tmp/index_advise.sql"
	fp, err := os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)
	defer func() {
		err = fp.Close()
		require.NoError(t, err)
		err = os.Remove(path)
		require.NoError(t, err)
	}()

	_, err = fp.WriteString("\n" +
		"select * from t;\n" +
		"\n" +
		"select * from t where a > 1;\n" +
		"select a from t where a > 1 and a < 100;\n" +
		"\n" +
		"\n" +
		"select a,b from t1,t2 where t1.a = t2.b;\n" +
		"\n")
	require.NoError(t, err)

	// TODO: Using "tastCase" to do more test when we finish the index advisor completely.
	tk.MustExec("index advise local infile '/tmp/index_advise.sql' max_minutes 3 max_idxnum per_table 4 per_db 5")
	ctx := tk.Session().(sessionctx.Context)
	ia, ok := ctx.Value(executor.IndexAdviseVarKey).(*executor.IndexAdviseInfo)
	defer ctx.SetValue(executor.IndexAdviseVarKey, nil)
	require.True(t, ok)
	require.Equal(t, uint64(3), ia.MaxMinutes)
	require.Equal(t, uint64(4), ia.MaxIndexNum.PerTable)
	require.Equal(t, uint64(5), ia.MaxIndexNum.PerDB)
}

func TestIndexJoinProjPattern(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1(
pnbrn_cnaps varchar(5) not null,
new_accno varchar(18) not null,
primary key(pnbrn_cnaps,new_accno) nonclustered
);`)
	tk.MustExec(`create table t2(
pnbrn_cnaps varchar(5) not null,
txn_accno varchar(18) not null,
txn_dt date not null,
yn_frz varchar(1) default null
);`)
	tk.MustExec(`insert into t1(pnbrn_cnaps,new_accno) values ("40001","123")`)
	tk.MustExec(`insert into t2(pnbrn_cnaps, txn_accno, txn_dt, yn_frz) values ("40001","123","20221201","0");`)

	sql := `update
/*+ inl_join(a) */
t2 b,
(
select t1.pnbrn_cnaps,
t1.new_accno
from t1
where t1.pnbrn_cnaps = '40001'
) a
set b.yn_frz = '1'
where b.txn_dt = str_to_date('20221201', '%Y%m%d')
and b.pnbrn_cnaps = a.pnbrn_cnaps
and b.txn_accno = a.new_accno;`
	rows := [][]interface{}{
		{"Update_8"},
		{"└─IndexJoin_14"},
		{"  ├─TableReader_25(Build)"},
		{"  │ └─Selection_24"},
		{"  │   └─TableFullScan_23"},
		{"  └─IndexReader_12(Probe)"},
		{"    └─Selection_11"},
		{"      └─IndexRangeScan_10"},
	}
	tk.MustExec("set @@session.tidb_enable_inl_join_inner_multi_pattern='ON'")
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)
	rows = [][]interface{}{
		{"Update_8"},
		{"└─HashJoin_10"},
		{"  ├─IndexReader_17(Build)"},
		{"  │ └─IndexRangeScan_16"},
		{"  └─TableReader_14(Probe)"},
		{"    └─Selection_13"},
		{"      └─TableFullScan_12"},
	}
	tk.MustExec("set @@session.tidb_enable_inl_join_inner_multi_pattern='OFF'")
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)

	tk.MustExec("set @@session.tidb_enable_inl_join_inner_multi_pattern='ON'")
	tk.MustExec(sql)
	tk.MustQuery("select yn_frz from t2").Check(testkit.Rows("1"))
}

func TestIndexJoinSelPattern(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(` create table tbl_miss(
id bigint(20) unsigned not null
,txn_dt date default null
,perip_sys_uuid varchar(32) not null
,rvrs_idr varchar(1) not null
,primary key(id) clustered
,key idx1 (txn_dt, perip_sys_uuid, rvrs_idr)
);
`)
	tk.MustExec(`insert into tbl_miss (id,txn_dt,perip_sys_uuid,rvrs_idr) values (1,"20221201","123","1");`)
	tk.MustExec(`create table tbl_src(
txn_dt date default null
,uuid varchar(32) not null
,rvrs_idr char(1)
,expd_inf varchar(5000)
,primary key(uuid,rvrs_idr) nonclustered
);
`)
	tk.MustExec(`insert into tbl_src (txn_dt,uuid,rvrs_idr) values ("20221201","123","1");`)
	sql := `select /*+ use_index(mis,) inl_join(src) */
    *
 from tbl_miss mis
     ,tbl_src src
 where src.txn_dt >= str_to_date('20221201', '%Y%m%d')
 and mis.id between 1 and 10000
 and mis.perip_sys_uuid = src.uuid
 and mis.rvrs_idr = src.rvrs_idr
 and mis.txn_dt = src.txn_dt
 and (
     case when isnull(src.expd_inf) = 1 then ''
     else
         substr(concat_ws('',src.expd_inf,'~~'),
             instr(concat_ws('',src.expd_inf,'~~'),'~~a4') + 4,
             instr(substr(concat_ws('',src.expd_inf,'~~'),
                 instr(concat_ws('',src.expd_inf,'~~'),'~~a4') + 4, length(concat_ws('',src.expd_inf,'~~'))),'~~') -1)
     end
 ) != '01';`
	rows := [][]interface{}{
		{"HashJoin_9"},
		{"├─TableReader_12(Build)"},
		{"│ └─Selection_11"},
		{"│   └─TableRangeScan_10"},
		{"└─Selection_13(Probe)"},
		{"  └─TableReader_16"},
		{"    └─Selection_15"},
		{"      └─TableFullScan_14"},
	}
	tk.MustExec("set @@session.tidb_enable_inl_join_inner_multi_pattern='OFF'")
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)
	rows = [][]interface{}{
		{"IndexJoin_13"},
		{"├─TableReader_25(Build)"},
		{"│ └─Selection_24"},
		{"│   └─TableRangeScan_23"},
		{"└─Selection_12(Probe)"},
		{"  └─IndexLookUp_11"},
		{"    ├─IndexRangeScan_8(Build)"},
		{"    └─Selection_10(Probe)"},
		{"      └─TableRowIDScan_9"},
	}
	tk.MustExec("set @@session.tidb_enable_inl_join_inner_multi_pattern='ON'")
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)
	tk.MustQuery(sql).Check(testkit.Rows("1 2022-12-01 123 1 2022-12-01 123 1 <nil>"))
	tk.MustExec("set @@session.tidb_enable_inl_join_inner_multi_pattern='OFF'")
	tk.MustQuery(sql).Check(testkit.Rows("1 2022-12-01 123 1 2022-12-01 123 1 <nil>"))
}
