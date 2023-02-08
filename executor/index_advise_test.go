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
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = true
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
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = false
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)

	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = true
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
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = false
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
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = true
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = true
	tk.MustQuery(sql).Check(testkit.Rows("1 2022-12-01 123 1 2022-12-01 123 1 <nil>"))
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = false
	tk.MustQuery(sql).Check(testkit.Rows("1 2022-12-01 123 1 2022-12-01 123 1 <nil>"))
}

func TestIndexJoinCases1(t *testing.T) {
	// Proj
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `cdm_tmp_me`( `c_col1` bigint(20) unsigned NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */ , `c_col2` date NOT NULL , `c_col3` varchar(32) NOT NULL , `c_col4` varchar(1) NOT NULL , `c_col5` varchar(6) DEFAULT NULL , PRIMARY KEY (`c_col1`) /*T![clustered_index] CLUSTERED */, KEY `o_col1x_old_n1` (`c_col2`, `c_col3`, `c_col4`), KEY `o_col1x_old_n2` (`c_col3`, `c_col4`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_rand_base] AUTO_RANDOM_BASE=60001 */ ")
	tk.MustExec("CREATE TABLE `ods_tmp_tbi`( `t_col1` varchar(1) DEFAULT NULL , `t_col2` varchar(8) DEFAULT NULL , `t_col3` varchar(8) DEFAULT NULL , `t_col4` date DEFAULT NULL , `t_col5` varchar(32) NOT NULL , `t_col6` varchar(4) DEFAULT NULL , `t_col7` varchar(7) DEFAULT NULL , `t_col8` varchar(5) DEFAULT NULL , `t_col9` varchar(70) DEFAULT NULL , `t_col10` varchar(6) DEFAULT NULL , `t_col11` varchar(6) DEFAULT NULL , `t_col12` varchar(2) DEFAULT NULL , `t_col13` varchar(1) NOT NULL , `t_col14` varchar(160) DEFAULT NULL , `t_col15` varchar(200) DEFAULT NULL , `t_col16` varchar(210) DEFAULT NULL , `t_col17` varchar(140) DEFAULT NULL , `t_col18` varchar(1) DEFAULT NULL , `t_col19` varchar(6) DEFAULT NULL , `t_col20` varchar(80) DEFAULT NULL , `t_col21` varchar(19) DEFAULT NULL , `t_col22` varchar(35) DEFAULT NULL , `t_col23` varchar(4) DEFAULT NULL , `t_col24` varchar(3) DEFAULT NULL , `t_col25` varchar(2) DEFAULT NULL , `t_col26` varchar(70) DEFAULT NULL , `t_col27` varchar(14) DEFAULT NULL , `t_col28` varchar(80) DEFAULT NULL , `t_col29` varchar(19) DEFAULT NULL , `t_col30` varchar(35) DEFAULT NULL , `t_col31` varchar(4) DEFAULT NULL , `t_col32` varchar(3) DEFAULT NULL , `t_col33` varchar(2) DEFAULT NULL , `t_col34` varchar(70) DEFAULT NULL , `t_col35` varchar(14) DEFAULT NULL , `t_col36` varchar(80) DEFAULT NULL , `t_col37` varchar(19) DEFAULT NULL , `t_col38` varchar(35) DEFAULT NULL , `t_col39` varchar(4) DEFAULT NULL , `t_col40` varchar(3) DEFAULT NULL , `t_col41` varchar(2) DEFAULT NULL , `t_col42` varchar(70) DEFAULT NULL , `t_col43` varchar(15) DEFAULT NULL , `t_col44` varchar(80) DEFAULT NULL , `t_col45` varchar(19) DEFAULT NULL , `t_col46` varchar(35) DEFAULT NULL , `t_col47` varchar(4) DEFAULT NULL , `t_col48` varchar(3) DEFAULT NULL , `t_col49` varchar(2) DEFAULT NULL , `t_col50` varchar(70) DEFAULT NULL , `t_col51` varchar(14) DEFAULT NULL , `t_col52` varchar(4) DEFAULT NULL , `t_col53` varchar(30) DEFAULT NULL , `t_col54` varchar(4) DEFAULT NULL , `t_col55` varchar(30) DEFAULT NULL , `t_col56` varchar(256) DEFAULT NULL , `t_col57` varchar(256) DEFAULT NULL , `t_col58` varchar(64) DEFAULT NULL , `t_col59` varchar(65) DEFAULT NULL , `t_col60` varchar(32) DEFAULT NULL , `t_col61` varchar(32) DEFAULT NULL , `t_col62` varchar(4000) DEFAULT NULL , `t_col63` date NOT NULL , `t_col64` varchar(10) NOT NULL , PRIMARY KEY (`t_col5`,`t_col13`) /*T![clustered_index] NONCLUSTERED */, KEY `o_col1x_tips` (`t_col4`,`t_col5`,`t_col13`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=6 */")
	tk.MustExec("CREATE TABLE `ODS_TMP_TXN`( `o_col1` bigint(20) unsigned NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */ , `o_col2` int(4) DEFAULT NULL , `o_col3` varchar(34) DEFAULT NULL , `o_col4` varchar(200) DEFAULT NULL, `o_col5` char(8) DEFAULT NULL , `o_col6` char(3) DEFAULT NULL , `o_col7` varchar(18) DEFAULT NULL , `o_col8` date DEFAULT NULL , `o_col9` varchar(9) DEFAULT NULL, `o_col10` varchar(9) DEFAULT NULL , `o_col11` char(5) DEFAULT NULL , `o_col12` varchar(18) DEFAULT NULL , `o_col13` char(4) DEFAULT NULL , `o_col14` char(3) DEFAULT NULL , `o_col15` char(2) DEFAULT NULL , `o_col16` varchar(12) DEFAULT NULL , `o_col138` varchar(32) NOT NULL , `o_col17` char(4) DEFAULT NULL , `o_col18` char(4) DEFAULT NULL, `o_col19` varchar(100) DEFAULT NULL , `o_col20` varchar(50) DEFAULT NULL , `o_col21` char(1) DEFAULT NULL , `o_col22` char(2) DEFAULT NULL , `o_col23` char(3) DEFAULT NULL , `o_col24` varchar(19) DEFAULT NULL , `o_col25` char(2) DEFAULT NULL , `o_col26` char(4) DEFAULT NULL , `o_col27` char(8) DEFAULT NULL , `o_col28` char(7) DEFAULT NULL , `o_col29` char(5) DEFAULT NULL , `o_col30` varchar(120) DEFAULT NULL , `o_col31` date DEFAULT NULL, `o_col32` char(1) DEFAULT NULL , `o_col33` varchar(10) DEFAULT NULL , `o_col34` varchar(10) DEFAULT NULL , `o_col35` char(2) DEFAULT NULL , `o_col36` char(6) DEFAULT NULL , `o_col37` char(6) DEFAULT NULL , `o_col38` date DEFAULT NULL , `o_col39` varchar(12) DEFAULT NULL , `o_col40` char(3) DEFAULT NULL , `o_col41` decimal(14, 6) DEFAULT NULL , `o_col42` char(1) DEFAULT NULL , `o_col43` char(1) DEFAULT NULL , `o_col44` decimal(17, 3) DEFAULT NULL , `o_col45` decimal(17, 3) DEFAULT NULL , `o_col46` decimal(17, 3) DEFAULT NULL , `o_col47` decimal(17, 3) DEFAULT NULL , `o_col48` decimal(17, 3) DEFAULT NULL , `o_col49` decimal(17, 3) DEFAULT NULL , `o_col50` decimal(17, 3) DEFAULT NULL , `o_col51` decimal(17, 3) DEFAULT NULL , `o_col52` decimal(17, 3) DEFAULT NULL , `o_col53` decimal(17, 3) DEFAULT NULL , `o_col54` decimal(17, 3) DEFAULT NULL , `o_col55` decimal(17, 3) DEFAULT NULL , `o_col56` decimal(17, 3) DEFAULT NULL , `o_col57` decimal(17, 3) DEFAULT NULL , `o_col58` decimal(17, 3) DEFAULT NULL , `o_col59` varchar(160) DEFAULT NULL , `o_col60` varchar(200) DEFAULT NULL , `o_col61` varchar(225) DEFAULT NULL , `o_col62` varchar(140) DEFAULT NULL , `o_col63` char(1) DEFAULT NULL , `o_col64` char(6) DEFAULT NULL , `o_col65` varchar(200) DEFAULT NULL , `o_col66` varchar(19) DEFAULT NULL , `o_col67` varchar(35) DEFAULT NULL, `o_col68` char(4) DEFAULT NULL , `o_col69` char(3) DEFAULT NULL , `o_col70` char(2) DEFAULT NULL , `o_col71` varchar(120) DEFAULT NULL , `o_col72` varchar(14) DEFAULT NULL , `o_col73` varchar(200) DEFAULT NULL , `o_col74` varchar(19) DEFAULT NULL , `o_col75` varchar(35) DEFAULT NULL , `o_col76` char(4) DEFAULT NULL , `o_col77` char(3) DEFAULT NULL , `o_col78` char(2) DEFAULT NULL , `o_col79` varchar(210) DEFAULT NULL , `o_col80` varchar(14) DEFAULT NULL , `o_col81` varchar(200) DEFAULT NULL , `o_col82` varchar(19) DEFAULT NULL , `o_col83` varchar(35) DEFAULT NULL , `o_col84` char(4) DEFAULT NULL , `o_col85` char(3) DEFAULT NULL , `o_col86` char(2) DEFAULT NULL , `o_col87` varchar(120) DEFAULT NULL , `o_col88` varchar(14) DEFAULT NULL , `o_col89` varchar(200) DEFAULT NULL, `o_col90` varchar(19) DEFAULT NULL , `o_col91` varchar(35) DEFAULT NULL , `o_col92` char(4) DEFAULT NULL , `o_col93` char(3) DEFAULT NULL , `o_col94` char(2) DEFAULT NULL , `o_col95` varchar(120) DEFAULT NULL , `o_col96` varchar(14) DEFAULT NULL , `o_col97` char(4) DEFAULT NULL , `o_col98` varchar(40) DEFAULT NULL , `o_col99` char(4) DEFAULT NULL , `o_col100` varchar(40) DEFAULT NULL , `o_col101` varchar(35) DEFAULT NULL , `o_col102` char(4) DEFAULT NULL , `o_col103` int(2) DEFAULT NULL , `o_col104` decimal(17, 3) DEFAULT NULL , `o_col105` decimal(17, 3) DEFAULT NULL , `o_col106` varchar(256) DEFAULT NULL , `o_col107` varchar(256) DEFAULT NULL , `o_col108` varchar(64) DEFAULT NULL , `o_col109` varchar(64) DEFAULT NULL , `o_col110` varchar(64) DEFAULT NULL , `o_col111` varchar(64) DEFAULT NULL , `o_col112` varchar(32) DEFAULT NULL , `o_col113` varchar(32) DEFAULT NULL , `o_col114` varchar(50) DEFAULT NULL , `o_col115` varchar(100) DEFAULT NULL , `o_col116` char(1) DEFAULT NULL , `o_col117` char(1) DEFAULT NULL , `o_col118` char(1) DEFAULT NULL , `o_col119` char(1) DEFAULT NULL , `o_col120` char(1) DEFAULT NULL , `o_col121` varchar(10) DEFAULT NULL , `o_col122` char(1) DEFAULT NULL , `o_col123` char(1) DEFAULT NULL , `o_col124` varchar(70) DEFAULT NULL , `o_col125` varchar(70) DEFAULT NULL , `o_col126` varchar(70) DEFAULT NULL , `o_col127` varchar(70) DEFAULT NULL , `o_col128` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP , `o_col129` char(8) DEFAULT NULL , `o_col130` varchar(8000) DEFAULT NULL , `o_col131` char(2) DEFAULT NULL , `o_col132` char(1) DEFAULT NULL , `o_col133` varchar(10) DEFAULT NULL , `o_col134` varchar(18) DEFAULT NULL , `o_col135` varchar(16) DEFAULT NULL , `o_col136` varchar(16) DEFAULT NULL , `o_col137` char(8) DEFAULT NULL , PRIMARY KEY(`o_col1`) /*T![clustered_index] CLUSTERED */, KEY `o_col1x_ods_txn_external_1` (`o_col138`), KEY `o_col1x_ods_txn_external_2` (`o_col3`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T![auto_rand_base] AUTO_RANDOM_BASE=30001 */ ")

	sql := "INSERT into ODS_TMP_TXN( o_col8, o_col138, o_col26, o_col28, o_col29, o_col30, o_col33, o_col34, o_col37, o_col43, o_col59, o_col60, o_col61, o_col62, o_col63, o_col64, o_col65, o_col66, o_col67, o_col68, o_col69, o_col70, o_col71, o_col72, o_col73, o_col74, o_col75, o_col76, o_col77, o_col78, o_col79, o_col80, o_col81, o_col82, o_col83, o_col84, o_col85, o_col86, o_col87, o_col88, o_col89, o_col90, o_col91, o_col92, o_col93, o_col94, o_col95, o_col96, o_col97, o_col98, o_col99, o_col100, o_col106, o_col107, o_col108, o_col109, o_col112, o_col113, o_col128, o_col130, o_col131) SELECT /*+ INL_JOIN(TBI) */ TBI.t_col4, TBI.t_col5, TBI.t_col6, TBI.t_col7, TBI.t_col8, TBI.t_col9, TBI.t_col10, TBI.t_col11, TBI.t_col12, TBI.t_col13, TBI.t_col14, TBI.t_col15, TBI.t_col16, TBI.t_col17, TBI.t_col18, TBI.t_col19, TBI.t_col20, TBI.t_col21, TBI.t_col22, TBI.t_col23, TBI.t_col24, TBI.t_col25, TBI.t_col26, TBI.t_col27, TBI.t_col28, TBI.t_col29, TBI.t_col30, TBI.t_col31, TBI.t_col32, TBI.t_col33, TBI.t_col34, TBI.t_col35, TBI.t_col36, TBI.t_col37, TBI.t_col38, TBI.t_col39, TBI.t_col40, TBI.t_col41, TBI.t_col42, TBI.t_col43, TBI.t_col44, TBI.t_col45, TBI.t_col46, TBI.t_col47, TBI.t_col48, TBI.t_col49, TBI.t_col50, TBI.t_col51, TBI.t_col52, TBI.t_col53, TBI.t_col54, TBI.t_col55, TBI.t_col56, TBI.t_col57, TBI.t_col58, TBI.t_col59, TBI.t_col60, TBI.t_col61, DATE_FORMAT(now(), '%Y%m%d%H%i%s') AS ENTRY_TIME, TBI.t_col62, '02' FROM CDM_TMP_ME CME,ODS_TMP_TBI TBI WHERE TBI.t_col4>=DATE_ADD(STR_TO_DATE('2022020','%Y%m%d'),INTERVAL -1 day) and(case WHEN isnull(TBI.t_col62)=1 THEN '' WHEN instr(concat_ws('',TBI.t_col62,'~~'),'~~a4')= 0 THEN '' ELSE substr(concat_ws('',TBI.t_col62,'~~'), instr(concat_ws('',TBI.t_col62,'~~'),'~~a4')+4, instr(substr(concat_ws('',TBI.t_col62,'~~'), instr(concat_ws('',TBI.t_col62,'~~'),'~~a4')+4,length(concat_ws('',TBI.t_col62,'~~'))),'~~') - 1) END) != '01' AND CME.c_col1 BETWEEN 111 AND 222 AND((SUBSTR(CME.c_col3, 1, 2) = '48' AND LENGTH(CME.c_col3) = 12) OR( SUBSTR(CME.c_col3, 1, 4) = '0148')) AND CME.c_col3 = TBI.t_col5 AND CME.c_col4 = TBI.t_col13 AND CME.c_col2 = TBI.t_col4;"
	rows := [][]interface{}{
		{"Insert_1"},
		{"└─Projection_9"},
		{"  └─HashJoin_25"},
		{"    ├─Selection_50(Build)"},
		{"    │ └─TableReader_49"},
		{"    │   └─Selection_48"},
		{"    │     └─TableRangeScan_47"},
		{"    └─Selection_37(Probe)"},
		{"      └─TableReader_40"},
		{"        └─Selection_39"},
		{"          └─TableFullScan_38"},
	}
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = false
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)

	rows = [][]interface{}{
		{"Insert_1"},
		{"└─Projection_9"},
		{"  └─IndexJoin_17"},
		{"    ├─Selection_32(Build)"},
		{"    │ └─TableReader_31"},
		{"    │   └─Selection_30"},
		{"    │     └─TableRangeScan_29"},
		{"    └─Selection_16(Probe)"},
		{"      └─IndexLookUp_14"},
		{"        ├─Selection_13(Build)"},
		{"        │ └─IndexRangeScan_11"},
		{"        └─TableRowIDScan_12(Probe)"},
	}
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = true
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)
}

func TestIndexJoinCases2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `t_0`( `c_7` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */ , `c_1` varchar(16) DEFAULT '' , `c_2` varchar(16) DEFAULT '' , `c_3` decimal(5,0) DEFAULT '0' , `c_4` varchar(28) DEFAULT NULL , `c_5` varchar(1) DEFAULT '' , `c_6` int(4) DEFAULT NULL , PRIMARY KEY (`c_7`) /*T![clustered_index] CLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ")
	tk.MustExec("CREATE TABLE `t_8`( `c_98` varchar(16) NOT NULL , `c_101` varchar(4) DEFAULT NULL , `c_10` varchar(4) DEFAULT NULL , `c_11` varchar(4) DEFAULT NULL , `c_12` varchar(16) DEFAULT NULL , `c_13` varchar(16) DEFAULT NULL , `c_14` varchar(3) DEFAULT NULL , `c_110` varchar(4) DEFAULT NULL , `c_16` date DEFAULT NULL , `c_17` varchar(4) DEFAULT NULL , `c_18` date DEFAULT NULL , `c_19` date DEFAULT NULL , `c_20` varchar(1) DEFAULT NULL , `c_21` varchar(20) DEFAULT NULL , `c_22` varchar(120) DEFAULT NULL , `c_23` varchar(4) DEFAULT NULL , `c_24` decimal(5,0) DEFAULT NULL , `c_25` decimal(5,0) DEFAULT NULL , `c_26` date DEFAULT NULL , `c_27` date DEFAULT NULL , `c_28` date DEFAULT NULL , `c_29` date DEFAULT NULL , `c_30` date DEFAULT NULL , `c_31` date DEFAULT NULL , `c_32` date DEFAULT NULL , `c_33` varchar(16) DEFAULT NULL , `c_34` varchar(1) DEFAULT NULL , `c_35` date DEFAULT NULL , `c_36` date DEFAULT NULL , `c_37` varchar(1) DEFAULT NULL , `c_38` date DEFAULT NULL , `c_39` date DEFAULT NULL , `c_40` date DEFAULT NULL , `c_41` date DEFAULT NULL , `c_42` decimal(5,0) DEFAULT NULL , `c_43` decimal(21,3) DEFAULT NULL , `c_44` decimal(21,3) DEFAULT NULL , `c_45` decimal(21,3) DEFAULT NULL , `c_46` decimal(21,3) DEFAULT NULL , `c_47` decimal(5,0) DEFAULT NULL , `c_48` decimal(5,0) DEFAULT NULL , `c_49` decimal(5,0) DEFAULT NULL , `c_50` decimal(5,0) DEFAULT NULL , `c_51` decimal(21,3) DEFAULT NULL , `c_52` decimal(5,0) DEFAULT NULL , `c_53` decimal(21,3) DEFAULT NULL , `c_54` decimal(5,0) DEFAULT NULL , `c_55` decimal(21,3) DEFAULT NULL , `c_56` varchar(1) DEFAULT NULL , `c_57` varchar(20) DEFAULT NULL , `c_58` date DEFAULT NULL , `c_59` varchar(4) DEFAULT NULL , `c_60` decimal(21,3) DEFAULT NULL , `c_61` varchar(3) DEFAULT NULL , `c_62` varchar(4) DEFAULT NULL , `c_63` date DEFAULT NULL , `c_64` decimal(21,3) DEFAULT NULL , `c_65` varchar(30) DEFAULT NULL , `c_66` date DEFAULT NULL , `c_67` decimal(21,3) DEFAULT NULL , `c_68` varchar(7) DEFAULT NULL , `c_69` varchar(3) DEFAULT NULL , `c_70` decimal(21,3) DEFAULT NULL , `c_71` decimal(21,3) DEFAULT NULL , `c_72` decimal(21,3) DEFAULT NULL , `c_73` decimal(21,3) DEFAULT NULL , `c_74` decimal(21,3) DEFAULT NULL , `c_75` decimal(21,3) DEFAULT NULL , `c_76` decimal(21,3) DEFAULT NULL , `c_77` varchar(3) DEFAULT NULL , `c_78` varchar(4) DEFAULT NULL , `c_79` varchar(1) DEFAULT NULL , `c_80` varchar(20) DEFAULT NULL , `c_81` varchar(1) DEFAULT NULL , `c_126` date DEFAULT NULL , `c_127` time DEFAULT NULL , `c_128` varchar(8) DEFAULT NULL , `c_129` varchar(5) DEFAULT NULL , `c_86` varchar(4) DEFAULT NULL , `c_87` varchar(1) DEFAULT NULL , `c_88` varchar(16) DEFAULT NULL , `c_89` decimal(5,0) DEFAULT NULL , `c_90` varchar(8) DEFAULT NULL , `c_91` varchar(24) DEFAULT NULL , `c_92` decimal(21,3) DEFAULT NULL , `c_93` decimal(21,3) DEFAULT NULL , `c_94` varchar(1) DEFAULT NULL , `c_95` varchar(4) DEFAULT NULL , `c_96` varchar(1) DEFAULT NULL , `c_97` decimal(21,3) DEFAULT NULL , PRIMARY KEY (`c_98`) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ")
	tk.MustExec("CREATE TABLE `t_99`( `c_130` varchar(16) NOT NULL , `c_100` varchar(5) NOT NULL , `c_101` varchar(4) DEFAULT NULL , `c_102` varchar(16) DEFAULT NULL , `c_103` varchar(16) DEFAULT NULL , `c_104` date DEFAULT NULL , `c_105` date DEFAULT NULL , `c_106` date DEFAULT NULL , `c_107` date DEFAULT NULL , `c_108` varchar(4) DEFAULT NULL , `c_109` varchar(4) DEFAULT NULL , `c_110` varchar(4) DEFAULT NULL , `c_111` varchar(4) DEFAULT NULL , `c_112` date DEFAULT NULL , `c_113` date DEFAULT NULL , `c_114` decimal(5,0) DEFAULT NULL , `c_115` varchar(60) DEFAULT NULL , `c_116` varchar(60) DEFAULT NULL , `c_117` varchar(4) DEFAULT NULL , `c_118` date DEFAULT NULL , `c_119` varchar(1) DEFAULT NULL , `c_120` date DEFAULT NULL , `c_121` varchar(4) DEFAULT NULL , `c_122` varchar(4) DEFAULT NULL , `c_123` varchar(4) DEFAULT NULL , `c_124` decimal(22,3) DEFAULT NULL , `c_125` decimal(22,3) DEFAULT NULL , `c_126` date DEFAULT NULL , `c_127` varchar(10) DEFAULT NULL , `c_128` varchar(8) DEFAULT NULL , `c_129` varchar(10) DEFAULT NULL , PRIMARY KEY (`c_130`,`c_100`) /*T![clustered_index] NONCLUSTERED */, KEY `c_131` (`c_130`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ")
	tk.MustExec("CREATE TABLE `t_122`( `c_400` bigint(20) NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */, `c_401` varchar(28) DEFAULT NULL , `c_402` varchar(16) DEFAULT NULL , PRIMARY KEY (`c_400`) /*T![clustered_index] CLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ")

	sql := "replace into t_122( c_401, c_402) SELECT /*+ inl_join(tgen010)*/ tgen016.c_4 AS PAN, CASE WHEN tgen016.c_5 = '0' AND data008.c_20 = '3' THEN tgen010.c_102 ELSE data008.c_33 END AS CTT_IDN_SKY FROM t_0 tgen016 LEFT JOIN t_8 data008 ON tgen016.c_1 = data008.c_98 LEFT JOIN t_99 tgen010 ON tgen016.c_2 = tgen010.c_130 AND tgen016.c_3 = tgen010.c_100 WHERE tgen016.c_7 BETWEEN 1 AND 1000;"
	rows := [][]interface{}{
		{"Insert_1"},
		{"└─Projection_13"},
		{"  └─HashJoin_15"},
		{"    ├─Projection_16(Build)"},
		{"    │ └─IndexJoin_20"},
		{"    │   ├─TableReader_32(Build)"},
		{"    │   │ └─TableRangeScan_31"},
		{"    │   └─IndexLookUp_19(Probe)"},
		{"    │     ├─IndexRangeScan_17(Build)"},
		{"    │     └─TableRowIDScan_18(Probe)"},
		{"    └─Projection_35(Probe)"},
		{"      └─TableReader_37"},
		{"        └─TableFullScan_36"},
	}
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = false
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)

	rows = [][]interface{}{
		{"Insert_1"},
		{"└─Projection_13"},
		{"  └─HashJoin_15"},
		{"    ├─Projection_16(Build)"},
		{"    │ └─IndexJoin_20"},
		{"    │   ├─TableReader_32(Build)"},
		{"    │   │ └─TableRangeScan_31"},
		{"    │   └─IndexLookUp_19(Probe)"},
		{"    │     ├─IndexRangeScan_17(Build)"},
		{"    │     └─TableRowIDScan_18(Probe)"},
		{"    └─Projection_35(Probe)"},
		{"      └─TableReader_37"},
		{"        └─TableFullScan_36"},
	}
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = true
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)
}

func TestIndexJoinCases3(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `t_0`( `c_0` varchar(18) DEFAULT '' , `c_6` varchar(19) NOT NULL , `c_15` decimal(21,3) NOT NULL , `c_3` datetime NOT NULL , `c_13` datetime NOT NULL , `c_5` datetime NOT NULL , PRIMARY KEY (`c_6`) /*T![clustered_index] NONCLUSTERED */, KEY `c_7` (`c_6`,`c_0`,`c_15`), KEY `c_8` (`c_6`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ")
	tk.MustExec("CREATE TABLE `t_9`( `c_19` varchar(4) NOT NULL , `c_10` varchar(35) NOT NULL DEFAULT '0' , `c_11` varchar(1) NOT NULL , `c_12` datetime NOT NULL , `c_13` datetime NOT NULL , `c_14` datetime NOT NULL , `c_15` decimal(21,3) DEFAULT NULL , `c_16` varchar(5) DEFAULT NULL , `c_17` varchar(19) NOT NULL DEFAULT '0' , `c_18` varchar(16) DEFAULT NULL , PRIMARY KEY (`c_19`,`c_10`,`c_11`,`c_17`) /*T![clustered_index] NONCLUSTERED */, KEY `c_20` (`c_14`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")

	sql := " select /*+ inl_join(A) */ E.c_6, E.c_15 FROM t_0 E LEFT JOIN t_9 A ON A.c_17 = E.c_6 WHERE A.c_17 is null and E._tidb_rowid BETWEEN 1 AND 1000;"
	rows := [][]interface{}{
		{"Projection_6"},
		{"└─Selection_7"},
		{"  └─HashJoin_8"},
		{"    ├─IndexReader_15(Build)"},
		{"    │ └─IndexFullScan_14"},
		{"    └─TableReader_11(Probe)"},
		{"      └─TableRangeScan_10"},
	}
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = false
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)

	rows = [][]interface{}{
		{"Projection_6"},
		{"└─Selection_7"},
		{"  └─HashJoin_8"},
		{"    ├─IndexReader_15(Build)"},
		{"    │ └─IndexFullScan_14"},
		{"    └─TableReader_11(Probe)"},
		{"      └─TableRangeScan_10"},
	}
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = true
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)
}

func TestIndexJoinCases4(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `t_0`( `c_9` bigint(20) NOT NULL AUTO_INCREMENT, `c_66` varchar(17) DEFAULT NULL , `c_56` varchar(5) DEFAULT NULL , `c_3` varchar(4) DEFAULT NULL , `c_78` varchar(3) DEFAULT NULL , `c_79` varchar(2) DEFAULT NULL , `c_83` varchar(17) NOT NULL , `c_7` varchar(1) DEFAULT NULL , `c_88` date DEFAULT NULL , PRIMARY KEY (`c_9`) /*T![clustered_index] CLUSTERED */, UNIQUE KEY `c_83` (`c_83`,`c_66`,`c_3`,`c_78`,`c_79`,`c_56`), KEY `c_11` (`c_66`,`c_56`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=30001 ")
	tk.MustExec("CREATE TABLE `t_12`( `c_12` varchar(5) NOT NULL , `c_23` varchar(18) NOT NULL , `c_14` varchar(2) DEFAULT NULL , `c_15` decimal(17,3) DEFAULT NULL , `c_16` varchar(3) DEFAULT NULL , `c_35` varchar(1) DEFAULT NULL , `c_18` varchar(1) DEFAULT NULL , `c_19` varchar(2) DEFAULT NULL , `c_20` date DEFAULT NULL , `c_89` varchar(4) DEFAULT NULL , `c_90` varchar(4) DEFAULT NULL , PRIMARY KEY (`c_23`,`c_12`) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=8 PRE_SPLIT_REGIONS=8 */")
	tk.MustExec("CREATE TABLE `t_24`( `c_89` varchar(4) NOT NULL , `c_90` varchar(4) NOT NULL , `c_26` varchar(100) DEFAULT NULL , `c_91` varchar(50) DEFAULT NULL , PRIMARY KEY (`c_89`,`c_90`) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ")
	tk.MustExec("CREATE TABLE `t_29`( `c_56` varchar(5) NOT NULL , `c_30` varchar(14) NOT NULL , `c_31` varchar(35) NOT NULL , `c_89` varchar(4) DEFAULT NULL , `c_90` varchar(4) DEFAULT NULL , `c_34` varchar(3) NOT NULL , `c_35` varchar(1) NOT NULL , `c_36` varchar(60) DEFAULT NULL , `c_37` varchar(16) DEFAULT NULL , `c_38` varchar(5) DEFAULT NULL , `c_39` varchar(5) DEFAULT NULL , `c_40` varchar(20) DEFAULT NULL , `c_84` varchar(1) NOT NULL , `c_42` varchar(14) DEFAULT NULL , `c_43` varchar(35) DEFAULT NULL , `c_44` varchar(1) NOT NULL , `c_87` datetime NOT NULL , `c_88` datetime NOT NULL , `c_93` datetime DEFAULT NULL , `c_48` varchar(5) DEFAULT NULL , `c_49` varchar(5) DEFAULT NULL , `c_50` varchar(80) DEFAULT NULL , `c_51` varchar(60) DEFAULT NULL , `c_52` varchar(20) DEFAULT NULL , `c_53` varchar(80) DEFAULT NULL , `c_54` datetime DEFAULT NULL , `c_55` datetime DEFAULT NULL , PRIMARY KEY (`c_56`,`c_87`,`c_30`,`c_31`) /*T![clustered_index] NONCLUSTERED */, KEY `c_57` (`c_56`,`c_84`,`c_34`,`c_31`,`c_87`), KEY `c_58` (`c_93`,`c_43`,`c_87`), KEY `c_59` (`c_43`,`c_84`,`c_34`), KEY `c_60` (`c_88`), KEY `c_61` (`c_31`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=2 PRE_SPLIT_REGIONS=2 */")
	tk.MustExec("CREATE TABLE `t_63`( `c_93` date NOT NULL , `c_64` varchar(1) NOT NULL , `c_65` varchar(35) NOT NULL , `c_98` varchar(35) DEFAULT NULL , `c_67` varchar(20) DEFAULT NULL , `c_68` varchar(5) DEFAULT NULL , `c_69` varchar(120) DEFAULT NULL , `c_70` varchar(80) DEFAULT NULL , `c_71` varchar(5) NOT NULL , `c_72` varchar(35) DEFAULT NULL , `c_73` varchar(20) DEFAULT NULL , `c_74` varchar(5) DEFAULT NULL , `c_75` varchar(120) DEFAULT NULL , `c_76` varchar(80) DEFAULT NULL , `c_77` varchar(4) DEFAULT NULL , `c_101` varchar(3) NOT NULL , `c_102` varchar(2) NOT NULL , `c_80` varchar(3) DEFAULT NULL , `c_81` char(1) DEFAULT NULL , `c_82` char(1) DEFAULT NULL , `c_105` varchar(35) NOT NULL , `c_84` varchar(1) NOT NULL , `c_85` decimal(21,3) DEFAULT NULL , `c_86` decimal(21,3) DEFAULT NULL , `c_87` datetime DEFAULT NULL , `c_88` date DEFAULT NULL , `c_89` varchar(4) DEFAULT NULL , `c_90` varchar(4) DEFAULT NULL , `c_91` varchar(50) DEFAULT NULL , `c_92` char(1) DEFAULT NULL , PRIMARY KEY (`c_93`,`c_64`,`c_71`,`c_101`,`c_102`,`c_105`) /*T![clustered_index] NONCLUSTERED */, KEY `c_94` (`c_65`), KEY `c_95` (`c_72`,`c_101`,`c_102`,`c_93`,`c_65`), KEY `c_96` (`c_98`), KEY `c_97` (`c_72`,`c_65`,`c_101`,`c_102`,`c_80`,`c_81`,`c_93`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=8 PRE_SPLIT_REGIONS=8 */")
	tk.MustExec("CREATE TABLE `t_98`( `c_98` varchar(17) DEFAULT NULL , `c_99` varchar(5) NOT NULL , `c_100` varchar(4) DEFAULT NULL , `c_101` int(3) DEFAULT NULL , `c_102` int(2) DEFAULT NULL , `c_105` varchar(17) NOT NULL , `c_104` varchar(1) DEFAULT NULL , PRIMARY KEY (`c_105`,`c_99`) /*T![clustered_index] NONCLUSTERED */, KEY `c_106` (`c_105`,`c_98`,`c_99`), KEY `c_107` (`c_98`,`c_99`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=8 PRE_SPLIT_REGIONS=8 */")

	sql := " INSERT INTO t_63( c_93, c_64, c_65, c_98, c_68, c_69, c_67, c_70, c_71, c_72, c_74, c_76, c_77, c_101, c_102, c_80, c_86, c_81, c_89, c_90, c_91, c_75, c_84, c_73, c_105, c_82, c_92, c_87, c_88) SELECT /* +inl_join(T)*/ STR_TO_DATE('20230203','%Y%m%d'), 'D', ACC.c_48, ACC.c_43 , ACC.c_49 , ACC.c_51 , ACC.c_52 , ACC.c_50, ACC.c_56 , ACC.c_31 , ACC.c_39 , ACC.c_53, T.c_3, case when TRIM(T.c_78) is null or TRIM(T.c_78) = '' then '0' else TRIM(T.c_78) end c_78, case when TRIM(T.c_79) is null or TRIM(T.c_79) = '' then '0' else TRIM(T.c_79) end c_79, BI.c_16, CASE WHEN T.c_7 = '9' THEN BI.c_15 ELSE (CASE WHEN (BI.c_20 =STR_TO_DATE('20230203','%Y%m%d')) THEN 0 ELSE BI.c_15 END) END, BI.c_35, BI.c_89, BI.c_90, case when TRIM(RP.c_91) is null or TRIM(RP.c_91) = '' then '其它' else TRIM(RP.c_91) end c_91, ACC.c_36, ACC.c_84, ACC.c_40, T.c_83, T.c_7, '1', ACC.c_87, LEAST(T.c_88,ACC.c_88) c_98 FROM t_0 T inner join t_12 BI on T.c_83=BI.c_23 AND T.c_56=BI.c_12 left join t_24 RP on BI.c_89=RP.c_89 and BI.c_90 = RP.c_90 inner join t_29 acc use index() on T.c_66=ACC.c_31 AND T.c_56=ACC.c_56 WHERE ACC.c_87<=STR_TO_DATE('20230203','%Y%m%d') AND ACC.c_88>=STR_TO_DATE('20230203','%Y%m%d') AND ACC.c_84='0' AND ACC.c_34='XXX' AND T.c_7='0' AND T.c_88=STR_TO_DATE('20230203','%Y%m%d') AND NOT EXISTS (SELECT 1 FROM t_98 II WHERE II.c_98=ACC.c_31 AND II.c_104='0' AND II.c_100=T.c_3) and ACC._tidb_rowid BETWEEN 1 AND 1000 AND ACC.c_56 = concat('4','123456')"
	rows := [][]interface{}{
		{"Insert_1"},
		{"└─Projection_23"},
		{"  └─IndexJoin_28"},
		{"    ├─Projection_38(Build)"},
		{"    │ └─IndexJoin_42"},
		{"    │   ├─IndexJoin_57(Build)"},
		{"    │   │ ├─IndexJoin_73(Build)"},
		{"    │   │ │ ├─TableReader_88(Build)"},
		{"    │   │ │ │ └─Selection_87"},
		{"    │   │ │ │   └─TableRangeScan_86"},
		{"    │   │ │ └─IndexLookUp_72(Probe)"},
		{"    │   │ │   ├─Selection_70(Build)"},
		{"    │   │ │   │ └─IndexRangeScan_68"},
		{"    │   │ │   └─Selection_71(Probe)"},
		{"    │   │ │     └─TableRowIDScan_69"},
		{"    │   │ └─IndexLookUp_56(Probe)"},
		{"    │   │   ├─IndexRangeScan_54(Build)"},
		{"    │   │   └─TableRowIDScan_55(Probe)"},
		{"    │   └─IndexLookUp_41(Probe)"},
		{"    │     ├─IndexRangeScan_39(Build)"},
		{"    │     └─TableRowIDScan_40(Probe)"},
		{"    └─IndexLookUp_27(Probe)"},
		{"      ├─IndexRangeScan_24(Build)"},
		{"      └─Selection_26(Probe)"},
		{"        └─TableRowIDScan_25"},
	}
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = false
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)

	rows = [][]interface{}{
		{"Insert_1"},
		{"└─Projection_23"},
		{"  └─IndexJoin_28"},
		{"    ├─Projection_38(Build)"},
		{"    │ └─IndexJoin_42"},
		{"    │   ├─IndexJoin_57(Build)"},
		{"    │   │ ├─IndexJoin_73(Build)"},
		{"    │   │ │ ├─TableReader_88(Build)"},
		{"    │   │ │ │ └─Selection_87"},
		{"    │   │ │ │   └─TableRangeScan_86"},
		{"    │   │ │ └─IndexLookUp_72(Probe)"},
		{"    │   │ │   ├─Selection_70(Build)"},
		{"    │   │ │   │ └─IndexRangeScan_68"},
		{"    │   │ │   └─Selection_71(Probe)"},
		{"    │   │ │     └─TableRowIDScan_69"},
		{"    │   │ └─IndexLookUp_56(Probe)"},
		{"    │   │   ├─IndexRangeScan_54(Build)"},
		{"    │   │   └─TableRowIDScan_55(Probe)"},
		{"    │   └─IndexLookUp_41(Probe)"},
		{"    │     ├─IndexRangeScan_39(Build)"},
		{"    │     └─TableRowIDScan_40(Probe)"},
		{"    └─IndexLookUp_27(Probe)"},
		{"      ├─IndexRangeScan_24(Build)"},
		{"      └─Selection_26(Probe)"},
		{"        └─TableRowIDScan_25"},
	}
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = true
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)
}

func TestIndexJoinCases5(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `t_0`( `c_128` varchar(34) NOT NULL , `c_1` varchar(16) NOT NULL , `c_2` varchar(16) NOT NULL , `c_3` varchar(8) NOT NULL , `c_4` varchar(3) NOT NULL , `c_5` varchar(17) DEFAULT NULL , `c_132` date DEFAULT NULL , `c_7` varchar(9) DEFAULT NULL , `c_8` varchar(9) DEFAULT NULL , `c_9` varchar(5) DEFAULT NULL , `c_10` varchar(17) DEFAULT NULL , `c_11` varchar(4) DEFAULT NULL , `c_12` varchar(3) DEFAULT NULL , `c_13` varchar(2) DEFAULT NULL , `c_14` varchar(12) DEFAULT NULL , `c_15` varchar(32) DEFAULT NULL , `c_16` varchar(4) DEFAULT NULL , `c_17` varchar(4) DEFAULT NULL , `c_18` varchar(100) DEFAULT NULL , `c_19` varchar(50) DEFAULT NULL , `c_20` varchar(1) DEFAULT NULL , `c_21` varchar(2) DEFAULT NULL , `c_22` varchar(3) DEFAULT NULL , `c_23` varchar(19) DEFAULT NULL , `c_24` varchar(2) DEFAULT NULL , `c_25` varchar(4) DEFAULT NULL , `c_26` varchar(6) DEFAULT NULL , `c_27` varchar(7) DEFAULT NULL , `c_28` varchar(5) DEFAULT NULL , `c_29` varchar(120) DEFAULT NULL , `c_30` date DEFAULT NULL , `c_31` varchar(1) DEFAULT NULL , `c_32` varchar(6) DEFAULT NULL , `c_33` varchar(10) DEFAULT NULL , `c_34` varchar(2) DEFAULT NULL , `c_35` varchar(6) DEFAULT NULL , `c_36` varchar(6) DEFAULT NULL , `c_37` date DEFAULT NULL , `c_38` varchar(12) DEFAULT NULL , `c_39` varchar(3) DEFAULT NULL , `c_40` decimal(14,6) DEFAULT NULL , `c_41` varchar(1) DEFAULT NULL , `c_134` varchar(1) DEFAULT NULL , `c_43` decimal(17,3) DEFAULT NULL , `c_44` decimal(17,3) DEFAULT NULL , `c_45` decimal(17,3) DEFAULT NULL , `c_46` decimal(17,3) DEFAULT NULL , `c_47` decimal(17,3) DEFAULT NULL , `c_48` decimal(17,3) DEFAULT NULL , `c_49` decimal(17,3) DEFAULT NULL , `c_50` decimal(17,3) DEFAULT NULL , `c_51` decimal(17,3) DEFAULT NULL , `c_52` decimal(17,3) DEFAULT NULL , `c_53` decimal(17,3) DEFAULT NULL , `c_54` decimal(17,3) DEFAULT NULL , `c_55` decimal(17,3) DEFAULT NULL , `c_56` decimal(17,3) DEFAULT NULL , `c_57` decimal(17,3) DEFAULT NULL , `c_58` varchar(160) DEFAULT NULL , `c_59` varchar(200) DEFAULT NULL , `c_60` varchar(225) DEFAULT NULL , `c_61` varchar(140) DEFAULT NULL , `c_62` varchar(1) DEFAULT NULL , `c_63` varchar(200) DEFAULT NULL , `c_64` varchar(19) DEFAULT NULL , `c_65` varchar(35) DEFAULT NULL , `c_66` varchar(4) DEFAULT NULL , `c_67` varchar(3) DEFAULT NULL , `c_68` varchar(2) DEFAULT NULL , `c_69` varchar(120) DEFAULT NULL , `c_70` varchar(14) DEFAULT NULL , `c_71` varchar(200) DEFAULT NULL , `c_72` varchar(19) DEFAULT NULL , `c_73` varchar(35) DEFAULT NULL , `c_74` varchar(4) DEFAULT NULL , `c_75` varchar(3) DEFAULT NULL , `c_76` varchar(2) DEFAULT NULL , `c_77` varchar(120) DEFAULT NULL , `c_78` varchar(14) DEFAULT NULL , `c_79` varchar(200) DEFAULT NULL , `c_80` varchar(19) DEFAULT NULL , `c_81` varchar(35) DEFAULT NULL , `c_82` varchar(4) DEFAULT NULL , `c_83` varchar(3) DEFAULT NULL , `c_84` varchar(2) DEFAULT NULL , `c_85` varchar(120) DEFAULT NULL , `c_86` varchar(14) DEFAULT NULL , `c_87` varchar(200) DEFAULT NULL , `c_88` varchar(19) DEFAULT NULL , `c_89` varchar(35) DEFAULT NULL , `c_90` varchar(4) DEFAULT NULL , `c_91` varchar(3) DEFAULT NULL , `c_92` varchar(2) DEFAULT NULL , `c_93` varchar(120) DEFAULT NULL , `c_94` varchar(14) DEFAULT NULL , `c_95` varchar(4) DEFAULT NULL , `c_96` varchar(40) DEFAULT NULL , `c_97` varchar(4) DEFAULT NULL , `c_98` varchar(40) DEFAULT NULL , `c_99` varchar(35) DEFAULT NULL , `c_100` varchar(4) DEFAULT NULL , `c_101` int(2) DEFAULT NULL , `c_102` decimal(17,3) DEFAULT NULL , `c_103` decimal(17,3) DEFAULT NULL , `c_104` varchar(256) DEFAULT NULL , `c_105` varchar(256) DEFAULT NULL , `c_106` varchar(64) DEFAULT NULL , `c_107` varchar(64) DEFAULT NULL , `c_108` varchar(64) DEFAULT NULL , `c_109` varchar(64) DEFAULT NULL , `c_110` varchar(32) DEFAULT NULL , `c_111` varchar(32) DEFAULT NULL , `c_112` varchar(50) DEFAULT NULL , `c_113` varchar(100) DEFAULT NULL , `c_114` varchar(1) DEFAULT NULL , `c_115` varchar(1) DEFAULT NULL , `c_116` varchar(1) DEFAULT NULL , `c_117` varchar(1) DEFAULT NULL , `c_118` varchar(1) DEFAULT NULL , `c_119` varchar(10) DEFAULT NULL , `c_120` varchar(1) DEFAULT NULL , `c_121` varchar(1) DEFAULT NULL , `c_122` varchar(2000) DEFAULT NULL , `c_123` varchar(1) NOT NULL , `c_124` varchar(10) DEFAULT NULL , `c_125` varchar(18) DEFAULT NULL , `c_126` date DEFAULT NULL , `c_127` varchar(6) DEFAULT NULL , PRIMARY KEY (`c_128`,`c_1`,`c_2`,`c_4`) /*T![clustered_index] NONCLUSTERED */, KEY `c_129` (`c_15`,`c_134`), KEY `c_130` (`c_128`,`c_1`,`c_2`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=6 PRE_SPLIT_REGIONS=6 */")
	tk.MustExec("CREATE TABLE `t_131`( `c_136` bigint(20) unsigned NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */ , `c_132` date NOT NULL , `c_138` varchar(32) NOT NULL , `c_134` varchar(1) NOT NULL , `c_135` varchar(10) DEFAULT NULL , PRIMARY KEY (`c_136`) /*T![clustered_index] CLUSTERED */, KEY `c_137` (`c_132`,`c_138`,`c_134`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ")
	//tk.MustExec("alter table t_131 add index idx_old_n2  (`c_138`,`c_134`,`c_132`,`c_135`);")

	sql := "SELECT /*+ INL_JOIN(TXN) */ TTBI.c_132, TTBI.c_15, TTBI.c_134, TTBI.c_127, TTBI.c_126 FROM t_0 TTBI WHERE TTBI.c_126 >= STR_TO_DATE('20221020', '%Y%m%d') AND SUBSTR(TTBI.c_15, 1, 2) = 'JJ' AND NOT EXISTS( SELECT 1 FROM t_131 TXN WHERE TXN.c_138 = TTBI.c_15 AND TXN.c_134 = TTBI.c_134 AND TXN.c_132 = TTBI.c_132 AND SUBSTR(TXN.c_138, 1, 2) = 'JJ' AND c_135 = '600000') AND TTBI._tidb_rowid BETWEEN 1 AND 1000"

	rows := [][]interface{}{
		{"Projection_8"},
		{"└─HashJoin_25"},
		{"  ├─TableReader_31(Build)"},
		{"  │ └─Selection_30"},
		{"  │   └─TableFullScan_29"},
		{"  └─TableReader_28(Probe)"},
		{"    └─Selection_27"},
		{"      └─TableRangeScan_26"},
	}
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = false
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)

	rows = [][]interface{}{
		{"Projection_8"},
		{"└─HashJoin_25"},
		{"  ├─TableReader_31(Build)"},
		{"  │ └─Selection_30"},
		{"  │   └─TableFullScan_29"},
		{"  └─TableReader_28(Probe)"},
		{"    └─Selection_27"},
		{"      └─TableRangeScan_26"},
	}
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = true
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)
	// There are no matching table names for (TXN) in optimizer hint /*+ INL_JOIN(txn) */ or /*+ TIDB_INLJ(txn) */. Maybe you can use the table alias name
}

func TestIndexJoinCases6(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION'")
	tk.MustExec("CREATE TABLE `t_0`( `c_30` date NOT NULL , `c_1` varchar(1) NOT NULL , `c_2` varchar(35) NOT NULL , `c_3` varchar(35) DEFAULT NULL , `c_4` varchar(20) DEFAULT NULL , `c_5` varchar(5) DEFAULT NULL , `c_6` varchar(120) DEFAULT NULL , `c_7` varchar(80) DEFAULT NULL , `c_8` varchar(5) NOT NULL , `c_189` varchar(35) DEFAULT NULL , `c_10` varchar(20) DEFAULT NULL , `c_11` varchar(5) DEFAULT NULL , `c_12` varchar(120) DEFAULT NULL , `c_13` varchar(80) DEFAULT NULL , `c_190` varchar(4) DEFAULT NULL , `c_193` varchar(3) NOT NULL , `c_194` varchar(2) NOT NULL , `c_195` varchar(3) DEFAULT NULL , `c_191` char(1) DEFAULT NULL , `c_19` char(1) DEFAULT NULL , `c_20` varchar(35) NOT NULL , `c_21` varchar(1) NOT NULL , `c_22` decimal(21,3) DEFAULT NULL , `c_23` decimal(21,3) DEFAULT NULL , `c_24` datetime DEFAULT NULL , `c_25` date DEFAULT NULL , `c_26` varchar(4) DEFAULT NULL , `c_49` varchar(4) DEFAULT NULL , `c_28` varchar(50) DEFAULT NULL , `c_29` char(1) DEFAULT NULL , PRIMARY KEY (`c_30`,`c_1`,`c_8`,`c_193`,`c_194`,`c_20`) /*T![clustered_index] NONCLUSTERED */, KEY `c_31` (`c_2`), KEY `c_32` (`c_189`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=8 PRE_SPLIT_REGIONS=8 */")
	tk.MustExec("CREATE TABLE `t_33`( `c_33` decimal(22,0) DEFAULT NULL , `c_34` decimal(22,0) NOT NULL , `c_35` varchar(3) DEFAULT NULL , `c_192` varchar(5) DEFAULT NULL , `c_37` varchar(18) DEFAULT NULL , `c_187` date NOT NULL , `c_39` varchar(9) DEFAULT NULL , `c_40` varchar(9) DEFAULT NULL , `c_41` varchar(5) DEFAULT NULL , `c_185` varchar(18) NOT NULL , `c_43` varchar(4) DEFAULT NULL , `c_44` int(3) DEFAULT NULL , `c_45` int(2) DEFAULT NULL , `c_46` varchar(12) DEFAULT NULL , `c_47` varchar(32) DEFAULT NULL , `c_48` varchar(4) DEFAULT NULL , `c_49` varchar(4) DEFAULT NULL , `c_50` varchar(30) DEFAULT NULL , `c_51` varchar(50) DEFAULT NULL , `c_52` varchar(1) DEFAULT NULL , `c_53` varchar(2) DEFAULT NULL , `c_54` varchar(3) DEFAULT NULL , `c_55` varchar(19) DEFAULT NULL , `c_56` varchar(2) DEFAULT NULL , `c_57` varchar(1) DEFAULT NULL , `c_58` varchar(4) DEFAULT NULL , `c_59` varchar(50) DEFAULT NULL , `c_60` time DEFAULT NULL , `c_61` varchar(7) DEFAULT NULL , `c_62` varchar(5) DEFAULT NULL , `c_63` varchar(120) DEFAULT NULL , `c_64` date DEFAULT NULL , `c_65` varchar(1) DEFAULT NULL , `c_66` varchar(10) DEFAULT NULL , `c_67` varchar(10) DEFAULT NULL , `c_68` varchar(2) DEFAULT NULL , `c_69` varchar(6) DEFAULT NULL , `c_70` varchar(6) DEFAULT NULL , `c_71` date DEFAULT NULL , `c_72` varchar(16) DEFAULT NULL , `c_73` varchar(3) DEFAULT NULL , `c_74` varchar(3) DEFAULT NULL , `c_75` decimal(14,6) DEFAULT NULL , `c_76` varchar(1) DEFAULT NULL , `c_77` varchar(1) DEFAULT NULL , `c_78` decimal(21,3) DEFAULT NULL , `c_79` decimal(21,3) DEFAULT NULL , `c_80` decimal(21,3) DEFAULT NULL , `c_81` decimal(21,3) DEFAULT NULL , `c_82` decimal(21,3) DEFAULT NULL , `c_83` decimal(21,3) DEFAULT NULL , `c_84` decimal(21,3) DEFAULT NULL , `c_85` decimal(21,3) DEFAULT NULL , `c_86` decimal(21,3) DEFAULT NULL , `c_87` decimal(21,3) DEFAULT NULL , `c_88` decimal(21,3) DEFAULT NULL , `c_89` decimal(21,3) DEFAULT NULL , `c_90` decimal(21,3) DEFAULT NULL , `c_91` decimal(21,3) DEFAULT NULL , `c_92` decimal(21,3) DEFAULT NULL , `c_93` varchar(1000) DEFAULT NULL , `c_94` varchar(200) DEFAULT NULL , `c_95` varchar(225) DEFAULT NULL , `c_96` varchar(140) DEFAULT NULL , `c_191` varchar(1) DEFAULT NULL , `c_98` varchar(6) DEFAULT NULL , `c_99` varchar(200) DEFAULT NULL , `c_100` varchar(19) DEFAULT NULL , `c_101` varchar(35) DEFAULT NULL , `c_102` varchar(4) DEFAULT NULL , `c_103` int(3) DEFAULT NULL , `c_104` int(2) DEFAULT NULL , `c_105` varchar(210) DEFAULT NULL , `c_106` varchar(14) DEFAULT NULL , `c_107` varchar(5) DEFAULT NULL , `c_108` varchar(200) DEFAULT NULL , `c_109` varchar(19) DEFAULT NULL , `c_110` varchar(35) DEFAULT NULL , `c_111` varchar(4) DEFAULT NULL , `c_112` int(3) DEFAULT NULL , `c_113` int(2) DEFAULT NULL , `c_114` varchar(210) DEFAULT NULL , `c_115` varchar(14) DEFAULT NULL , `c_116` varchar(5) DEFAULT NULL , `c_117` varchar(200) DEFAULT NULL , `c_118` varchar(19) DEFAULT NULL , `c_119` varchar(35) DEFAULT NULL , `c_120` varchar(4) DEFAULT NULL , `c_121` int(3) DEFAULT NULL , `c_122` int(2) DEFAULT NULL , `c_123` varchar(210) DEFAULT NULL , `c_124` varchar(14) DEFAULT NULL , `c_125` varchar(5) DEFAULT NULL , `c_126` varchar(200) DEFAULT NULL , `c_127` varchar(19) DEFAULT NULL , `c_128` varchar(35) DEFAULT NULL , `c_129` varchar(4) DEFAULT NULL , `c_130` int(3) DEFAULT NULL , `c_131` int(2) DEFAULT NULL , `c_132` varchar(210) DEFAULT NULL , `c_133` varchar(14) DEFAULT NULL , `c_134` varchar(5) DEFAULT NULL , `c_135` varchar(4) DEFAULT NULL , `c_136` varchar(30) DEFAULT NULL , `c_137` varchar(40) DEFAULT NULL , `c_138` varchar(4) DEFAULT NULL , `c_139` varchar(30) DEFAULT NULL , `c_140` varchar(40) DEFAULT NULL , `c_141` varchar(35) DEFAULT NULL , `c_142` varchar(4) DEFAULT NULL , `c_143` int(22) DEFAULT NULL , `c_144` decimal(21,3) DEFAULT NULL , `c_145` decimal(21,3) DEFAULT NULL , `c_146` varchar(500) DEFAULT NULL , `c_147` varchar(500) DEFAULT NULL , `c_148` varchar(64) DEFAULT NULL , `c_149` varchar(64) DEFAULT NULL , `c_150` varchar(64) DEFAULT NULL , `c_151` varchar(64) DEFAULT NULL , `c_152` varchar(40) DEFAULT NULL , `c_153` varchar(40) DEFAULT NULL , `c_154` varchar(6) DEFAULT NULL , `c_155` varchar(5) DEFAULT NULL , `c_156` date DEFAULT NULL , `c_157` varchar(4) DEFAULT NULL , `c_158` varchar(30) DEFAULT NULL , `c_159` varchar(1) DEFAULT NULL , `c_160` varchar(1) DEFAULT NULL , `c_161` varchar(18) DEFAULT NULL , `c_162` varchar(5) DEFAULT NULL , `c_163` varchar(16) DEFAULT NULL , `c_164` varchar(16) DEFAULT NULL , `c_165` varchar(35) DEFAULT NULL , `c_166` varchar(200) DEFAULT NULL , `c_167` datetime DEFAULT NULL , `c_168` varchar(16) DEFAULT NULL , `c_169` varchar(2) DEFAULT NULL , `c_170` varchar(50) DEFAULT NULL , `c_171` varchar(100) DEFAULT NULL , `c_172` varchar(1) DEFAULT NULL , `c_173` varchar(1) DEFAULT NULL , `c_174` varchar(1) DEFAULT NULL , `c_175` varchar(1) DEFAULT NULL , `c_176` varchar(1) DEFAULT NULL , `c_177` varchar(10) DEFAULT NULL , `c_178` varchar(1) DEFAULT NULL , `c_179` varchar(1) DEFAULT NULL , `c_180` varchar(70) DEFAULT NULL , `c_181` varchar(70) DEFAULT NULL , `c_182` varchar(70) DEFAULT NULL , `c_183` varchar(70) DEFAULT NULL , `c_184` varchar(1000) DEFAULT NULL , PRIMARY KEY (`c_185`,`c_187`,`c_34`) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin PARTITION BY RANGE COLUMNS(`c_187`) (PARTITION `p_20220601` VALUES LESS THAN (\"2022-06-01\"), PARTITION `p_20221202` VALUES LESS THAN (\"2022-12-03\"), PARTITION `p_20230603` VALUES LESS THAN (\"2023-06-04\"))")
	tk.MustExec("CREATE TABLE `t_188`( `c_200` bigint(20) NOT NULL AUTO_INCREMENT, `c_189` varchar(35) DEFAULT NULL , `c_190` varchar(4) DEFAULT NULL , `c_191` char(1) DEFAULT NULL , `c_192` varchar(5) DEFAULT NULL , `c_193` varchar(3) DEFAULT NULL , `c_194` varchar(2) DEFAULT NULL , `c_195` varchar(3) DEFAULT NULL , `c_196` decimal(21,3) NOT NULL , `c_197` decimal(21,3) NOT NULL , `c_198` decimal(21,3) NOT NULL , `c_199` decimal(21,3) NOT NULL , PRIMARY KEY (`c_200`) /*T![clustered_index] CLUSTERED */, KEY `c_201` (`c_189`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=60001")

	sql := "insert into t_188(c_189, c_191, c_192, c_193, c_194, c_195, c_196, c_197, c_198, c_199) SELECT /*+ inl_join(txn1) */ ACCT.c_189, ACCT.c_191, ACCT.c_8, ACCT.c_193, ACCT.c_194, ACCT.c_195, SUM(CASE WHEN ((TXN1.c_76 = 'C' and TXN1.c_77 = '0') or (TXN1.c_76 = 'D' and TXN1.c_77 = '1')) THEN TXN1.c_78 ELSE 0 END)C_AMT, SUM(CASE WHEN ((TXN1.c_76 = 'D' AND TXN1.c_77 = '0') OR (TXN1.c_76 = 'C' AND TXN1.c_77 = '1')) THEN TXN1.c_78 ELSE 0 END )D_AMT, SUM(CASE WHEN ((TXN1.c_76 = 'C' AND TXN1.c_77 = '0') OR (TXN1.c_76 = 'D' AND TXN1.c_77 = '1')) THEN 1 ELSE 0 END )C_CNT, SUM(CASE WHEN ((TXN1.c_76 = 'D' AND TXN1.c_77 = '0') OR (TXN1.c_76 = 'C' AND TXN1.c_77 = '1')) THEN 1 ELSE 0 END )D_CNT FROM t_0 ACCT inner join t_33 TXN1 ON TXN1.c_187 = STR_TO_DATE('20230203', '%Y%m%d') AND ACCT.c_30 = STR_TO_DATE('20230203', '%Y%m%d') AND TXN1.c_185 = ACCT.c_189 AND TXN1.c_74 = ACCT.c_195 AND case when TRIM(TXN1.c_191) is null or TRIM(TXN1.c_191) = '' then 0 else TRIM(TXN1.c_191) end = ACCT.c_191 AND TXN1.c_44 = cast(case when TRIM(ACCT.c_193) is null or TRIM(ACCT.c_193) = '' then 0 else TRIM(ACCT.c_193) end as unsigned int) AND TXN1.c_45 = cast(case when TRIM(ACCT.c_194) is null or TRIM(ACCT.c_194) = '' then 0 else TRIM(ACCT.c_194) end as unsigned int) WHERE ACCT.c_189 = '123456789' AND ACCT.c_2 = '12345' AND ACCT.c_193 = 'ddee' AND ACCT.c_194 = '1' AND ACCT.c_195 = '012' AND ACCT.c_191 = '0'"
	rows := [][]interface{}{
		{"Insert_1"},
		{"└─Projection_11"},
		{"  └─StreamAgg_13"},
		{"    └─Projection_41"},
		{"      └─HashJoin_15"},
		{"        ├─IndexLookUp_23(Build)"},
		{"        │ ├─Selection_21(Build)"},
		{"        │ │ └─IndexRangeScan_19"},
		{"        │ └─Selection_22(Probe)"},
		{"        │   └─TableRowIDScan_20"},
		{"        └─Selection_40(Probe)"},
		{"          └─IndexLookUp_39"},
		{"            ├─IndexRangeScan_36(Build)"},
		{"            └─Selection_38(Probe)"},
		{"              └─TableRowIDScan_37"},
	}
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = false
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)

	rows = [][]interface{}{
		{"Insert_1"},
		{"└─Projection_11"},
		{"  └─StreamAgg_13"},
		{"    └─Projection_41"},
		{"      └─HashJoin_15"},
		{"        ├─IndexLookUp_23(Build)"},
		{"        │ ├─Selection_21(Build)"},
		{"        │ │ └─IndexRangeScan_19"},
		{"        │ └─Selection_22(Probe)"},
		{"        │   └─TableRowIDScan_20"},
		{"        └─Selection_40(Probe)"},
		{"          └─IndexLookUp_39"},
		{"            ├─IndexRangeScan_36(Build)"},
		{"            └─Selection_38(Probe)"},
		{"              └─TableRowIDScan_37"},
	}
	tk.Session().GetSessionVars().EnableIndexJoinInnerSideMultiPattern = true
	tk.MustQuery("explain "+sql).CheckAt([]int{0}, rows)
}
