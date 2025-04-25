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

package addindextest

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/pingcap/tidb/tests/realtikvtest/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Path = "127.0.0.1:2379"
	})
}

func TestCreateNonUniqueIndex(t *testing.T) {
	var colIDs = [][]int{
		{1, 4, 7, 10, 13, 16, 19, 22, 25},
		{2, 5, 8, 11, 14, 17, 20, 23, 26},
		{3, 6, 9, 12, 15, 18, 21, 24, 27},
	}
	ctx := testutils.InitTest(t)
	testutils.TestOneColFrame(ctx, colIDs, testutils.AddIndexNonUnique)
}

func TestCreateUniqueIndex(t *testing.T) {
	var colIDs [][]int = [][]int{
		{1, 6, 7, 8, 11, 13, 15, 16, 18, 19, 22, 26},
		{2, 9, 11, 17},
		{3, 12, 25},
	}
	ctx := testutils.InitTest(t)
	testutils.TestOneColFrame(ctx, colIDs, testutils.AddIndexUnique)
}

func TestCreatePrimaryKey(t *testing.T) {
	ctx := testutils.InitTest(t)
	testutils.TestOneIndexFrame(ctx, 0, testutils.AddIndexPK)
}

func TestCreateGenColIndex(t *testing.T) {
	ctx := testutils.InitTest(t)
	testutils.TestOneIndexFrame(ctx, 29, testutils.AddIndexGenCol)
}

func TestCreateMultiColsIndex(t *testing.T) {
	var coliIDs = [][]int{
		{1, 4, 7},
		{2, 5},
		{3, 6, 9},
	}
	var coljIDs = [][]int{
		{16, 19},
		{14, 17, 20},
		{18, 21},
	}

	if *FullMode {
		coliIDs = [][]int{
			{1, 4, 7, 10, 13},
			{2, 5, 8, 11},
			{3, 6, 9, 12, 15},
		}
		coljIDs = [][]int{
			{16, 19, 22, 25},
			{14, 17, 20, 23, 26},
			{18, 21, 24, 27},
		}
	}
	ctx := testutils.InitTest(t)
	testutils.TestTwoColsFrame(ctx, coliIDs, coljIDs, testutils.AddIndexMultiCols)
}

func TestAddForeignKeyWithAutoCreateIndex(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists fk_index;")
	tk.MustExec("create database fk_index;")
	tk.MustExec("use fk_index;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=1;`)
	tk.MustExec("create table employee (id bigint auto_increment key, pid bigint)")
	tk.MustExec("insert into employee (id) values (1),(2),(3),(4),(5),(6),(7),(8)")
	for i := 0; i < 14; i++ {
		tk.MustExec("insert into employee (pid) select pid from employee")
	}
	tk.MustExec("update employee set pid=id-1 where id>1")
	tk.MustQuery("select count(*) from employee").Check(testkit.Rows("131072"))
	tk.MustExec("alter table employee add foreign key fk_1(pid) references employee(id)")
	tk.MustExec("alter table employee drop foreign key fk_1")
	tk.MustExec("alter table employee drop index fk_1")
	tk.MustExec("update employee set pid=0 where id=1")
	tk.MustGetErrMsg("alter table employee add foreign key fk_1(pid) references employee(id)",
		"[ddl:1452]Cannot add or update a child row: a foreign key constraint fails (`fk_index`.`employee`, CONSTRAINT `fk_1` FOREIGN KEY (`pid`) REFERENCES `employee` (`id`))")
	tk.MustExec("insert into employee (pid) select pid from employee")
	tk.MustExec("update employee set pid=id")

	tk.MustExec("alter table employee add foreign key fk_1(pid) references employee(id)")
}

func TestIssue51162(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global tidb_enable_fast_table_check=0")
	tk.MustExec(`CREATE TABLE tl (
	 col_42 json NOT NULL,
	 col_43 tinyint(1) DEFAULT NULL,
	 col_44 char(168) CHARACTER SET gbk COLLATE gbk_bin DEFAULT NULL,
	 col_45 json DEFAULT NULL,
	 col_46 text COLLATE utf8mb4_unicode_ci NOT NULL,
	 col_47 char(43) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'xW2YNb99pse4)',
	 col_48 time NOT NULL DEFAULT '12:31:25',
	 PRIMARY KEY (col_47,col_46(2)) /*T![clustered_index] CLUSTERED */
	  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`)

	tk.MustExec(`INSERT INTO tl VALUES
	('[\"1\"]',0,'1','[1]','Wxup81','1','10:14:20');`)

	tk.MustExec("alter table tl add index idx_16(`col_48`,(cast(`col_45` as signed array)),`col_46`(5));")
	tk.MustExec("admin check table tl")
}

func TestAddUKWithSmallIntHandles(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists small;")
	tk.MustExec("create database small;")
	tk.MustExec("use small;")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=1;`)
	tk.MustExec("create table t (a bigint, b int, primary key (a) clustered)")
	tk.MustExec("insert into t values (-9223372036854775808, 1),(-9223372036854775807, 1)")
	tk.MustContainErrMsg("alter table t add unique index uk(b)", "Duplicate entry '1' for key 't.uk'")
}

func TestAddUniqueDuplicateIndexes(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_ddl_enable_fast_reorg=1;`)
	tk.MustExec("create table t(a int DEFAULT '-13202', b varchar(221) NOT NULL DEFAULT 'duplicatevalue', " +
		"c int NOT NULL DEFAULT '0');")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")

	tk1.Exec("INSERT INTO t VALUES (-18585,'duplicatevalue',0);")

	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		switch job.SchemaState {
		case model.StateDeleteOnly:
			_, err := tk1.Exec("delete from t where c = 0;")
			assert.NoError(t, err)
			_, err = tk1.Exec("insert INTO t VALUES (-18585,'duplicatevalue',1);")
			assert.NoError(t, err)
		}
	})

	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")
	ingest.MockDMLExecutionStateBeforeImport = func() {
		tk3.MustExec("replace INTO t VALUES (-18585,'duplicatevalue',4);")
		tk3.MustQuery("select * from t;").Check(testkit.Rows("-18585 duplicatevalue 1", "-18585 duplicatevalue 4"))
	}
	ddl.MockDMLExecutionStateBeforeMerge = func() {
		tk3.MustQuery("select * from t;").Check(testkit.Rows("-18585 duplicatevalue 1", "-18585 duplicatevalue 4"))
		tk3.MustExec("replace into t values (-18585,'duplicatevalue',0);")
	}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/ingest/mockDMLExecutionStateBeforeImport", "1*return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionStateBeforeMerge", "return(true)"))
	tk.MustExec("alter table t add unique index idx(b);")
	tk.MustExec("admin check table t;")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/ingest/mockDMLExecutionStateBeforeImport"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockDMLExecutionStateBeforeMerge"))
}

func TestAddIndexOnGB18030Bin(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t (
		a varchar(198) COLLATE gb18030_bin NOT NULL,
		b varchar(178) COLLATE gb18030_bin NOT NULL,
		PRIMARY KEY (b,a),
		KEY k1 (b,a),
		KEY k2 (b)
	) ENGINE=InnoDB DEFAULT CHARSET=gb18030 COLLATE=gb18030_bin;`)
	tk.MustExec("insert into t values ('a', 'b');")
	tk.MustExec("admin check table t;")
}

func TestCommonhandle(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `t3` (`col_1` bigint DEFAULT 1,  `col_2` int NOT NULL,  `col_3` varbinary(23) DEFAULT NULL,  `col_4` timestamp NOT NULL,  `col_5` date NOT NULL DEFAULT '2018-02-16',  `col_6` text DEFAULT NULL,  `col_7` datetime DEFAULT '1971-02-24 00:00:00' ON UPDATE CURRENT_TIMESTAMP,  `col_8` float DEFAULT NULL,  primary key (`col_1`,`col_2`) clustered , KEY `idx_1` (`col_4`),  KEY `idx_2` (`col_4`,`col_5`,`col_8`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("INSERT INTO `t3` VALUES ('1','11',x'61784f4b626f6e7030514c6b','2025-03-27 00:56:13','2023-11-22','xUhXa6=s%j7h~!G','2025-03-27 01:00:18',4497.738);")
	tk.MustExec("CREATE TABLE `t4` ( `col_1` bigint DEFAULT 1, `col_2` int NOT NULL, `col_3` varbinary(23) DEFAULT NULL, `col_4` timestamp NOT NULL, `col_5` date NOT NULL DEFAULT '2018-02-16', `col_6` text DEFAULT NULL, `col_7` datetime DEFAULT '1971-02-24 00:00:00' ON UPDATE CURRENT_TIMESTAMP, `col_8` float DEFAULT NULL, primary key (`col_1`, `col_2`) clustered , KEY `idx_1` (`col_4`), KEY `idx_2` (`col_4`,`col_5`,`col_8`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("INSERT INTO `t4` VALUES ('1','11',x'61784f4b626f6e7030514c6b','2025-03-27 00:56:13','2023-11-22','xUhXa6=s%j7h~!G','2025-03-27 01:00:18',4497.738);")
	tk.MustExec("select /*+ inl_join(st_475) */ ucase( st_475.r4 ) as r0 , concat_ws(',', t3.col_6 , t3.col_3 ) as r1 , lower( t3.col_3 ) as r2 , ucase( st_475.r4 ) as r3 from t3 left join (select /*+  stream_agg() */ t4.col_4 as r0 , max(   t4.col_4 ) as r1 , t4.col_4 as r2 , count(   t4.col_4 ) as r3 , min(   t4.col_3 ) as r4 from t4 where IsNull( t4.col_3 ) group by t4.col_4) st_475 on t3.col_4 = st_475.r0 order by r0,r1,r2,r3;")
}
