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

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/pingcap/tidb/tests/realtikvtest/addindextestutil"
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
	ctx := addindextestutil.InitTest(t)
	addindextestutil.TestOneColFrame(ctx, colIDs, addindextestutil.AddIndexNonUnique)
}

func TestCreateUniqueIndex(t *testing.T) {
	var colIDs [][]int = [][]int{
		{1, 6, 7, 8, 11, 13, 15, 16, 18, 19, 22, 26},
		{2, 9, 11, 17},
		{3, 12, 25},
	}
	ctx := addindextestutil.InitTest(t)
	addindextestutil.TestOneColFrame(ctx, colIDs, addindextestutil.AddIndexUnique)
}

func TestCreatePrimaryKey(t *testing.T) {
	ctx := addindextestutil.InitTest(t)
	addindextestutil.TestOneIndexFrame(ctx, 0, addindextestutil.AddIndexPK)
}

func TestCreateGenColIndex(t *testing.T) {
	ctx := addindextestutil.InitTest(t)
	addindextestutil.TestOneIndexFrame(ctx, 29, addindextestutil.AddIndexGenCol)
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
	ctx := addindextestutil.InitTest(t)
	addindextestutil.TestTwoColsFrame(ctx, coliIDs, coljIDs, addindextestutil.AddIndexMultiCols)
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
	tk.MustExec("update employee set pid=null where id=1")
	tk.MustExec("insert into employee (pid) select pid from employee")
	tk.MustExec("update employee set pid=id-1 where id>1 and pid is null")
	tk.MustExec("alter table employee add foreign key fk_1(pid) references employee(id)")
}

func TestIssue51162(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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
	('[\"01qHMy8yycbq2v1CZ80kiIECEQFLXYsnM0tpVh9vbx71DMpaPdp6hBkrpTodkXVt\", \"dGugZHV3pmqjLSdICAYbiwFGZsYWX9MrzgSGCslmEsfFzXYOiXyE0YMS1TSfnX5R\", \"Ypz8Jj0hBSBQbiWd3LbPRV2qvSEAxV8rlXnxpn93nXmFyMJkCQzyhtNn0FUwH6Ce\", \"Yf6kag9ruOT04V8AHT6NYhCNIa2CYZDVg8zWaxxHFm0GPAJh0e6Mc5Upa0EAp1Sr\", \"ljrRnPUdxJNkWxmkn4K25YeFwVtedGtLDrWlWYZyqt2Shpfg4p7jroOwbOZHuHiE\"]',0,'釸7鈡w歀痂暰苕rv)Ts','[5661899762065804824, 3294089805449875423, 2378579521053711229, 6262068190112960080]','Wxup81婱鍋cH爀
	2潺檛楍1N7','2ZNeu2n5-2+Q3qYp','10:14:20'), 
	('[\"iOl0B9dVMoNYk8Qroj6JTwm0MaCJmr5qnQzdjMtaoDar4jGe6q20HAyVu3GacogW\", \"PBy3AXmGhzJMBX3Ow9INdeATLld8bVVjrIUVTlspzpHMZPuFGlEsDeRN6JOLqRLy\", \"jyr0odf7TOhLpuejrfbvsBbjqCchHSKEFZ079c5KUhu0FIuoqm6tBNbWCBb0xb3c\", \"PmPE8oZf73Zwffi97D7AaQM5g0xameUwPvO15LEUPVlF4iiDVsXyGMESSbEzR5L7\", \"4y0WxQIzPw217QeSXCySsK3pGbHdMxx6ETtIZQfGI8v7Pq9Rxhwkslr1ih8M7Dok\"]',0,'','[2142591371645505560, 2975660707908269211]','','aC794Ir-)iJZr&a','02:30:55'), ('[\"je9igPre8L6VdmtEtVDwxCgTqcuSKdqdO4fheHXRJOQLojiHx4F8cXmFpiYq9955\", \"zPjn7sAeMVLLGE3o9lpnZai26ad8qxHNQF6CSsMshFbP2fDuByV3SYxhxkVwIQd3\", \"Etj8PQZe64XWTbBElIEhWA3F3OcytxNMLvxQqr4miDxoK96Q8fGjBg34H3i4y3V5\", \"IDNAQjueBRQyUE285zoPSMhIvVKrxbiXAVvge0iC9AaEFK18Gh3sign84Nu6M3Ar\", \"MK79tEDnyHn2Cmw4W55hEzAsKvqUTZbViv6UyiEqnUE9jpBOnMEDQbq6yQip4qm3\"]',1,')譺
	)RK5','[1760220588316922379, 4921535278728445755, 5623214347361271244, 5272407040485486430, 9017432485293265763]','RLFF','gS','06:27:28');`)

	tk.MustExec("alter table tl add index idx_16(`col_48`,(cast(`col_45` as signed array)),`col_46`(5));")
	tk.MustExec("admin check table tl")
}
