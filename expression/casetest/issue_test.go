// Copyright 2018 PingCAP, Inc.
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

package casetest

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestInvalidEnumName(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t01")
	tk.MustExec(`
	CREATE TABLE t01 (
		a timestamp DEFAULT '2024-10-02 01:54:55',
		b int(11) NOT NULL DEFAULT '2023959529',
		c varchar(122) DEFAULT '36h0hvfpylz0f0iv9h0ownfcg3rehi4',
		d enum('l7i9','3sdz3','83','4','92p','4g','8y5rn','7gp','7','1','e') NOT NULL DEFAULT '4',
		PRIMARY KEY (b, d) /*T![clustered_index] CLUSTERED */
	      ) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci COMMENT='7ad99128'
	      PARTITION BY HASH (b) PARTITIONS 9;`)
	tk.MustExec("insert ignore into t01 values ('2023-01-01 20:01:02', 123, 'abcd', '');")
	tk.MustQuery("select `t01`.`d` as r0 from `t01` where `t01`.`a` in ( '2010-05-25') or not( `t01`.`d` > '1' ) ;").Check(testkit.Rows(""))
}

func TestIssue49440(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t047d7221, t3fa8f3ec")
	tk.MustExec("create table t047d7221 (col_14_1 enum('Alice','Bob','Charlie','David') COLLATE utf8mb4_general_ci DEFAULT NULL)")
	tk.MustExec("INSERT INTO `t047d7221` VALUES ('Charlie'),('Charlie'),('David'),('Bob'),('Bob'),('Charlie'),('David'),('Bob'),('Charlie'),('David'),('Bob'),('Bob'),('David'),('Alice'),('David'),('Alice'),('Charlie'),('Charlie'),('David'),('Alice'),('David');")
	tk.MustExec("create table t3fa8f3ec (`col_31` timestamp NOT NULL, `col_32` mediumint(9) DEFAULT '-4350144', `col_33` json DEFAULT NULL, `col_34` time NOT NULL DEFAULT '14:52:13', `col_35` float DEFAULT NULL, `col_36` decimal(14,10) DEFAULT NULL, `col_37` bit(32) DEFAULT NULL, UNIQUE KEY `idx_14` (`col_32`,`col_37`,`col_34`))")
	tk.MustExec("INSERT INTO `t3fa8f3ec` VALUES ('2032-12-07 16:00:00',4171813,'null','06:50:04',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',-562928,'null','06:35:17',628.79083,0.0000000000,_binary '0x1'),('2012-05-10 16:00:00',-5344713,'null','03:29:46',628.79083,0.0000000000,_binary '0x1'),('1983-06-11 16:00:00',3067543,'null','07:07:40',628.79083,0.0000000000,_binary '0x1'),('1979-03-16 16:00:00',5251228,'null','06:21:55',628.79083,0.0000000000,_binary '0x1'),('2008-04-22 16:00:00',-3305758,'null','02:42:21',628.79083,0.0000000000,_binary '0x1'),('2025-03-16 16:00:00',1451903,'null','09:50:08',628.79083,0.0000000000,_binary '0x1'),('2017-03-17 16:00:00',1752413,'null','15:55:09',628.79083,0.0000000000,_binary '0x1'),('2020-03-11 16:00:00',-5845368,'null','03:40:14',628.79083,0.0000000000,_binary '0x1'),('2002-11-27 16:00:00',693868,'null','16:15:51',628.79083,0.0000000000,_binary '0x1'),('2020-10-06 16:00:00',6098278,'null','03:01:46',628.79083,0.0000000000,_binary '0x1'),('2001-01-24 16:00:00',-5515593,'null','09:49:41',628.79083,0.0000000000,_binary '0x1'),('1973-12-09 16:00:00',7401513,'null','14:00:07',628.79083,0.0000000000,_binary '0x1'),('1982-03-19 16:00:00',4056108,'null','19:08:54',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',6734101,'null','05:06:04',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',-2705751,'null','03:18:49',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',3783896,'null','03:03:39',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',7486166,'null','02:47:01',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',-5914941,'null','03:17:47',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',6356646,'null','06:14:33',628.79083,0.0000000000,_binary '0x1'),('2035-05-01 16:00:00',-718476,'null','03:08:09',628.79083,0.0000000000,_binary '0x1'),('1991-03-10 16:00:00',-3825016,'null','11:39:20',628.79083,0.0000000000,_binary '0x1'),('2014-10-05 16:00:00',7724461,'null','18:16:29',628.79083,0.0000000000,_binary '0x1'),('1980-08-13 16:00:00',-1425586,'null','19:32:41',628.79083,0.0000000000,_binary '0x1'),('2009-08-22 16:00:00',-6087216,'null','07:49:31',628.79083,0.0000000000,_binary '0x1'),('2004-02-14 16:00:00',-2440696,'null','06:25:48',628.79083,0.0000000000,_binary '0x1'),('2002-02-02 16:00:00',-3965686,'null','18:36:41',628.79083,0.0000000000,_binary '0x1'),('2018-09-20 16:00:00',-2090316,'null','01:21:13',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',-2719454,'null','06:12:56',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',-2719454,'null','01:04:04',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',-5801694,'null','06:59:41',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',-2719454,'null','03:48:16',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',-2719454,'null','05:13:54',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',-2719454,'null','03:42:29',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',-2719454,'null','01:19:57',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',-2719454,'null','00:55:46',628.79083,0.0000000000,_binary '0x1'),('2010-09-14 16:00:00',4849424,'null','06:02:32',628.79083,0.0000000000,_binary '0x1'),('2002-07-30 16:00:00',6109034,'null','06:33:39',628.79083,0.0000000000,_binary '0x1'),('1971-08-21 16:00:00',5571999,'null','12:13:37',628.79083,0.0000000000,_binary '0x1'),('2032-10-11 16:00:00',3762434,'null','09:10:40',628.79083,0.0000000000,_binary '0x1'),('2005-09-08 16:00:00',-6554119,'null','19:36:37',628.79083,0.0000000000,_binary '0x1'),('1981-01-10 16:00:00',-1179289,'null','09:35:00',628.79083,0.0000000000,_binary '0x1'),('2028-08-22 16:00:00',8316284,'null','08:16:44',628.79083,0.0000000000,_binary '0x1'),('1979-05-17 16:00:00',-5318419,'null','17:59:56',628.79083,0.0000000000,_binary '0x1'),('2027-05-11 16:00:00',-5371444,'null','17:19:10',628.79083,0.0000000000,_binary '0x1'),('2013-12-25 16:00:00',-189564,'null','12:04:41',628.79083,0.0000000000,_binary '0x1'),('2016-01-16 16:00:00',-3987539,'null','02:11:34',628.79083,0.0000000000,_binary '0x1'),('1982-11-11 16:00:00',-852334,'null','03:04:13',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',7925662,'null','03:18:23',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',-2454587,'null','02:55:28',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',5739127,'null','06:03:13',628.79083,0.0000000000,_binary '0x1'),('2003-09-24 16:00:00',7753112,'null','00:20:56',628.79083,0.0000000000,_binary '0x1'),('1974-06-27 16:00:00',5429127,'null','02:58:31',628.79083,0.0000000000,_binary '0x1'),('2019-05-10 16:00:00',-5681972,'null','02:17:08',628.79083,0.0000000000,_binary '0x1'),('1992-02-11 16:00:00',1122337,'null','03:41:03',628.79083,0.0000000000,_binary '0x1'),('2036-11-28 16:00:00',40717,'null','03:30:04',628.79083,0.0000000000,_binary '0x1'),('1985-09-24 16:00:00',-4983092,'null','19:41:50',628.79083,0.0000000000,_binary '0x1'),('1972-04-05 16:00:00',520097,'null','19:24:54',628.79083,0.0000000000,_binary '0x1'),('2023-08-19 16:00:00',396327,'null','22:37:52',628.79083,0.0000000000,_binary '0x1'),('2019-12-27 16:00:00',-4990207,'null','12:18:44',628.79083,0.0000000000,_binary '0x1'),('2011-10-04 16:00:00',-149632,'null','20:59:59',628.79083,0.0000000000,_binary '0x1'),('1979-02-22 16:00:00',1099937,'null','12:28:27',628.79083,0.0000000000,_binary '0x1'),('2033-11-19 16:00:00',1089042,'true','14:37:01',4436.5244,0.0138000000,_binary '0x1'),('2032-12-07 16:00:00',-6008365,'null','00:45:53',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',NULL,'null','03:22:27',628.79083,0.0000000000,_binary '0x1'),('2032-12-07 16:00:00',-3819435,'null','00:03:28',628.79083,0.0000000000,_binary '0x1'),('2012-01-19 16:00:00',7593580,'null','03:17:19',628.79083,0.0000000000,_binary '0x1'),('2007-06-11 16:00:00',-5412530,'null','05:17:36',628.79083,0.0000000000,_binary '0x1'),('2010-05-01 16:00:00',2846505,'null','02:19:05',628.79083,0.0000000000,_binary '0x1'),('1989-03-30 16:00:00',7038365,'null','13:30:27',628.79083,0.0000000000,_binary '0x1'),('2021-01-20 16:00:00',NULL,'null','11:07:47',628.79083,0.0000000000,_binary '0x1'),('2025-07-01 16:00:00',4456660,'null','01:27:15',628.79083,0.0000000000,_binary '0x1'),('2029-02-04 16:00:00',4791195,'null','14:38:02',628.79083,0.0000000000,_binary '0x1'),('1978-08-02 16:00:00',4369375,'null','11:30:30',628.79083,0.0000000000,_binary '0x1'),('1974-11-15 16:00:00',4095160,'null','01:56:26',628.79083,0.0000000000,_binary '0x1'),('2008-03-28 16:00:00',NULL,'null','22:59:27',628.79083,0.0000000000,_binary '0x1');")
	tk.MustQuery("select t047d7221.col_14_1, t3fa8f3ec.col_36 from t047d7221 left join t3fa8f3ec on t047d7221.col_14_1 = t3fa8f3ec.col_36 where t047d7221.col_14_1 in ( 'Charlie' );").Check(testkit.Rows("Charlie <nil>", "Charlie <nil>", "Charlie <nil>", "Charlie <nil>", "Charlie <nil>", "Charlie <nil>"))
	tk.MustExec("insert into t3fa8f3ec values('1982-03-19 16:00:00', 23423, null, '09:49:41', 628.79083, 3.0, 0x00307831);")
	tk.MustQuery("select t047d7221.col_14_1, t3fa8f3ec.col_36 from t047d7221 left join t3fa8f3ec on t047d7221.col_14_1 = t3fa8f3ec.col_36 where t047d7221.col_14_1 in ( 'Charlie' );").Check(testkit.Rows("Charlie 3.0000000000", "Charlie 3.0000000000", "Charlie 3.0000000000", "Charlie 3.0000000000", "Charlie 3.0000000000", "Charlie 3.0000000000"))
	tk.MustExec("insert into t3fa8f3ec values('1982-03-19 16:00:00', 23424, null, '09:49:41', 628.79083, 3.1, 0x00307831);")
	tk.MustQuery("select t047d7221.col_14_1, t3fa8f3ec.col_36 from t047d7221 left join t3fa8f3ec on t047d7221.col_14_1 = t3fa8f3ec.col_36 where t047d7221.col_14_1 in ( 'Charlie' );").Check(testkit.Rows("Charlie 3.0000000000", "Charlie 3.0000000000", "Charlie 3.0000000000", "Charlie 3.0000000000", "Charlie 3.0000000000", "Charlie 3.0000000000"))
}

func Test53726(t *testing.T) {
	// test for RemoveUnnecessaryFirstRow
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t7(c int); ")
	tk.MustExec("insert into t7 values (575932053), (-258025139);")
	tk.MustQuery("select distinct cast(c as decimal), cast(c as signed) from t7").
		Sort().Check(testkit.Rows("-258025139 -258025139", "575932053 575932053"))
	tk.MustQuery("explain select distinct cast(c as decimal), cast(c as signed) from t7").
		Check(testkit.Rows(
			"HashAgg_8 8000.00 root  group by:Column#7, Column#8, funcs:firstrow(Column#7)->Column#3, funcs:firstrow(Column#8)->Column#4",
			"└─TableReader_9 8000.00 root  data:HashAgg_4",
			"  └─HashAgg_4 8000.00 cop[tikv]  group by:cast(test.t7.c, bigint(22) BINARY), cast(test.t7.c, decimal(10,0) BINARY), ",
			"    └─TableFullScan_7 10000.00 cop[tikv] table:t7 keep order:false, stats:pseudo"))

	tk.MustExec("analyze table t7")
	tk.MustQuery("select distinct cast(c as decimal), cast(c as signed) from t7").
		Sort().
		Check(testkit.Rows("-258025139 -258025139", "575932053 575932053"))
	tk.MustQuery("explain select distinct cast(c as decimal), cast(c as signed) from t7").
		Check(testkit.Rows(
			"HashAgg_6 2.00 root  group by:Column#13, Column#14, funcs:firstrow(Column#11)->Column#3, funcs:firstrow(Column#12)->Column#4",
			"└─Projection_12 2.00 root  cast(test.t7.c, decimal(10,0) BINARY)->Column#11, cast(test.t7.c, bigint(22) BINARY)->Column#12, cast(test.t7.c, decimal(10,0) BINARY)->Column#13, cast(test.t7.c, bigint(22) BINARY)->Column#14",
			"  └─TableReader_11 2.00 root  data:TableFullScan_10",
			"    └─TableFullScan_10 2.00 cop[tikv] table:t7 keep order:false"))
}