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

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit"
)

func TestCharsetFeature(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("set names gbk")
	tk.MustQuery("select @@character_set_connection").Check(testkit.Rows("gbk"))
	tk.MustQuery("select @@collation_connection").Check(testkit.Rows("gbk_chinese_ci"))
	tk.MustExec("set @@character_set_client=gbk")
	tk.MustQuery("select @@character_set_client").Check(testkit.Rows("gbk"))
	tk.MustExec("set names utf8mb4")
	tk.MustExec("set @@character_set_connection=gbk")
	tk.MustQuery("select @@character_set_connection").Check(testkit.Rows("gbk"))
	tk.MustQuery("select @@collation_connection").Check(testkit.Rows("gbk_chinese_ci"))

	tk.MustGetErrCode("select _gbk 'a'", errno.ErrUnknownCharacterSet)

	tk.MustExec("use test")
	tk.MustExec("create table t1(a char(10) charset gbk)")
	tk.MustExec("create table t2(a char(10) charset gbk collate gbk_bin)")
	tk.MustExec("create table t3(a char(10)) charset gbk")
	tk.MustExec("alter table t3 add column b char(10) charset gbk")
	tk.MustQuery("show create table t3").Check(testkit.Rows("t3 CREATE TABLE `t3` (\n" +
		"  `a` char(10) DEFAULT NULL,\n" +
		"  `b` char(10) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci",
	))
	tk.MustExec("create table t4(a char(10))")
	tk.MustExec("alter table t4 add column b char(10) charset gbk")
	tk.MustQuery("show create table t4").Check(testkit.Rows("t4 CREATE TABLE `t4` (\n" +
		"  `a` char(10) DEFAULT NULL,\n" +
		"  `b` char(10) CHARACTER SET gbk COLLATE gbk_chinese_ci DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec("create table t5(a char(20), b char(20) charset utf8, c binary) charset gbk collate gbk_bin")

	tk.MustExec("create database test_gbk charset gbk")
	tk.MustExec("use test_gbk")
	tk.MustExec("create table t1(a char(10))")
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" +
		"  `a` char(10) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci",
	))
}

func TestCharsetFeatureCollation(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t" +
		"(ascii_char char(10) character set ascii," +
		"gbk_char char(10) character set gbk collate gbk_bin," +
		"latin_char char(10) character set latin1," +
		"utf8mb4_char char(10) character set utf8mb4)",
	)
	tk.MustExec("insert into t values ('a', 'a', 'a', 'a'), ('a', '啊', '€', 'ㅂ')")
	tk.MustQuery("select collation(concat(ascii_char, gbk_char)) from t").Check(testkit.Rows("gbk_bin", "gbk_bin"))
	tk.MustQuery("select collation(concat(gbk_char, ascii_char)) from t").Check(testkit.Rows("gbk_bin", "gbk_bin"))
	tk.MustQuery("select collation(concat(utf8mb4_char, gbk_char)) from t").Check(testkit.Rows("utf8mb4_bin", "utf8mb4_bin"))
	tk.MustQuery("select collation(concat(gbk_char, utf8mb4_char)) from t").Check(testkit.Rows("utf8mb4_bin", "utf8mb4_bin"))
	tk.MustQuery("select collation(concat('啊', convert('啊' using gbk) collate gbk_bin))").Check(testkit.Rows("gbk_bin"))
	tk.MustQuery("select collation(concat(_latin1 'a', convert('啊' using gbk) collate gbk_bin))").Check(testkit.Rows("gbk_bin"))

	tk.MustGetErrCode("select collation(concat(latin_char, gbk_char)) from t", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select collation(concat(convert('€' using latin1), convert('啊' using gbk) collate gbk_bin))", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select collation(concat(utf8mb4_char, gbk_char collate gbk_bin)) from t", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select collation(concat('ㅂ', convert('啊' using gbk) collate gbk_bin))", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select collation(concat(ascii_char collate ascii_bin, gbk_char)) from t", mysql.ErrCantAggregate2collations)
}

func TestCharsetWithPrefixIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a char(20) charset gbk, b char(20) charset gbk, primary key (a(2)))")
	tk.MustExec("insert into t values ('a', '中文'), ('中文', '中文'), ('一二三', '一二三'), ('b', '一二三')")
	tk.MustQuery("select * from t").Check(testkit.Rows("a 中文", "中文 中文", "一二三 一二三", "b 一二三"))
	tk.MustExec("drop table t")
	tk.MustExec("create table t(a char(20) charset gbk, b char(20) charset gbk, unique index idx_a(a(2)))")
	tk.MustExec("insert into t values ('a', '中文'), ('中文', '中文'), ('一二三', '一二三'), ('b', '一二三')")
	tk.MustQuery("select * from t").Check(testkit.Rows("a 中文", "中文 中文", "一二三 一二三", "b 一二三"))
}
