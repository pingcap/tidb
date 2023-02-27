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
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t" +
		"(ascii_char char(10) character set ascii," +
		"gbk_char char(10) character set gbk collate gbk_bin," +
		"latin_char char(10) character set latin1," +
		"utf8mb4_char char(10) character set utf8mb4," +
		"gb18030_char char(10) character set gb18030)",
	)
	tk.MustExec("insert into t values ('a', 'a', 'a', 'a', 'a'), ('a', '啊', '€', 'ㅂ', '🀁')")
	tk.MustQuery("select collation(concat(ascii_char, gbk_char)) from t").Check(testkit.Rows("gbk_bin", "gbk_bin"))
	tk.MustQuery("select collation(concat(gbk_char, ascii_char)) from t").Check(testkit.Rows("gbk_bin", "gbk_bin"))
	tk.MustQuery("select collation(concat(utf8mb4_char, gbk_char)) from t").Check(testkit.Rows("utf8mb4_bin", "utf8mb4_bin"))
	tk.MustQuery("select collation(concat(gbk_char, utf8mb4_char)) from t").Check(testkit.Rows("utf8mb4_bin", "utf8mb4_bin"))
	tk.MustQuery("select collation(concat(utf8mb4_char, gb18030_char)) from t").Check(testkit.Rows("utf8mb4_bin", "utf8mb4_bin"))
	tk.MustQuery("select collation(concat(gb18030_char, utf8mb4_char)) from t").Check(testkit.Rows("utf8mb4_bin", "utf8mb4_bin"))
	tk.MustGetErrCode("select collation(concat(gbk_char, gb18030_char)) from t", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select collation(concat(gb18030_char, gbk_char)) from t", mysql.ErrCantAggregate2collations)
	tk.MustQuery("select collation(concat('啊', convert('啊' using gbk) collate gbk_bin))").Check(testkit.Rows("gbk_bin"))
	tk.MustQuery("select collation(concat(_latin1 'a', convert('啊' using gbk) collate gbk_bin))").Check(testkit.Rows("gbk_bin"))

	tk.MustGetErrCode("select collation(concat(latin_char, gbk_char)) from t", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select collation(concat(convert('€' using latin1), convert('啊' using gbk) collate gbk_bin))", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select collation(concat(utf8mb4_char, gbk_char collate gbk_bin)) from t", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select collation(concat('ㅂ', convert('啊' using gbk) collate gbk_bin))", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select collation(concat(ascii_char collate ascii_bin, gbk_char)) from t", mysql.ErrCantAggregate2collations)
}

func TestCharsetWithPrefixIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a char(20) charset gbk, b char(20) charset gbk, primary key (a(2)))")
	tk.MustExec("insert into t values ('a', '中文'), ('中文', '中文'), ('一二三', '一二三'), ('b', '一二三')")
	tk.MustQuery("select * from t;").Sort().Check(testkit.Rows("a 中文", "b 一二三", "一二三 一二三", "中文 中文"))
	tk.MustExec("drop table t")
	tk.MustExec("create table t(a char(20) charset gbk, b char(20) charset gbk, unique index idx_a(a(2)))")
	tk.MustExec("insert into t values ('a', '中文'), ('中文', '中文'), ('一二三', '一二三'), ('b', '一二三')")
	tk.MustQuery("select * from t;").Sort().Check(testkit.Rows("a 中文", "b 一二三", "一二三 一二三", "中文 中文"))
}

func TestGB18030(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustQuery("select upper(convert('àáèéêìíòóùúüāēěīńňōūǎǐǒǔǖǘǚǜⅪⅫ' using gb18030))").
		Check(testkit.Rows("ÀÁÈÉÊÌÍÒÓÙÚÜĀĒĚĪŃŇŌŪǍǏǑǓǕǗǙǛⅪⅫ"))
	tk.MustQuery("select lower(convert('àáèéêìíòóùúüāēěīńňōūǎǐǒǔǖǘǚǜⅪⅫ' using gb18030))").
		Check(testkit.Rows("àáèéêìíòóùúüāēěīńňōūǎǐǒǔǖǘǚǜⅺⅻ"))
	tk.MustQuery("select convert(0x1e2 using gb18030)").
		Check(testkit.Rows("<nil>"))
	tk.MustQuery("select char(0x1234 using gb18030)").
		Check(testkit.Rows("\x124"))
	tk.MustQuery("select char(0xd2 using gb18030)").
		Check(testkit.Rows("<nil>"))

	tk.MustExec("create table t (a char(20) charset gb18030)")
	tk.MustExec("create table t1 (a binary(20))")
	tk.MustExec("create table t2 (a char(20) charset gb18030, b char(20) charset gb18030)")

	tk.MustExec("insert into t values ('a'), ('一二三')")
	tk.MustQuery("select hex(a) from t").Check(testkit.Rows("61", "D2BBB6FEC8FD"))
	tk.MustQuery("select hex('ㅂ')").Check(testkit.Rows("E38582"))
	tk.MustQuery("select ascii(a) from t").Check(testkit.Rows("97", "210"))
	tk.MustQuery("select ascii('ㅂ')").Check(testkit.Rows("227"))
	tk.MustQuery("select concat(a, 0x3f) from t").Check(testkit.Rows("a?", "一二三?"))
	tk.MustQuery(`select concat_ws("你", a, a) from t`).Check(testkit.Rows("a你a", "一二三你一二三"))
	tk.MustQuery("select length(a), octet_length(a), bit_length(a) from t").Check(testkit.Rows("1 1 8", "6 6 48"))
	tk.MustQuery("select to_base64(a) from t").Check(testkit.Rows("YQ==", "0ru2/sj9"))
	tk.MustQuery("select lower(a), upper(a) from t").Check(testkit.Rows("a A", "一二三 一二三"))
	tk.MustQuery(`select upper("abcABC一二三abcABC"), lower("abcABC一二三abcABC")`).Check(testkit.Rows("ABCABC一二三ABCABC abcabc一二三abcabc"))
	tk.MustQuery("select ord(a) from t").Check(testkit.Rows("97", "53947"))
	tk.MustQuery("select aes_encrypt(a, 'key') from t").
		Check(testkit.Rows("U)\xfai\xbe:\x14\xb5\xbd\x89R\x00LO\xceZ", "l\x1d*\xb7$\x92\xd2\xf9\xc8*\xe3\x8d\xf3J\\\x13"))
	tk.MustQuery("select aes_decrypt(aes_encrypt(a, 'key'), 'key'), hex(a) from t").Check(testkit.Rows("a 61", "һ\xb6\xfe\xc8\xfd D2BBB6FEC8FD"))
	tk.MustQuery(`select encode(a, "key") from t`).Check(testkit.Rows("\xa2", "\x89\xb1a\xee}\x8c"))
	tk.MustQuery(`select decode(encode(a, "key"), "key"), hex(a) from t`).Check(testkit.Rows("a 61", "һ\xb6\xfe\xc8\xfd D2BBB6FEC8FD"))
	tk.MustQuery(`select md5(a) from t`).Check(testkit.Rows("0cc175b9c0f1b6a831c399e269772661", "a45d4af7b243e7f393fa09bed72ac73e"))
	tk.MustQuery(`select password(a) from t`).Check(testkit.Rows("*667F407DE7C6AD07358FA38DAED7828A72014B4E", "*A669F2B2DD49E2463FE62D8F72DDF4F858687EA5"))
	tk.MustQuery(`select compress(a) from t`).Check(testkit.Rows("\x01\x00\x00\x00x\x9cJ\x04\x04\x00\x00\xff\xff\x00b\x00b", "\x06\x00\x00\x00x\x9c\xba\xb4{ۿ\x13\x7f\x01\x01\x00\x00\xff\xff\x10\xf8\x05\a"))
	tk.MustQuery(`select uncompress(compress(a)), a from t`).Check(testkit.Rows("a a", "һ\xb6\xfe\xc8\xfd 一二三"))
	tk.MustQuery(`select sha1(a), sha2(a, "key") from t`).
		Check(testkit.Rows("86f7e437faa5a7fce15d1ddcb9eaeaea377667b8 ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
			"30cda4eed59a2ff592f2881f39d42fed6e10cad8 b6c1ae1f8d8a07426ddb13fca5124fb0b9f1f0ef1cca6730615099cf198ca8af"))
	tk.MustQuery(`select hex(a) from t where hex(a) = "D2BBB6FEC8FD"`).Check(testkit.Rows("D2BBB6FEC8FD"))
	tk.MustQuery(`select length(a) from t where length(a) = 6`).Check(testkit.Rows("6"))
	tk.MustQuery(`select bit_length(a) from t where bit_length(a) = 48`).Check(testkit.Rows("48"))
	tk.MustQuery(`select ascii(a) from t where ascii(a) = 210`).Check(testkit.Rows("210"))
	tk.MustQuery(`select concat(a, 0x3f) from t where concat(a, 0x3f) = "一二三?"`).Check(testkit.Rows("一二三?"))
	tk.MustQuery(`select md5(a) from t where md5(a) = "a45d4af7b243e7f393fa09bed72ac73e"`).Check(testkit.Rows("a45d4af7b243e7f393fa09bed72ac73e"))
	tk.MustQuery(`select sha1(a) from t where sha1(a) = "30cda4eed59a2ff592f2881f39d42fed6e10cad8"`).Check(testkit.Rows("30cda4eed59a2ff592f2881f39d42fed6e10cad8"))

	tk.MustExec("insert into t1 values (0xe2e2)")
	tk.MustQuery("select convert(a using gb18030) from t1").Check(testkit.Rows("忖\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))

	tk.MustExec(`insert into t2 values ("abc", "abc"), ("abc", "xyz"), ("abc", "qwe"), ("abc","234")`)
	tk.MustExec(`insert into t2 values ("一二三", "一"), ("一二三", "二"), ("一二三", "三"), ("一二三","四")`)
	tk.MustQuery(`select a, b, rank() over (partition by a order by b) as x from t2`).
		Check(testkit.Rows("一二三 二 1", "一二三 三 2", "一二三 四 3", "一二三 一 4", "abc 234 1", "abc abc 2", "abc qwe 3", "abc xyz 4"))
}
