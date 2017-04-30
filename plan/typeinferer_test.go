// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plan_test

import (
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testTypeInferrerSuite{})

type testTypeInferrerSuite struct {
}

func (ts *testTypeInferrerSuite) TestInferType(c *C) {
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	sql := `create table t (
		c_int int,
		c_bigint bigint,
		c_float float,
		c_double double,
		c_decimal decimal,
		c_datetime datetime,
		c_time time,
		c_timestamp timestamp,
		c_char char,
		c_varchar varchar(20),
		c_text text,
		c_binary binary,
		c_varbinary varbinary(20),
		c_blob blob,
		c_set set('a', 'b', 'c'),
		c_enum enum('a', 'b', 'c'))`
	testKit.MustExec(sql)
	tests := []struct {
		expr string
		tp   byte
		chs  string
		flag int
	}{
		{"c_int", mysql.TypeLong, charset.CharsetBin, mysql.BinaryFlag},
		{"+1", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"-1", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"-'1'", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"-curtime()", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"-now()", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"~1", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag | mysql.UnsignedFlag},
		{"1e0", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"1.0", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},
		{"!true", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},

		{"c_int is true", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"c_double is null", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"isnull(1/0)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"cast(1 as decimal)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},

		{"1 and 1", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"1 or 1", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"1 xor 1", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},

		{"'1' & 2", mysql.TypeLonglong, charset.CharsetBin, mysql.UnsignedFlag | mysql.BinaryFlag},
		{"'1' | 2", mysql.TypeLonglong, charset.CharsetBin, mysql.UnsignedFlag | mysql.BinaryFlag},
		{"'1' ^ 2", mysql.TypeLonglong, charset.CharsetBin, mysql.UnsignedFlag | mysql.BinaryFlag},
		{"'1' << 1", mysql.TypeLonglong, charset.CharsetBin, mysql.UnsignedFlag | mysql.BinaryFlag},
		{"'1' >> 1", mysql.TypeLonglong, charset.CharsetBin, mysql.UnsignedFlag | mysql.BinaryFlag},

		{"1 + '1'", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"1 + 1.1", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},
		{"now() + 0", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"curtime() + 0", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"now(0) + 0", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"now(2) + 0", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},
		{"now() + 1.1", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},
		{"now() + '1'", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"now(2) + '1'", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"now() + curtime()", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"now() + now()", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"now() + now(2)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},
		{"c_double + now()", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"c_timestamp + 1", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"c_timestamp + 1.1", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},
		{"c_timestamp + '1.1'", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"1.1 + now()", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},
		{"1 + now()", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"1 div 2", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"1 / 2", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},

		{"1 > any (select 1)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"exists (select 1)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"1 in (2, 3)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"'abc' like 'abc'", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"'abc' rlike 'abc'", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"(1+1)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},

		// Functions
		{"version()", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"count(c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"abs()", mysql.TypeNull, charset.CharsetBin, mysql.BinaryFlag},
		{"abs(1)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"abs(1.1)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},
		{"abs(cast(\"20150817015609\" as DATETIME))", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"IF(1>2,2,3)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"IFNULL(1,0)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"POW(2,2)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"POWER(2,2)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"LN(3)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"LOG(3)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"LOG(3, 10)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"LOG2(3)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"LOG10(3)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"SQRT(3)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"PI()", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"PI() + 0.000000000000000000", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"SIN(0)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"COS(0)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"TAN(0)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"COT(1)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"ACOS(1)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"ASIN(1)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"ATAN(1)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"ATAN(0, 1)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"rand()", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"curdate()", mysql.TypeDate, charset.CharsetBin, mysql.BinaryFlag},
		{"current_date()", mysql.TypeDate, charset.CharsetBin, mysql.BinaryFlag},
		{"DATE('2003-12-31 01:02:03')", mysql.TypeDate, charset.CharsetBin, mysql.BinaryFlag},
		{"curtime()", mysql.TypeDuration, charset.CharsetBin, mysql.BinaryFlag},
		{"current_time()", mysql.TypeDuration, charset.CharsetBin, mysql.BinaryFlag},
		{"curtime()", mysql.TypeDuration, charset.CharsetBin, mysql.BinaryFlag},
		{"curtime(2)", mysql.TypeDuration, charset.CharsetBin, mysql.BinaryFlag},
		{"makedate(2017,31)", mysql.TypeDate, charset.CharsetBin, mysql.BinaryFlag},
		{"maketime(12, 15, 30)", mysql.TypeDuration, charset.CharsetBin, mysql.BinaryFlag},
		{"sec_to_time(2378)", mysql.TypeDuration, charset.CharsetBin, mysql.BinaryFlag},
		{"current_timestamp()", mysql.TypeDatetime, charset.CharsetBin, mysql.BinaryFlag},
		{"utc_time()", mysql.TypeDuration, charset.CharsetBin, mysql.BinaryFlag},
		{"utc_time(3)", mysql.TypeDuration, charset.CharsetBin, mysql.BinaryFlag},
		{"utc_timestamp()", mysql.TypeDatetime, charset.CharsetBin, mysql.BinaryFlag},
		{"microsecond('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"second('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"minute('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"hour('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"day('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"week('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"month('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"quarter('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"year('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"dayofweek('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"dayofmonth('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"dayofyear('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"weekday('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"weekofyear('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"yearweek('2009-12-31 23:59:59.000010')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"found_rows()", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"length('tidb')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"is_ipv4('192.168.1.1')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"period_add(199206, 2)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"now()", mysql.TypeDatetime, charset.CharsetBin, mysql.BinaryFlag},
		{"from_unixtime(1447430881)", mysql.TypeDatetime, charset.CharsetBin, mysql.BinaryFlag},
		{"from_unixtime(1447430881, '%Y %D %M %h:%i:%s %x')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"sysdate()", mysql.TypeDatetime, charset.CharsetBin, mysql.BinaryFlag},
		{"dayname('2007-02-03')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"version()", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"database()", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"schema()", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"user()", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"current_user()", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"CONCAT('T', 'i', 'DB')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"CONCAT_WS('-', 'T', 'i', 'DB')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"left('TiDB', 2)", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"right('TiDB', 2)", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"lower('TiDB')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"lcase('TiDB')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"locate('foo', 'foobar')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"position('foo' in 'foobar')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"repeat('TiDB', 3)", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"replace('TiDB', 'D', 'd')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"upper('TiDB')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"ucase('TiDB')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"trim(' TiDB ')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"ltrim(' TiDB')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"rtrim('TiDB ')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"connection_id()", mysql.TypeLonglong, charset.CharsetBin, mysql.UnsignedFlag | mysql.BinaryFlag},
		{"if(1>2, 2, 3)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"case c_int when null then 2 when 2 then 1.1 else 1 END", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},
		{"case c_int when null then 2 when 2 then 'tidb' else 1.1 END", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"case c_int when null then c_char when 2 then c_float else 1.1 END", mysql.TypeString, charset.CharsetUTF8, 0},
		{"case c_int when null then c_binary when 2 then c_float else 1.1 END", mysql.TypeString, charset.CharsetBin, mysql.BinaryFlag},
		// least() is the same as greatest()
		{"greatest('TiDB', 'D', 'd')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"greatest(1.1, 2.2)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},
		{"greatest('TiDB', 3)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag},
		{"greatest(c_decimal, c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"greatest(c_int, c_int)", mysql.TypeLong, charset.CharsetBin, mysql.BinaryFlag},
		{"greatest(c_int, c_double)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"greatest(c_bigint, c_int, c_int)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"greatest(c_varchar, c_text)", mysql.TypeBlob, charset.CharsetUTF8, 0},
		{"greatest(c_binary, c_text)", mysql.TypeBlob, charset.CharsetBin, mysql.BinaryFlag},
		{"greatest(c_varchar, c_varbinary)", mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag},
		{"greatest(c_enum, c_int)", mysql.TypeString, charset.CharsetBin, mysql.BinaryFlag},
		{"greatest(c_set, c_int)", mysql.TypeString, charset.CharsetBin, mysql.BinaryFlag},
		{"greatest(c_enum, c_set)", mysql.TypeString, charset.CharsetUTF8, 0},
		{"interval(1, 2, 3)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"interval(1.0, 2.0, 3.0)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"interval('1', '2', '3')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"round()", mysql.TypeNull, charset.CharsetBin, mysql.BinaryFlag},
		{"round(null, 2)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"round('1.2', 2)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"round(1e2, 2)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"round(1.2, 2)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},
		{"round(true, 2)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"round(1000, 2)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"truncate(null, 2)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"truncate('1.2', 2)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"truncate(1e2, 2)", mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{"truncate(1.2, 2)", mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},
		{"truncate(true, 2)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"truncate(1000, 2)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"hex('TiDB')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"hex(12)", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"unhex('TiDB')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"unhex(12)", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"rpad('TiDB', 12, 'go')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"lpad('TiDB', 12, 'go')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"elt(1, 'TiDB', 17, 'go')", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"export_set(5, 'Y', 'N', ',', 4)", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"bit_length('TiDB')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"char(66)", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"char_length('TiDB')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"crc32('TiDB')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"instr('foobarbar', 'bar')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"timestampdiff(MINUTE,'2003-02-01','2003-05-01 12:05:55')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"sign(0)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"sign(null)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"unix_timestamp()", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"unix_timestamp('2015-11-13 10:20:19')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"to_days('2015-11-13')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"to_days(950501)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"ceiling()", mysql.TypeNull, charset.CharsetBin, mysql.BinaryFlag},
		{"ceiling(1.23)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"ceil()", mysql.TypeNull, charset.CharsetBin, mysql.BinaryFlag},
		{"ceil(1.23)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"floor()", mysql.TypeNull, charset.CharsetBin, mysql.BinaryFlag},
		{"floor(1.23)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"field('foo', null)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"find_in_set('foo', 'foo,bar')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"find_in_set('foo', null)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"find_in_set(null, 'bar')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"conv('TiDB',36,10)", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"timestamp('2003-12-31 12:00:00')", mysql.TypeDatetime, charset.CharsetBin, mysql.BinaryFlag},
		{"timestamp('2003-12-31 12:00:00','12:00:00')", mysql.TypeDatetime, charset.CharsetBin, mysql.BinaryFlag},
		{"timestampadd(WEEK,40,'2003-01-01 01:01:01.000011')", mysql.TypeDatetime, charset.CharsetBin, mysql.BinaryFlag},
		{`aes_encrypt("pingcap", "fit2cloud@2014")`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`aes_decrypt("pingcap", "fit2cloud@2014")`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`md5(123)`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`compress('love')`, mysql.TypeBlob, charset.CharsetBin, mysql.BinaryFlag},
		{`sha1(123)`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`sha(123)`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`sha2(123, 256)`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`uuid()`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`from_base64('YWJj')`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`to_base64('abc')`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`random_bytes(32)`, mysql.TypeVarString, charset.CharsetBin, mysql.BinaryFlag},
		{`coalesce(null, 0)`, mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{`coalesce(null, 0.1)`, mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},
		{`coalesce(1, "1" + 1)`, mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{`coalesce(1, "abc")`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`coalesce(c_int, c_char)`, mysql.TypeString, charset.CharsetUTF8, 0},
		{`coalesce(c_int, c_binary)`, mysql.TypeString, charset.CharsetBin, mysql.BinaryFlag},
		{`coalesce(c_int, c_int)`, mysql.TypeLong, charset.CharsetBin, mysql.BinaryFlag},
		{`any_value("abc")`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`any_value(1)`, mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{`any_value(1.234)`, mysql.TypeNewDecimal, charset.CharsetBin, mysql.BinaryFlag},
		{`degrees(1)`, mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{`radians(90)`, mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{`make_set(1 | 3, "hello", "nice", null, "world")`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`oct(12)`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`exp(1)`, mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{`exp(1.23)`, mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{`exp('1.23')`, mysql.TypeDouble, charset.CharsetBin, mysql.BinaryFlag},
		{`inet_aton('255.255.255.255')`, mysql.TypeLonglong, charset.CharsetBin, mysql.UnsignedFlag | mysql.BinaryFlag},
		{`inet_aton('')`, mysql.TypeLonglong, charset.CharsetBin, mysql.UnsignedFlag | mysql.BinaryFlag},
		{`quote("Don\\'t!")`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`insert("Titanium", 3, 6, "DB")`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`is_ipv6('FE80::AAAA:0000:00C2:0002')`, mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{`format(12332.123456, 4)`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"inet_ntoa(1)", mysql.TypeVarString, charset.CharsetUTF8, 0},
		{`ord('2')`, mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{`ord(2)`, mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{`ord(true)`, mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{`ord(null)`, mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{`bin(1)`, mysql.TypeVarString, charset.CharsetUTF8, 0},
		{"to_seconds('2003-05-01 12:05:55')", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{"to_seconds(950501)", mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{`bit_count(1)`, mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{`time_to_sec("23:59:59")`, mysql.TypeLonglong, charset.CharsetBin, mysql.BinaryFlag},
		{`inet6_aton('FE80::AAAA:0000:00C2:0002')`, mysql.TypeVarString, charset.CharsetUTF8, 0},
	}
	for _, tt := range tests {
		ctx := testKit.Se.(context.Context)
		stmts, err := tidb.Parse(ctx, "select "+tt.expr+" from t")
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0].(*ast.SelectStmt)
		is := sessionctx.GetDomain(ctx).InfoSchema()
		err = plan.ResolveName(stmt, is, ctx)
		c.Assert(err, IsNil)
		plan.InferType(ctx.GetSessionVars().StmtCtx, stmt)
		tp := stmt.GetResultFields()[0].Column.Tp
		chs := stmt.GetResultFields()[0].Column.Charset
		flag := stmt.GetResultFields()[0].Column.Flag
		c.Assert(tp, Equals, tt.tp, Commentf("Tp for %s", tt.expr))
		c.Assert(chs, Equals, tt.chs, Commentf("Charset for %s", tt.expr))
		c.Assert(flag^uint(tt.flag), Equals, uint(0x0), Commentf("Charset for %s", tt.flag))
	}
}

func (s *testTypeInferrerSuite) TestColumnInfoModified(c *C) {
	defer testleak.AfterTest(c)()
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists tab0")
	testKit.MustExec("CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER)")
	testKit.MustExec("SELECT + - (- CASE + col0 WHEN + CAST( col0 AS SIGNED ) THEN col1 WHEN 79 THEN NULL WHEN + - col1 THEN col0 / + col0 END ) * - 16 FROM tab0")
	ctx := testKit.Se.(context.Context)
	is := sessionctx.GetDomain(ctx).InfoSchema()
	tbl, _ := is.TableByName(model.NewCIStr("test"), model.NewCIStr("tab0"))
	col := table.FindCol(tbl.Cols(), "col1")
	c.Assert(col.Tp, Equals, mysql.TypeLong)
}

func newStoreWithBootstrap() (kv.Storage, error) {
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, err = tidb.BootstrapSession(store)
	return store, errors.Trace(err)
}
