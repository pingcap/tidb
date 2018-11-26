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
// See the License for the specific language governing permissions and
// limitations under the License.

package infoschema_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testSuite) TestInfoschemaFieldValue(c *C) {
	testleak.BeforeTest()
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	session.SetStatsLease(0)
	do, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer do.Close()

	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists numschema, timeschema")
	tk.MustExec("create table numschema(i int(2), f float(4,2), d decimal(4,3))")
	tk.MustExec("create table timeschema(d date, dt datetime(3), ts timestamp(3), t time(4), y year(4))")
	tk.MustExec("create table strschema(c char(3), c2 varchar(3), b blob(3), t text(3))")
	tk.MustExec("create table floatschema(a float, b double(7, 3))")

	tk.MustQuery("select CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION from information_schema.COLUMNS where table_name='numschema'").
		Check(testkit.Rows("<nil> <nil> 2 0 <nil>", "<nil> <nil> 4 2 <nil>", "<nil> <nil> 4 3 <nil>")) // FIXME: for mysql first one will be "<nil> <nil> 10 0 <nil>"
	tk.MustQuery("select CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION from information_schema.COLUMNS where table_name='timeschema'").
		Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>", "<nil> <nil> <nil> <nil> 3", "<nil> <nil> <nil> <nil> 3", "<nil> <nil> <nil> <nil> 4", "<nil> <nil> <nil> <nil> <nil>"))
	tk.MustQuery("select CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION from information_schema.COLUMNS where table_name='strschema'").
		Check(testkit.Rows("3 3 <nil> <nil> <nil>", "3 3 <nil> <nil> <nil>", "3 3 <nil> <nil> <nil>", "3 3 <nil> <nil> <nil>")) // FIXME: for mysql last two will be "255 255 <nil> <nil> <nil>", "255 255 <nil> <nil> <nil>"
	tk.MustQuery("select NUMERIC_SCALE from information_schema.COLUMNS where table_name='floatschema'").
		Check(testkit.Rows("<nil>", "3"))

	// Test for auto increment ID.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int auto_increment primary key, d int)")
	tk.MustQuery("select auto_increment from information_schema.tables where table_name='t'").Check(
		testkit.Rows("1"))
	tk.MustExec("insert into t(c, d) values(1, 1)")
	tk.MustQuery("select auto_increment from information_schema.tables where table_name='t'").Check(
		testkit.Rows("30002"))

	tk.MustExec("create user xxx")
	tk.MustExec("flush privileges")

	// Test for length of enum and set
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t ( s set('a','bc','def','ghij') default NULL, e1 enum('a', 'ab', 'cdef'), s2 SET('1','2','3','4','1585','ONE','TWO','Y','N','THREE'))")
	tk.MustQuery("select column_name, character_maximum_length from information_schema.columns where table_schema=Database() and table_name = 't' and column_name = 's'").Check(
		testkit.Rows("s 13"))
	tk.MustQuery("select column_name, character_maximum_length from information_schema.columns where table_schema=Database() and table_name = 't' and column_name = 's2'").Check(
		testkit.Rows("s2 30"))
	tk.MustQuery("select column_name, character_maximum_length from information_schema.columns where table_schema=Database() and table_name = 't' and column_name = 'e1'").Check(
		testkit.Rows("e1 4"))

	tk1 := testkit.NewTestKit(c, store)
	tk1.MustExec("use test")
	c.Assert(tk1.Se.Auth(&auth.UserIdentity{
		Username: "xxx",
		Hostname: "127.0.0.1",
	}, nil, nil), IsTrue)

	tk1.MustQuery("select distinct(table_schema) from information_schema.tables").Check(testkit.Rows("INFORMATION_SCHEMA"))
}

func (s *testSuite) TestDataForTableStatsField(c *C) {
	testleak.BeforeTest()
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	session.SetStatsLease(0)
	do, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	do.SetStatsUpdating(true)
	defer do.Close()
	oldExpiryTime := infoschema.TableStatsCacheExpiry
	infoschema.TableStatsCacheExpiry = 0
	defer func() { infoschema.TableStatsCacheExpiry = oldExpiryTime }()

	h := do.StatsHandle()
	is := do.InfoSchema()
	tk := testkit.NewTestKit(c, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int, e char(5), index idx(e))")
	h.HandleDDLEvent(<-h.DDLEventCh())
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("0 0 0 0"))
	tk.MustExec(`insert into t(c, d, e) values(1, 2, "c"), (2, 3, "d"), (3, 4, "e")`)
	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("3 17 51 3"))
	tk.MustExec(`insert into t(c, d, e) values(4, 5, "f")`)
	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("4 17 68 4"))
	tk.MustExec("delete from t where c >= 3")
	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("2 17 34 2"))
	tk.MustExec("delete from t where c=3")
	h.DumpStatsDeltaToKV(statistics.DumpAll)
	h.Update(is)
	tk.MustQuery("select table_rows, avg_row_length, data_length, index_length from information_schema.tables where table_name='t'").Check(
		testkit.Rows("2 17 34 2"))
}

func (s *testSuite) TestCharacterSetCollations(c *C) {

	testleak.BeforeTest()
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	session.SetStatsLease(0)
	do, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer do.Close()

	tk := testkit.NewTestKit(c, store)

	// The description column is not important
	tk.MustQuery("SELECT default_collate_name, maxlen FROM information_schema.character_sets ORDER BY character_set_name").Check(
		testkit.Rows("ascii_bin 1", "binary 1", "latin1_bin 1", "utf8_bin 3", "utf8mb4_bin 4"))

	// The is_default column is not important
	// but the id's are used by client libraries and must be stable
	tk.MustQuery("SELECT character_set_name, id, sortlen FROM information_schema.collations ORDER BY collation_name").Check(
		testkit.Rows("armscii8 64 1", "armscii8 32 1", "ascii 65 1", "ascii 11 1", "big5 84 1", "big5 1 1", "binary 63 1", "cp1250 66 1", "cp1250 44 1", "cp1250 34 1", "cp1250 26 1", "cp1250 99 1", "cp1251 50 1", "cp1251 14 1", "cp1251 51 1", "cp1251 52 1", "cp1251 23 1", "cp1256 67 1", "cp1256 57 1", "cp1257 58 1", "cp1257 59 1", "cp1257 29 1", "cp850 80 1", "cp850 4 1", "cp852 81 1", "cp852 40 1", "cp866 68 1", "cp866 36 1", "cp932 96 1", "cp932 95 1", "dec8 69 1", "dec8 3 1", "eucjpms 98 1", "eucjpms 97 1", "euckr 85 1", "euckr 19 1", "gb2312 86 1", "gb2312 24 1", "gbk 87 1", "gbk 28 1", "geostd8 93 1", "geostd8 92 1", "greek 70 1", "greek 25 1", "hebrew 71 1", "hebrew 16 1", "hp8 72 1", "hp8 6 1", "keybcs2 73 1", "keybcs2 37 1", "koi8r 74 1", "koi8r 7 1", "koi8u 75 1", "koi8u 22 1", "latin1 47 1", "latin1 15 1", "latin1 48 1", "latin1 49 1", "latin1 5 1", "latin1 31 1", "latin1 94 1", "latin1 8 1", "latin2 77 1", "latin2 27 1", "latin2 2 1", "latin2 9 1", "latin2 21 1", "latin5 78 1", "latin5 30 1", "latin7 79 1", "latin7 20 1", "latin7 41 1", "latin7 42 1", "macce 43 1", "macce 38 1", "macroman 53 1", "macroman 39 1", "sjis 88 1", "sjis 13 1", "swe7 82 1", "swe7 10 1", "tis620 89 1", "tis620 18 1", "ucs2 90 1", "ucs2 149 1", "ucs2 138 1", "ucs2 139 1", "ucs2 145 1", "ucs2 134 1", "ucs2 35 1", "ucs2 159 1", "ucs2 148 1", "ucs2 146 1", "ucs2 129 1", "ucs2 130 1", "ucs2 140 1", "ucs2 144 1", "ucs2 133 1", "ucs2 143 1", "ucs2 131 1", "ucs2 147 1", "ucs2 141 1", "ucs2 132 1", "ucs2 142 1", "ucs2 135 1", "ucs2 136 1", "ucs2 137 1", "ucs2 150 1", "ucs2 128 1", "ucs2 151 1", "ujis 91 1", "ujis 12 1", "utf16 55 1", "utf16 122 1", "utf16 111 1", "utf16 112 1", "utf16 118 1", "utf16 107 1", "utf16 54 1", "utf16 121 1", "utf16 119 1", "utf16 102 1", "utf16 103 1", "utf16 113 1", "utf16 117 1", "utf16 106 1", "utf16 116 1", "utf16 104 1", "utf16 120 1", "utf16 114 1", "utf16 105 1", "utf16 115 1", "utf16 108 1", "utf16 109 1", "utf16 110 1", "utf16 123 1", "utf16 101 1", "utf16 124 1", "utf16le 62 1", "utf16le 56 1", "utf32 61 1", "utf32 181 1", "utf32 170 1", "utf32 171 1", "utf32 177 1", "utf32 166 1", "utf32 60 1", "utf32 180 1", "utf32 178 1", "utf32 161 1", "utf32 162 1", "utf32 172 1", "utf32 176 1", "utf32 165 1", "utf32 175 1", "utf32 163 1", "utf32 179 1", "utf32 173 1", "utf32 164 1", "utf32 174 1", "utf32 167 1", "utf32 168 1", "utf32 169 1", "utf32 182 1", "utf32 160 1", "utf32 183 1", "utf8 83 1", "utf8 213 1", "utf8 202 1", "utf8 203 1", "utf8 209 1", "utf8 198 1", "utf8 33 1", "utf8 223 1", "utf8 212 1", "utf8 210 1", "utf8 193 1", "utf8 194 1", "utf8 204 1", "utf8 208 1", "utf8 197 1", "utf8 207 1", "utf8 195 1", "utf8 211 1", "utf8 205 1", "utf8 196 1", "utf8 206 1", "utf8 199 1", "utf8 200 1", "utf8 201 1", "utf8 214 1", "utf8 192 1", "utf8 215 1", "utf8mb4 46 1", "utf8mb4 245 1", "utf8mb4 234 1", "utf8mb4 235 1", "utf8mb4 241 1", "utf8mb4 230 1", "utf8mb4 45 1", "utf8mb4 244 1", "utf8mb4 242 1", "utf8mb4 225 1", "utf8mb4 226 1", "utf8mb4 236 1", "utf8mb4 240 1", "utf8mb4 229 1", "utf8mb4 239 1", "utf8mb4 227 1", "utf8mb4 243 1", "utf8mb4 237 1", "utf8mb4 228 1", "utf8mb4 238 1", "utf8mb4 231 1", "utf8mb4 232 1", "utf8mb4 233 1", "utf8mb4 246 1", "utf8mb4 224 1", "utf8mb4 247 1"))

}

func (s *testSuite) TestSchemataCharacterSet(c *C) {

	testleak.BeforeTest()
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	session.SetStatsLease(0)
	do, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer do.Close()

	tk := testkit.NewTestKit(c, store)
	tk.MustExec("CREATE DATABASE `foo` DEFAULT CHARACTER SET = 'utf8mb4'")
	tk.MustQuery("select default_character_set_name, default_collation_name FROM information_schema.SCHEMATA  WHERE schema_name = 'foo'").Check(
		testkit.Rows("utf8mb4 utf8mb4_bin"))

}

func (s *testSuite) TestProfiling(c *C) {
	testleak.BeforeTest()
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	session.SetStatsLease(0)
	do, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer do.Close()

	tk := testkit.NewTestKit(c, store)
	tk.MustQuery("select * from information_schema.profiling").Check(testkit.Rows())
	tk.MustExec("set @@profiling=1")
	tk.MustQuery("select * from information_schema.profiling").Check(testkit.Rows("0 0  0 0 0 0 0 0 0 0 0 0 0 0   0"))
}
