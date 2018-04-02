// Copyright 2017 PingCAP, Inc.
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

package server

import (
	"database/sql"
	"strconv"

	. "github.com/pingcap/check"
)

var defaultXPort uint = 14002
var defaultXDSN = "root@tcp(localhost:" + strconv.Itoa(int(defaultXPort)) + ")/test?xprotocol=1"

// runXTests runs tests using the default database `test`.
func runXTests(c *C, tests ...func(dbt *DBTest)) {
	dsn := defaultXDSN
	db, err := sql.Open("mysql/xprotocol", dsn)
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()

	dbt := &DBTest{c, db}
	for _, test := range tests {
		test(dbt)
	}
}

func runXTestCommon(c *C) {
	runXTests(c, func(dbt *DBTest) {
		var out string
		rows := dbt.mustQuery("SELECT DATABASE()")
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err := rows.Scan(&out)
		dbt.Check(err, IsNil)
		dbt.Check(out, DeepEquals, "test")
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()

		rows = dbt.mustQuery("SELECT ABC")
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()

		// admin bogus
		args := []interface{}{
			"mysqlx",
		}
		rows = dbt.mustQuery("whatever", args...)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()

		args = []interface{}{
			"bogus",
		}
		rows = dbt.mustQuery("whatever", args...)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()

		// admin create collection
		args = []interface{}{
			"mysqlx",
			"test",
			"books",
		}
		dbt.mustExec("create_collection", args...)
		dbt.mustExec("drop_collection", args...)

		args = []interface{}{
			"mysqlx",
			"",
			"",
		}
		dbt.mustExec("create_collection", args...)

		args = []interface{}{
			"mysqlx",
			"test",
			"",
		}
		dbt.mustExec("create_collection", args...)

		// ensure collection
		args = []interface{}{
			"mysqlx",
			"test",
			"books",
		}
		dbt.mustExec("create_collection", args...)
		dbt.mustExec("ensure_collection", args...)

		// kill client
		args = []interface{}{
			"mysqlx",
		}
		var (
			id     int
			user   string
			host   string
			sessID int
		)
		rows = dbt.mustQuery("list_clients", args...)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&id, &user, &host, &sessID)
		dbt.Check(err, IsNil)
		dbt.Check(user, DeepEquals, "root")
		dbt.Check(host, DeepEquals, "")
		//dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		//dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()

		args = []interface{}{
			"mysqlx",
			2,
		}
		dbt.mustExec("kill_client", args...)

		args = []interface{}{
			"mysqlx",
		}
		rows = dbt.mustQuery("list_clients", args...)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		//dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		//dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()

		args = []interface{}{
			"mysqlx",
			1,
		}
		dbt.mustExec("kill_client", args...)

		args = []interface{}{
			"mysqlx",
		}
		rows = dbt.mustQuery("list_clients", args...)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		//dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		//dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()

		args = []interface{}{
			"mysqlx",
			3,
		}
		dbt.mustExec("kill_client", args...)

		// list objects
		args = []interface{}{
			"mysqlx",
			"test",
		}
		rows = dbt.mustQuery("list_objects", args...)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Close()

		args = []interface{}{
			"mysqlx",
			"invalid",
		}
		rows = dbt.mustQuery("list_objects", args...)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()

		args = []interface{}{
			"mysqlx",
			"test",
			"myt%",
		}
		rows = dbt.mustQuery("list_objects", args...)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()

		args = []interface{}{
			"mysqlx",
			"test",
			"bla%",
		}
		rows = dbt.mustQuery("list_objects", args...)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()

		args = []interface{}{
			"mysqlx",
		}
		rows = dbt.mustQuery("list_objects", args...)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		rows.Close()

		args = []interface{}{
			"mysqlx",
			"test",
			"books",
		}
		var (
			collectionName string
			collectionType string
		)
		rows = dbt.mustQuery("list_objects", args...)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&collectionName, &collectionType)
		dbt.Check(err, IsNil)
		dbt.Check(collectionName, DeepEquals, "books")
		dbt.Check(collectionType, DeepEquals, "COLLECTION")
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()

		// ping
		args = []interface{}{
			"mysqlx",
		}
		dbt.mustExec("ping", args...)

		// enable_notices
		args = []interface{}{
			"mysqlx",
			"warnings",
			"account_expired",
			"generated_insert_id",
			"rows_affected",
			"produced_message",
		}
		dbt.mustExec("enable_notices", args...)

		// disable_notices
		args = []interface{}{
			"mysqlx",
			"warnings",
			"account_expired",
			"generated_insert_id",
			"rows_affected",
			"produced_message",
		}
		dbt.mustExec("disable_notices", args...)

		// list_notices
		var (
			notice  string
			enabled int
		)
		args = []interface{}{
			"mysqlx",
		}
		rows = dbt.mustQuery("list_notices", args...)
		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&notice, &enabled)
		dbt.Check(err, IsNil)
		dbt.Check(notice, DeepEquals, "warnings")
		dbt.Check(enabled, DeepEquals, 0)

		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&notice, &enabled)
		dbt.Check(err, IsNil)
		dbt.Check(notice, DeepEquals, "account_expired")
		dbt.Check(enabled, DeepEquals, 1)

		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&notice, &enabled)
		dbt.Check(err, IsNil)
		dbt.Check(notice, DeepEquals, "generated_insert_id")
		dbt.Check(enabled, DeepEquals, 1)

		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&notice, &enabled)
		dbt.Check(err, IsNil)
		dbt.Check(notice, DeepEquals, "rows_affected")
		dbt.Check(enabled, DeepEquals, 1)

		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		err = rows.Scan(&notice, &enabled)
		dbt.Check(err, IsNil)
		dbt.Check(notice, DeepEquals, "produced_message")
		dbt.Check(enabled, DeepEquals, 1)

		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()
	})
}

func runXTestValue(c *C) {
	runXTests(c, func(dbt *DBTest) {
		_ = dbt.mustExec("DROP TABLE IF EXISTS xtest")
		sql := `create table xtest (
				c_bit bit(10),
				c_int_d int,
				c_uint_d int unsigned,
				c_bigint_d bigint,
				c_ubigint_d bigint unsigned,
				c_float_d float,
				c_ufloat_d float unsigned,
				c_double_d double,
				c_udouble_d double unsigned,
				c_decimal decimal(6, 3),
				c_udecimal decimal(10, 3) unsigned,
				c_decimal_d decimal,
				c_udecimal_d decimal unsigned,
				c_datetime datetime(2),
				c_datetime_d datetime,
				c_time time(3),
				c_time_d time,
				c_date date,
				c_timestamp timestamp(4) DEFAULT CURRENT_TIMESTAMP(4),
				c_timestamp_d timestamp DEFAULT CURRENT_TIMESTAMP,
				c_char char(20),
				c_bchar char(20) binary,
				c_varchar varchar(20),
				c_bvarchar varchar(20) binary,
				c_text_d text,
				c_btext_d text binary,
				c_binary binary(20),
				c_varbinary varbinary(20),
				c_blob_d blob,
				c_set set('a', 'b', 'c'),
				c_enum enum('a', 'b', 'c'),
				c_json JSON,
				c_year year
			)`
		_ = dbt.mustExec(sql)
		sql = `insert into xtest values (
			        0b11111,
			        -1,
			        1,
			        -1,
			        1,
			        -1.2,
			        1.2,
			        -1.2,
			        1.2,
			        -1.2,
			        1.2,
			        -1.2,
			        1.2,
			        '2017-01-01 11:22:33',
			        '2017-01-01',
			        '-11:22:33',
			        '-11:22:33',
			        '2017-01-01 11:22:33',
			        '2017-01-01 11:22:33',
			        '2017-01-01 11:22:33',
			        'abc',
			        'abc',
			        'abc',
			        'abc',
			        'abc',
			        'abc',
			        'abc',
			        'abc',
			        'abc',
			        'b',
			        'b',
			        CAST(CAST(1 AS UNSIGNED) AS JSON),
			        11
				);`
		_ = dbt.mustExec(sql)
		sql = `select * from xtest;`
		rows := dbt.mustQuery(sql)

		dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
		var (
			cBit        []uint8
			cIntd       int
			cUintd      int
			cBigintd    int
			cUbigintd   int
			cFloatd     float64
			cUfloatd    float64
			cDoubled    float64
			cUdoubled   float64
			cDecimal    []uint8
			cUdecimal   []uint8
			cDecimald   []uint8
			cUdecimald  []uint8
			cDatetime   string
			cDatetimed  string
			cTime       string
			cTimed      string
			cDate       string
			cTimestamp  string
			cTimestampd string
			cChar       string
			cBchar      string
			cVarchar    string
			cBvarchar   string
			cTextd      string
			cBtextd     string
			cBinary     string
			cVarbinary  string
			cBlobd      string
			cSet        string
			cEnum       string
			cJSON       string
			cYear       int
		)
		err := rows.Scan(
			&cBit,
			&cIntd,
			&cUintd,
			&cBigintd,
			&cUbigintd,
			&cFloatd,
			&cUfloatd,
			&cDoubled,
			&cUdoubled,
			&cDecimal,
			&cUdecimal,
			&cDecimald,
			&cUdecimald,
			&cDatetime,
			&cDatetimed,
			&cTime,
			&cTimed,
			&cDate,
			&cTimestamp,
			&cTimestampd,
			&cChar,
			&cBchar,
			&cVarchar,
			&cBvarchar,
			&cTextd,
			&cBtextd,
			&cBinary,
			&cVarbinary,
			&cBlobd,
			&cSet,
			&cEnum,
			&cJSON,
			&cYear,
		)
		dbt.Check(err, IsNil)
		dbt.Check(cBit, DeepEquals, []byte{})
		dbt.Check(cIntd, DeepEquals, -1)
		dbt.Check(cUintd, DeepEquals, 1)
		dbt.Check(cBigintd, DeepEquals, -1)
		dbt.Check(cUbigintd, DeepEquals, 1)
		dbt.Check(cFloatd, DeepEquals, -1.2)
		dbt.Check(cUfloatd, DeepEquals, 1.2)
		dbt.Check(cDoubled, DeepEquals, -1.2)
		dbt.Check(cUdoubled, DeepEquals, 1.2)
		dbt.Check(cDecimal, DeepEquals, []byte{0x3, 0x12, 0x0})
		dbt.Check(cUdecimal, DeepEquals, []byte{0x3, 0x12, 0x0})
		dbt.Check(cDecimald, DeepEquals, []byte{0x0})
		dbt.Check(cUdecimald, DeepEquals, []byte{0x0})
		dbt.Check(cDatetime, DeepEquals, "\xe1\x0f\x01\x01\v\x16")
		dbt.Check(cDatetimed, DeepEquals, "\xe1\x0f\x01\x01\x00\x00")
		dbt.Check(cTime, DeepEquals, "\x01\v\x16")
		dbt.Check(cTimed, DeepEquals, "\x01\v\x16")
		dbt.Check(cDate, DeepEquals, "\xe1\x0f\x01")
		dbt.Check(cTimestamp, DeepEquals, "\xe1\x0f\x01\x01\v\x16")
		dbt.Check(cTimestampd, DeepEquals, "\xe1\x0f\x01\x01\v\x16")
		dbt.Check(cChar, DeepEquals, "abc")
		dbt.Check(cBchar, DeepEquals, "abc")
		dbt.Check(cVarchar, DeepEquals, "abc")
		dbt.Check(cBvarchar, DeepEquals, "abc")
		dbt.Check(cTextd, DeepEquals, "abc")
		dbt.Check(cBtextd, DeepEquals, "abc")
		dbt.Check(cBinary, DeepEquals, "abc\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
		dbt.Check(cVarbinary, DeepEquals, "abc")
		dbt.Check(cBlobd, DeepEquals, "abc")
		dbt.Check(cSet, DeepEquals, "b")
		dbt.Check(cEnum, DeepEquals, "b")
		dbt.Check(cJSON, DeepEquals, "1")
		dbt.Check(cYear, DeepEquals, 4022)

		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
		rows.Close()
	})
}
