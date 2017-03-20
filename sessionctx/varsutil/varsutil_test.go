// Copyright 2016 PingCAP, Inc.
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

package varsutil

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testVarsutilSuite{})

type testVarsutilSuite struct {
}

func (s *testVarsutilSuite) TestTiDBOptOn(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		val string
		on  bool
	}{
		{"ON", true},
		{"on", true},
		{"On", true},
		{"1", true},
		{"off", false},
		{"No", false},
		{"0", false},
		{"1.1", false},
		{"", false},
	}
	for _, t := range tbl {
		on := tidbOptOn(t.val)
		c.Assert(on, Equals, t.on)
	}
}

func (s *testVarsutilSuite) TestVarsutil(c *C) {
	defer testleak.AfterTest(c)()
	v := variable.NewSessionVars()

	SetSessionSystemVar(v, "autocommit", types.NewStringDatum("1"))
	val, err := GetSessionSystemVar(v, "autocommit")
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")
	c.Assert(SetSessionSystemVar(v, "autocommit", types.Datum{}), NotNil)

	SetSessionSystemVar(v, "sql_mode", types.NewStringDatum("strict_trans_tables"))
	val, err = GetSessionSystemVar(v, "sql_mode")
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "STRICT_TRANS_TABLES")
	c.Assert(v.StrictSQLMode, IsTrue)
	SetSessionSystemVar(v, "sql_mode", types.NewStringDatum(""))
	c.Assert(v.StrictSQLMode, IsFalse)

	SetSessionSystemVar(v, "character_set_connection", types.NewStringDatum("utf8"))
	SetSessionSystemVar(v, "collation_connection", types.NewStringDatum("utf8_general_ci"))
	charset, collation := v.GetCharsetInfo()
	c.Assert(charset, Equals, "utf8")
	c.Assert(collation, Equals, "utf8_general_ci")

	c.Assert(SetSessionSystemVar(v, "character_set_results", types.Datum{}), IsNil)

	// Test case for get TiDBSkipConstraintCheck session variable.
	val, err = GetSessionSystemVar(v, variable.TiDBSkipConstraintCheck)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "0")

	// Test case for tidb_skip_constraint_check
	c.Assert(v.SkipConstraintCheck, IsFalse)
	SetSessionSystemVar(v, variable.TiDBSkipConstraintCheck, types.NewStringDatum("0"))
	c.Assert(v.SkipConstraintCheck, IsFalse)
	SetSessionSystemVar(v, variable.TiDBSkipConstraintCheck, types.NewStringDatum("1"))
	c.Assert(v.SkipConstraintCheck, IsTrue)
	SetSessionSystemVar(v, variable.TiDBSkipConstraintCheck, types.NewStringDatum("0"))
	c.Assert(v.SkipConstraintCheck, IsFalse)

	// Test case for change TiDBSkipConstraintCheck session variable.
	SetSessionSystemVar(v, variable.TiDBSkipConstraintCheck, types.NewStringDatum("1"))
	val, err = GetSessionSystemVar(v, variable.TiDBSkipConstraintCheck)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")

	// Test case for get TiDBSkipDDLWait session variable.
	val, err = GetSessionSystemVar(v, variable.TiDBSkipDDLWait)
	c.Assert(val, Equals, "0")
	c.Assert(v.SkipDDLWait, IsFalse)
	SetSessionSystemVar(v, variable.TiDBSkipDDLWait, types.NewStringDatum("0"))
	c.Assert(v.SkipDDLWait, IsFalse)
	SetSessionSystemVar(v, variable.TiDBSkipDDLWait, types.NewStringDatum("1"))
	c.Assert(v.SkipDDLWait, IsTrue)
	val, err = GetSessionSystemVar(v, variable.TiDBSkipDDLWait)
	c.Assert(val, Equals, "1")

	// Test case for time_zone session variable.
	SetSessionSystemVar(v, variable.TimeZone, types.NewStringDatum("Europe/Helsinki"))
	c.Assert(v.TimeZone.String(), Equals, "Europe/Helsinki")
	SetSessionSystemVar(v, variable.TimeZone, types.NewStringDatum("US/Eastern"))
	c.Assert(v.TimeZone.String(), Equals, "US/Eastern")
	SetSessionSystemVar(v, variable.TimeZone, types.NewStringDatum("SYSTEM"))
	c.Assert(v.TimeZone.String(), Equals, "Local")
	SetSessionSystemVar(v, variable.TimeZone, types.NewStringDatum("+10:00"))
	c.Assert(v.TimeZone.String(), Equals, "UTC")
	t1 := time.Date(2000, 1, 1, 0, 0, 0, 0, v.TimeZone)
	t2 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	c.Assert(t2.Sub(t1), Equals, 10*time.Hour)
	SetSessionSystemVar(v, variable.TimeZone, types.NewStringDatum("-6:00"))
	c.Assert(v.TimeZone.String(), Equals, "UTC")

	// Test case for sql mode.
	for str, mode := range mysql.Str2SQLMode {
		SetSessionSystemVar(v, "sql_mode", types.NewStringDatum(str))
		c.Assert(v.SQLMode, Equals, mode)
	}

	// Combined sql_mode
	SetSessionSystemVar(v, "sql_mode", types.NewStringDatum("REAL_AS_FLOAT,ANSI_QUOTES"))
	c.Assert(v.SQLMode, Equals, mysql.ModeRealAsFloat|mysql.ModeANSIQuotes)
}
