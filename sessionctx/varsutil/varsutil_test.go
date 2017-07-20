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
	"github.com/pingcap/tidb/terror"
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
	v.GlobalVarsAccessor = newMockGlobalAccessor()

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

	// Test case for time_zone session variable.
	tests := []struct {
		input        string
		expect       string
		compareValue bool
		diff         time.Duration
	}{
		{"Europe/Helsinki", "Europe/Helsinki", true, -2 * time.Hour},
		{"US/Eastern", "US/Eastern", true, 5 * time.Hour},
		//TODO: Check it out and reopen this case.
		//{"SYSTEM", "Local", false, 0},
		{"+10:00", "UTC", true, -10 * time.Hour},
		{"-6:00", "UTC", true, 6 * time.Hour},
	}
	for _, tt := range tests {
		err := SetSessionSystemVar(v, variable.TimeZone, types.NewStringDatum(tt.input))
		c.Assert(err, IsNil)
		c.Assert(v.TimeZone.String(), Equals, tt.expect)
		if tt.compareValue {
			SetSessionSystemVar(v, variable.TimeZone, types.NewStringDatum(tt.input))
			t1 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			t2 := time.Date(2000, 1, 1, 0, 0, 0, 0, v.TimeZone)
			c.Assert(t2.Sub(t1), Equals, tt.diff)
		}
	}
	err = SetSessionSystemVar(v, variable.TimeZone, types.NewStringDatum("6:00"))
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, variable.ErrUnknownTimeZone), IsTrue)

	// Test case for sql mode.
	for str, mode := range mysql.Str2SQLMode {
		SetSessionSystemVar(v, "sql_mode", types.NewStringDatum(str))
		c.Assert(v.SQLMode, Equals, mode)
	}

	// Combined sql_mode
	SetSessionSystemVar(v, "sql_mode", types.NewStringDatum("REAL_AS_FLOAT,ANSI_QUOTES"))
	c.Assert(v.SQLMode, Equals, mysql.ModeRealAsFloat|mysql.ModeANSIQuotes)

	// Test case for tidb_index_serial_scan_concurrency.
	c.Assert(v.IndexSerialScanConcurrency, Equals, 1)
	SetSessionSystemVar(v, variable.TiDBIndexSerialScanConcurrency, types.NewStringDatum("4"))
	c.Assert(v.IndexSerialScanConcurrency, Equals, 4)

	// Test case for tidb_batch_insert.
	c.Assert(v.BatchInsert, IsFalse)
	SetSessionSystemVar(v, variable.TiDBBatchInsert, types.NewStringDatum("1"))
	c.Assert(v.BatchInsert, IsTrue)

	//Test case for tidb_max_row_count_for_inlj.
	c.Assert(v.MaxRowCountForINLJ, Equals, 128)
	SetSessionSystemVar(v, variable.TiDBMaxRowCountForINLJ, types.NewStringDatum("127"))
	c.Assert(v.MaxRowCountForINLJ, Equals, 127)
}

type mockGlobalAccessor struct {
	vars map[string]string
}

func newMockGlobalAccessor() *mockGlobalAccessor {
	m := &mockGlobalAccessor{
		vars: make(map[string]string),
	}
	for name, val := range variable.SysVars {
		m.vars[name] = val.Value
	}
	return m
}

func (m *mockGlobalAccessor) GetGlobalSysVar(name string) (string, error) {
	return m.vars[name], nil
}

func (m *mockGlobalAccessor) SetGlobalSysVar(name string, value string) error {
	m.vars[name] = value
	return nil
}
