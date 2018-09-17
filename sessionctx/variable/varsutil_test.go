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

package variable

import (
	"encoding/json"
	"reflect"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testleak"
)

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
		on := TiDBOptOn(t.val)
		c.Assert(on, Equals, t.on)
	}
}

func (s *testVarsutilSuite) TestNewSessionVars(c *C) {
	defer testleak.AfterTest(c)()
	vars := NewSessionVars()

	c.Assert(vars.IndexJoinBatchSize, Equals, DefIndexJoinBatchSize)
	c.Assert(vars.IndexLookupSize, Equals, DefIndexLookupSize)
	c.Assert(vars.IndexLookupConcurrency, Equals, DefIndexLookupConcurrency)
	c.Assert(vars.IndexSerialScanConcurrency, Equals, DefIndexSerialScanConcurrency)
	c.Assert(vars.IndexLookupJoinConcurrency, Equals, DefIndexLookupJoinConcurrency)
	c.Assert(vars.HashJoinConcurrency, Equals, DefTiDBHashJoinConcurrency)
	c.Assert(vars.ProjectionConcurrency, Equals, int64(DefTiDBProjectionConcurrency))
	c.Assert(vars.HashAggPartialConcurrency, Equals, DefTiDBHashAggPartialConcurrency)
	c.Assert(vars.HashAggFinalConcurrency, Equals, DefTiDBHashAggFinalConcurrency)
	c.Assert(vars.DistSQLScanConcurrency, Equals, DefDistSQLScanConcurrency)
	c.Assert(vars.MaxChunkSize, Equals, DefMaxChunkSize)
	c.Assert(vars.DMLBatchSize, Equals, DefDMLBatchSize)
	c.Assert(vars.MemQuotaQuery, Equals, int64(config.GetGlobalConfig().MemQuotaQuery))
	c.Assert(vars.MemQuotaHashJoin, Equals, int64(DefTiDBMemQuotaHashJoin))
	c.Assert(vars.MemQuotaMergeJoin, Equals, int64(DefTiDBMemQuotaMergeJoin))
	c.Assert(vars.MemQuotaSort, Equals, int64(DefTiDBMemQuotaSort))
	c.Assert(vars.MemQuotaTopn, Equals, int64(DefTiDBMemQuotaTopn))
	c.Assert(vars.MemQuotaIndexLookupReader, Equals, int64(DefTiDBMemQuotaIndexLookupReader))
	c.Assert(vars.MemQuotaIndexLookupJoin, Equals, int64(DefTiDBMemQuotaIndexLookupJoin))
	c.Assert(vars.MemQuotaNestedLoopApply, Equals, int64(DefTiDBMemQuotaNestedLoopApply))

	assertFieldsGreaterThanZero(c, reflect.ValueOf(vars.Concurrency))
	assertFieldsGreaterThanZero(c, reflect.ValueOf(vars.MemQuota))
	assertFieldsGreaterThanZero(c, reflect.ValueOf(vars.BatchSize))
}

func assertFieldsGreaterThanZero(c *C, val reflect.Value) {
	for i := 0; i < val.NumField(); i++ {
		fieldVal := val.Field(i)
		c.Assert(fieldVal.Int(), Greater, int64(0))
	}
}

func (s *testVarsutilSuite) TestVarsutil(c *C) {
	defer testleak.AfterTest(c)()
	v := NewSessionVars()
	v.GlobalVarsAccessor = NewMockGlobalAccessor()

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
		{"+10:00", "", true, -10 * time.Hour},
		{"-6:00", "", true, 6 * time.Hour},
	}
	for _, tt := range tests {
		err = SetSessionSystemVar(v, TimeZone, types.NewStringDatum(tt.input))
		c.Assert(err, IsNil)
		c.Assert(v.TimeZone.String(), Equals, tt.expect)
		if tt.compareValue {
			SetSessionSystemVar(v, TimeZone, types.NewStringDatum(tt.input))
			t1 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			t2 := time.Date(2000, 1, 1, 0, 0, 0, 0, v.TimeZone)
			c.Assert(t2.Sub(t1), Equals, tt.diff)
		}
	}
	err = SetSessionSystemVar(v, TimeZone, types.NewStringDatum("6:00"))
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, ErrUnknownTimeZone), IsTrue)

	// Test case for sql mode.
	for str, mode := range mysql.Str2SQLMode {
		SetSessionSystemVar(v, "sql_mode", types.NewStringDatum(str))
		if modeParts, exists := mysql.CombinationSQLMode[str]; exists {
			for _, part := range modeParts {
				mode |= mysql.Str2SQLMode[part]
			}
		}
		c.Assert(v.SQLMode, Equals, mode)
	}

	// Combined sql_mode
	SetSessionSystemVar(v, "sql_mode", types.NewStringDatum("REAL_AS_FLOAT,ANSI_QUOTES"))
	c.Assert(v.SQLMode, Equals, mysql.ModeRealAsFloat|mysql.ModeANSIQuotes)

	// Test case for tidb_index_serial_scan_concurrency.
	c.Assert(v.IndexSerialScanConcurrency, Equals, 1)
	SetSessionSystemVar(v, TiDBIndexSerialScanConcurrency, types.NewStringDatum("4"))
	c.Assert(v.IndexSerialScanConcurrency, Equals, 4)

	// Test case for tidb_batch_insert.
	c.Assert(v.BatchInsert, IsFalse)
	SetSessionSystemVar(v, TiDBBatchInsert, types.NewStringDatum("1"))
	c.Assert(v.BatchInsert, IsTrue)

	c.Assert(v.MaxChunkSize, Equals, 32)
	SetSessionSystemVar(v, TiDBMaxChunkSize, types.NewStringDatum("2"))
	c.Assert(v.MaxChunkSize, Equals, 2)

	// Test case for TiDBConfig session variable.
	err = SetSessionSystemVar(v, TiDBConfig, types.NewStringDatum("abc"))
	c.Assert(terror.ErrorEqual(err, ErrReadOnly), IsTrue)
	val, err = GetSessionSystemVar(v, TiDBConfig)
	c.Assert(err, IsNil)
	bVal, err := json.MarshalIndent(config.GetGlobalConfig(), "", "\t")
	c.Assert(err, IsNil)
	c.Assert(val, Equals, string(bVal))

	SetSessionSystemVar(v, TiDBEnableStreaming, types.NewStringDatum("1"))
	val, err = GetSessionSystemVar(v, TiDBEnableStreaming)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")
	c.Assert(v.EnableStreaming, Equals, true)
	SetSessionSystemVar(v, TiDBEnableStreaming, types.NewStringDatum("0"))
	val, err = GetSessionSystemVar(v, TiDBEnableStreaming)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "0")
	c.Assert(v.EnableStreaming, Equals, false)

	c.Assert(v.OptimizerSelectivityLevel, Equals, DefTiDBOptimizerSelectivityLevel)
	SetSessionSystemVar(v, TiDBOptimizerSelectivityLevel, types.NewIntDatum(1))
	c.Assert(v.OptimizerSelectivityLevel, Equals, 1)

	c.Assert(GetDDLReorgWorkerCounter(), Equals, int32(DefTiDBDDLReorgWorkerCount))
	SetSessionSystemVar(v, TiDBDDLReorgWorkerCount, types.NewIntDatum(1))
	c.Assert(GetDDLReorgWorkerCounter(), Equals, int32(1))

	err = SetSessionSystemVar(v, TiDBDDLReorgWorkerCount, types.NewIntDatum(-1))
	c.Assert(terror.ErrorEqual(err, ErrWrongValueForVar), IsTrue)

	SetSessionSystemVar(v, TiDBDDLReorgWorkerCount, types.NewIntDatum(int64(maxDDLReorgWorkerCount)+1))
	c.Assert(terror.ErrorEqual(err, ErrWrongValueForVar), IsTrue)

	err = SetSessionSystemVar(v, TiDBRetryLimit, types.NewStringDatum("3"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBRetryLimit)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "3")
	c.Assert(v.RetryLimit, Equals, int64(3))

	c.Assert(v.EnableTablePartition, IsFalse)
	err = SetSessionSystemVar(v, TiDBEnableTablePartition, types.NewStringDatum("1"))
	c.Assert(err, IsNil)
	val, err = GetSessionSystemVar(v, TiDBEnableTablePartition)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "1")
	c.Assert(v.EnableTablePartition, IsTrue)
}
