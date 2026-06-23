// Copyright 2023 PingCAP, Inc.
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

package fixcontrol_test

import (
	"maps"
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

type resultForSingleFix struct {
	ValueInMap string
	GetStr     string
	GetBool    bool
	GetInt     int64
	GetFloat   float64
}

func getTestResultForSingleFix(fixControlMap map[uint64]string, key uint64) *resultForSingleFix {
	result := &resultForSingleFix{}
	result.ValueInMap = fixControlMap[key]
	result.GetBool = fixcontrol.GetBoolWithDefault(fixControlMap, key, false)
	result.GetStr = fixcontrol.GetStrWithDefault(fixControlMap, key, "default")
	result.GetInt = fixcontrol.GetIntWithDefault(fixControlMap, key, 12345)
	result.GetFloat = fixcontrol.GetFloatWithDefault(fixControlMap, key, 1234.5)
	return result
}

func TestFixControl(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	s := tk.Session()
	var input []string
	var output []struct {
		SQL        string
		FixControl map[uint64]*resultForSingleFix
		Error      string
		Warnings   [][]any
		Variable   []string
	}

	integrationSuiteData := testDataMap["fix_control_suite"]
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		tk.MustExec("set @@tidb_opt_fix_control = \"\"")
		err := tk.ExecToErr(tt)
		var errStr string
		if err != nil {
			errStr = err.Error()
		}
		warning := tk.MustQuery("show warnings").Sort().Rows()
		rows := testdata.ConvertRowsToStrings(tk.MustQuery("select @@tidb_opt_fix_control").Sort().Rows())
		testdata.OnRecord(func() {
			output[i].SQL = tt
			keys := slices.Collect(maps.Keys(s.GetSessionVars().OptimizerFixControl))
			output[i].FixControl = make(map[uint64]*resultForSingleFix, len(keys))
			for _, key := range keys {
				output[i].FixControl[key] = getTestResultForSingleFix(s.GetSessionVars().OptimizerFixControl, key)
			}
			output[i].Error = errStr
			output[i].Warnings = warning
			output[i].Variable = rows
		})
		keys := slices.Collect(maps.Keys(s.GetSessionVars().OptimizerFixControl))
		for _, key := range keys {
			require.Equal(t, output[i].FixControl[key], getTestResultForSingleFix(s.GetSessionVars().OptimizerFixControl, key))
		}
		require.Equal(t, output[i].Error, errStr)
		require.Equal(t, output[i].Warnings, warning)
		require.Equal(t, output[i].Variable, rows)
	}

	require.Equal(t, int64(12345), fixcontrol.GetIntWithDefault(map[uint64]string{1: "on"}, 1, 12345))
	require.Equal(t, int64(12345), fixcontrol.GetIntWithDefault(map[uint64]string{1: "off"}, 1, 12345))
	require.Equal(t, int64(12345), fixcontrol.GetIntWithDefault(map[uint64]string{1: "false"}, 1, 12345))
	require.Equal(t, int64(12345), fixcontrol.GetIntWithDefault(map[uint64]string{1: " 26 "}, 1, 12345))
}

func TestGetPositiveIntWithDefault(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input map[uint64]string
		value int64
		ok    bool
	}{
		{name: "missing", value: 12345, ok: true},
		{name: "on", input: map[uint64]string{1: "on"}, value: 12345, ok: true},
		{name: "true", input: map[uint64]string{1: "true"}, value: 12345, ok: true},
		{name: "off", input: map[uint64]string{1: "off"}, ok: false},
		{name: "false", input: map[uint64]string{1: "false"}, ok: false},
		{name: "zero", input: map[uint64]string{1: "0"}, ok: false},
		{name: "negative", input: map[uint64]string{1: "-1"}, ok: false},
		{name: "bad", input: map[uint64]string{1: "bad"}, ok: false},
		{name: "float", input: map[uint64]string{1: "1.5"}, ok: false},
		{name: "overflow", input: map[uint64]string{1: "9223372036854775808"}, ok: false},
		{name: "trimmed positive", input: map[uint64]string{1: " 26 "}, value: 26, ok: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			value, ok := fixcontrol.GetPositiveIntWithDefault(tt.input, 1, 12345)
			require.Equal(t, tt.value, value)
			require.Equal(t, tt.ok, ok)
		})
	}
}

func TestParseToMapEmptyValue(t *testing.T) {
	m, warns, err := fixcontrol.ParseToMap("123:")
	require.NoError(t, err)
	require.Len(t, warns, 0)
	require.Equal(t, "", m[123])
}
