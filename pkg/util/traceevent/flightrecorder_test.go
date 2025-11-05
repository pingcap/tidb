// Copyright 2025 PingCAP, Inc.
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

package traceevent

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func testFlightRecorderConfigGoodCase(t *testing.T) {
	conf1 := `{
	"enabled_categories": ["txn_2pc", "stmt_plan"],
	"dump_trigger": {
	"type": "sampling",
	"sampling": 100
	}
	}`
	name1 := "dump_trigger.sampling"

	conf2 := `{
	"enabled_categories": ["*"],
	"dump_trigger": {
	"type": "sampling",
	"sampling": 1
	}
	}`
	name2 := "dump_trigger.sampling"

	conf3 := `{
	"enabled_categories": ["general"],
	"dump_trigger": {
	"type": "user_command",
	"user_command": {
 "type": "sql_regexp",
 "sql_regexp": "^select"
	}
	}
	}`
	name3 := "dump_trigger.user_command.sql_regexp"

	conf4 := `{
	"enabled_categories": ["*"],
	"dump_trigger": {
	"type": "user_command",
	"user_command": {
 "type": "stmt_label",
 "stmt_label": "CreateTable"
	}
	}
	}`
	name4 := "dump_trigger.user_command.stmt_label"

	conf5 := `{
	"enabled_categories": ["*"],
	"dump_trigger": {
	"type": "user_command",
	"user_command": {
 "type": "sql_regexp",
 "sql_regexp": "^select"
	}
	}
	}`
	name5 := "dump_trigger.user_command.sql_regexp"

	conf6 := `{
	"enabled_categories": ["*"],
	"dump_trigger": {
	"type": "suspicious_event",
	"suspicious_event": {
	"type": "slow_query"
	}
	}
	}`
	name6 := "dump_trigger.suspicious_event"

	conf7 := `{
	"enabled_categories": ["*"],
	"dump_trigger": {
	"type": "suspicious_event",
	"suspicious_event": {
	"type": "region_error"
	}
	}
	}`
	name7 := "dump_trigger.suspicious_event"

	testcases := []struct {
		conf string
		name string
	}{
		{conf1, name1},
		{conf2, name2},
		{conf3, name3},
		{conf4, name4},
		{conf5, name5},
		{conf6, name6},
		{conf7, name7},
	}

	var b strings.Builder
	for idx, testcase := range testcases {
		var value FlightRecorderConfig
		// marshal success
		err := json.Unmarshal([]byte(testcase.conf), &value)
		require.NoError(t, err, idx)

		// validate success
		err = value.Validate(&b)
		require.NoError(t, err, idx)

		// result expected
		require.Equal(t, b.String(), testcase.name, idx)

		b.Reset()
	}
}

func testFlightRecorderConfigBadCase(t *testing.T) {
	badcaseJSONDecode := `"enabled_categories": ["*"],
	"dump_trigger": {
	"type": "sampling",
	"sampling": 5,
	}`
	badcaseValidate := `{
	"enabled_categories": ["txn_2pc", "stmt_plan"],
	"dump_trigger": {
	"type": "user_command",
	"sampling": 5
	}
	}`
	badcaseValidate1 := `{
	"enabled_categories": ["sdaf"],
	"dump_trigger": {
	"type": "user_command",
	"user_command": {
	"type": "non_exist",
	"sql_regexp": "^select"
	}
	}
	}`

	badcases := []struct {
		conf    string
		errkind int
	}{
		{badcaseJSONDecode, 1},
		{badcaseValidate, 2},
		{badcaseValidate1, 2},
	}
	var b strings.Builder
	for idx, badcase := range badcases {
		var value FlightRecorderConfig
		err := json.Unmarshal([]byte(badcase.conf), &value)
		if badcase.errkind == 1 {
			require.Error(t, err, idx)
			continue
		}
		require.NoError(t, err, idx)

		err = value.Validate(&b)
		if badcase.errkind == 2 {
			require.Error(t, err, idx)
			continue
		}
		require.NoError(t, err, idx)
		b.Reset()
	}
}

func TestFlightRecorderConfig(t *testing.T) {
	testFlightRecorderConfigGoodCase(t)
	testFlightRecorderConfigBadCase(t)
}

func TestParseTraceCategory(t *testing.T) {
	testcases := []struct {
		input  []string
		expect TraceCategory
	}{
		{[]string{"*"}, AllCategories},
		{[]string{"-", "general"}, AllCategories &^ General},
		{[]string{"txn_2pc"}, Txn2PC},
		{[]string{"txn_2pc", "stmt_plan", "non_exist"}, Txn2PC | StmtPlan},
		{[]string{"non_exist"}, 0},
	}

	for idx, testcase := range testcases {
		categories := parseCategories(testcase.input)
		require.Equal(t, testcase.expect, categories, idx)
	}
}
