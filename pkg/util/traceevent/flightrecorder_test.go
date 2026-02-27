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

// Those functions are used in test only.
func (r *HTTPFlightRecorder) SetCategories(categories TraceCategory) {
	r.enabledCategories = categories
}

func (r *HTTPFlightRecorder) Disable(categories TraceCategory) {
	current := r.enabledCategories
	next := current &^ categories
	r.enabledCategories = next
}

// Enable enables trace events for the specified categories.
func (r *HTTPFlightRecorder) Enable(categories TraceCategory) {
	current := r.enabledCategories
	next := current | categories
	r.enabledCategories = next
}

func testFlightRecorderConfigGoodCase(t *testing.T) {
	conf1 := `{
  "enabled_categories": [
    "txn_2pc",
    "stmt_plan"
  ],
  "dump_trigger": {
    "type": "sampling",
    "sampling": 100
  }
}`
	name1 := "dump_trigger.sampling"

	conf2 := `{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "sampling",
    "sampling": 1
  }
}`
	name2 := "dump_trigger.sampling"

	conf3 := `{
  "enabled_categories": [
    "general"
  ],
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
  "enabled_categories": [
    "*"
  ],
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
  "enabled_categories": [
    "*"
  ],
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
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "suspicious_event",
    "suspicious_event": {
      "type": "slow_query"
    }
  }
}`
	name6 := "dump_trigger.suspicious_event"

	conf7 := `{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "suspicious_event",
    "suspicious_event": {
      "type": "region_error"
    }
  }
}`
	name7 := "dump_trigger.suspicious_event"

	conf8 := `{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "and",
    "and": [
      {
        "type": "user_command",
        "user_command": {
          "type": "stmt_label",
          "stmt_label": "Select"
        }
      },
      {
        "type": "suspicious_event",
        "suspicious_event": {
          "type": "resolve_lock"
        }
      }
    ]
  }
}`

	conf9 := `{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "or",
    "or": [
      {
        "type": "and",
        "and": [
          {
            "type": "user_command",
            "user_command": {
              "type": "stmt_label",
              "stmt_label": "Insert"
            }
          },
          {
            "type": "suspicious_event",
            "suspicious_event": {
              "type": "query_fail"
            }
          }
        ]
      },
      {
        "type": "sampling",
        "sampling": 10
      }
    ]
  }
}`

	conf10 := `{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "suspicious_event",
    "suspicious_event": {
      "type": "is_internal",
      "is_internal": true
    }
  }
}`
	name10 := "dump_trigger.suspicious_event.is_internal"

	conf11 := `{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "suspicious_event",
    "suspicious_event": {
      "type": "dev_debug",
      "dev_debug": {
        "type": "execute_internal_trace_missing"
      }
    }
  }
}`
	name11 := "dump_trigger.suspicious_event.dev_debug"

	testcases := []struct {
		conf   string
		result map[string]int
	}{
		{conf1, map[string]int{name1: 0}},
		{conf2, map[string]int{name2: 0}},
		{conf3, map[string]int{name3: 0}},
		{conf4, map[string]int{name4: 0}},
		{conf5, map[string]int{name5: 0}},
		{conf6, map[string]int{name6: 0}},
		{conf7, map[string]int{name7: 0}},
		{conf8, map[string]int{"dump_trigger.user_command.stmt_label": 0, "dump_trigger.suspicious_event": 1}},
		{conf9, map[string]int{"dump_trigger.user_command.stmt_label": 0, "dump_trigger.suspicious_event": 1, "dump_trigger.sampling": 2}},
		{conf10, map[string]int{name10: 0}},
		{conf11, map[string]int{name11: 0}},
	}

	var b strings.Builder
	for idx, testcase := range testcases {
		var value FlightRecorderConfig
		// marshal success
		err := json.Unmarshal([]byte(testcase.conf), &value)
		require.NoError(t, err, idx)

		// compile success
		res, err := value.Compile()
		require.NoError(t, err, idx)

		// result expected
		require.Equal(t, res.nameMapping, testcase.result, idx)

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
  "enabled_categories": [
    "txn_2pc",
    "stmt_plan"
  ],
  "dump_trigger": {
    "type": "user_command",
    "sampling": 5
  }
}`
	badcaseValidate1 := `{
  "enabled_categories": [
    "sdaf"
  ],
  "dump_trigger": {
    "type": "user_command",
    "user_command": {
      "type": "non_exist",
      "sql_regexp": "^select"
    }
  }
}`
	badcaseDuplicated := `{
  "enabled_categories": [
    "*"
  ],
  "dump_trigger": {
    "type": "and",
    "and": [
      {
        "type": "suspicious_event",
        "suspicious_event": {
          "type": "slow_query"
        }
      },
      {
        "type": "suspicious_event",
        "suspicious_event": {
          "type": "query_fail"
        }
      }
    ]
  }
}`
	badcases := []struct {
		conf    string
		errkind int
	}{
		{badcaseJSONDecode, 1},
		{badcaseValidate, 2},
		{badcaseValidate1, 2},
		{badcaseDuplicated, 2},
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

		_, err = value.Compile()
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

func TestAndOrCombination(t *testing.T) {
	compiled := compiledDumpTriggerConfig{
		nameMapping: make(map[string]int),
	}
	A, err := compiled.addTrigger("A", nil)
	require.NoError(t, err)
	B, err := compiled.addTrigger("B", nil)
	require.NoError(t, err)
	C, err := compiled.addTrigger("C", nil)
	require.NoError(t, err)
	D, err := compiled.addTrigger("D", nil)
	require.NoError(t, err)

	truthTable := truthTableForAnd([]uint64{A}, []uint64{B, C})
	require.False(t, checkTruthTable(A, truthTable))
	require.False(t, checkTruthTable(D, truthTable))
	require.True(t, checkTruthTable(A|B, truthTable))
	require.True(t, checkTruthTable(A|C, truthTable))
	require.False(t, checkTruthTable(B|C, truthTable))

	truthTable = truthTableForAnd([]uint64{A | B}, []uint64{C})
	require.False(t, checkTruthTable(A, truthTable))
	require.False(t, checkTruthTable(D, truthTable))
	require.False(t, checkTruthTable(A|B, truthTable))
	require.False(t, checkTruthTable(A|C, truthTable))
	require.False(t, checkTruthTable(B|C, truthTable))
	require.True(t, checkTruthTable(A|B|C, truthTable))
	require.True(t, checkTruthTable(A|B|C|D, truthTable))

	truthTable = truthTableForAnd([]uint64{A, B}, []uint64{C, D})
	require.False(t, checkTruthTable(A, truthTable))
	require.False(t, checkTruthTable(B, truthTable))
	require.False(t, checkTruthTable(C, truthTable))
	require.False(t, checkTruthTable(D, truthTable))
	require.False(t, checkTruthTable(A|B, truthTable))
	require.False(t, checkTruthTable(C|D, truthTable))
	require.True(t, checkTruthTable(A|C, truthTable))
	require.True(t, checkTruthTable(B|C, truthTable))
	require.True(t, checkTruthTable(B|D, truthTable))
	require.True(t, checkTruthTable(B|C|D, truthTable))
	require.True(t, checkTruthTable(A|B|C|D, truthTable))

	truthTable = truthTableForOr([]uint64{A, B}, []uint64{C | D})
	require.True(t, checkTruthTable(A, truthTable))
	require.True(t, checkTruthTable(B, truthTable))
	require.False(t, checkTruthTable(C, truthTable))
	require.False(t, checkTruthTable(D, truthTable))
	require.True(t, checkTruthTable(A|D, truthTable))
	require.True(t, checkTruthTable(A|B, truthTable))

	truthTable = truthTableForOr([]uint64{A | C}, []uint64{B | D})
	require.False(t, checkTruthTable(A, truthTable))
	require.False(t, checkTruthTable(B, truthTable))
	require.False(t, checkTruthTable(C, truthTable))
	require.False(t, checkTruthTable(D, truthTable))
	require.False(t, checkTruthTable(A|D, truthTable))
	require.True(t, checkTruthTable(A|C, truthTable))
	require.False(t, checkTruthTable(B|C, truthTable))
	require.True(t, checkTruthTable(B|D, truthTable))
	require.False(t, checkTruthTable(C|D, truthTable))
	require.True(t, checkTruthTable(A|C|D, truthTable))
}
