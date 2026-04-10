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

package traceevent_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Those functions are now using exported versions from the main package.
// Note: We use helper functions instead of methods since we can't define methods on non-local types

func SetCategories(r *traceevent.ExportedHTTPFlightRecorder, categories traceevent.ExportedTraceCategory) {
	traceevent.ExportedSetCategories(r, categories)
}

func Disable(r *traceevent.ExportedHTTPFlightRecorder, categories traceevent.ExportedTraceCategory) {
	traceevent.ExportedDisable(r, categories)
}

// Enable enables trace events for the specified categories.
func Enable(r *traceevent.ExportedHTTPFlightRecorder, categories traceevent.ExportedTraceCategory) {
	traceevent.ExportedEnable(r, categories)
}

// SetMode sets the trace event mode and returns the previous mode.
func SetMode(mode string) (string, error) {
	return traceevent.ExportedSetMode(mode)
}

// CurrentMode returns the current trace event mode.
func CurrentMode() string {
	return traceevent.ExportedCurrentMode()
}

// NormalizeMode validates and normalizes a mode string.
func NormalizeMode(mode string) (string, error) {
	return traceevent.ExportedNormalizeMode(mode)
}

// FlightRecorder returns the global flight recorder.
func FlightRecorder() *traceevent.ExportedRingBufferSink {
	return traceevent.ExportedFlightRecorder()
}

// TraceEvent records a trace event.
func TraceEvent(ctx context.Context, category traceevent.ExportedTraceCategory, name string, fields ...zap.Field) {
	traceevent.ExportedTraceEvent(ctx, category, name, fields...)
}

// NewRingBufferSink creates a new ring buffer sink.
func NewRingBufferSink(capacity int) *traceevent.ExportedRingBufferSink {
	return traceevent.ExportedNewRingBufferSink(capacity)
}

// DumpFlightRecorderToLogger dumps the flight recorder to the logger.
func DumpFlightRecorderToLogger(reason string) {
	traceevent.ExportedDumpFlightRecorderToLogger(reason)
}

// StartLogFlightRecorder starts the log flight recorder.
func StartLogFlightRecorder(config *traceevent.ExportedFlightRecorderConfig) error {
	return traceevent.ExportedStartLogFlightRecorder(config)
}

// GetFlightRecorder returns the flight recorder.
func GetFlightRecorder() *traceevent.ExportedHTTPFlightRecorder {
	return traceevent.ExportedGetFlightRecorder()
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
		var value traceevent.ExportedFlightRecorderConfig
		// marshal success
		err := json.Unmarshal([]byte(testcase.conf), &value)
		require.NoError(t, err, idx)

		// compile success
		res, err := value.Compile()
		require.NoError(t, err, idx)

		// result expected
		require.Equal(t, traceevent.ExportedGetNameMapping(&res), testcase.result, idx)

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
		var value traceevent.ExportedFlightRecorderConfig
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

func RunFlightRecorderConfig(t *testing.T) {
	testFlightRecorderConfigGoodCase(t)
	testFlightRecorderConfigBadCase(t)
}

func RunParseTraceCategory(t *testing.T) {
	testcases := []struct {
		input  []string
		expect traceevent.ExportedTraceCategory
	}{
		{[]string{"*"}, tracing.AllCategories},
		{[]string{"-", "general"}, tracing.AllCategories &^ tracing.General},
		{[]string{"txn_2pc"}, tracing.Txn2PC},
		{[]string{"txn_2pc", "stmt_plan", "non_exist"}, tracing.Txn2PC | tracing.StmtPlan},
		{[]string{"non_exist"}, 0},
	}

	for idx, testcase := range testcases {
		categories := traceevent.ExportedParseCategories(testcase.input)
		require.Equal(t, testcase.expect, categories, idx)
	}
}

func RunAndOrCombination(t *testing.T) {
	compiled := traceevent.ExportedNewCompiledDumpTriggerConfig()
	A, err := traceevent.ExportedAddTrigger(compiled, "A", nil)
	require.NoError(t, err)
	B, err := traceevent.ExportedAddTrigger(compiled, "B", nil)
	require.NoError(t, err)
	C, err := traceevent.ExportedAddTrigger(compiled, "C", nil)
	require.NoError(t, err)
	D, err := traceevent.ExportedAddTrigger(compiled, "D", nil)
	require.NoError(t, err)

	truthTable := traceevent.ExportedTruthTableForAnd([]uint64{A}, []uint64{B, C})
	require.False(t, traceevent.ExportedCheckTruthTableBool(A, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(D, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(A|B, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(A|C, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(B|C, truthTable))

	truthTable = traceevent.ExportedTruthTableForAnd([]uint64{A | B}, []uint64{C})
	require.False(t, traceevent.ExportedCheckTruthTableBool(A, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(D, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(A|B, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(A|C, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(B|C, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(A|B|C, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(A|B|C|D, truthTable))

	truthTable = traceevent.ExportedTruthTableForAnd([]uint64{A, B}, []uint64{C, D})
	require.False(t, traceevent.ExportedCheckTruthTableBool(A, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(B, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(C, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(D, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(A|B, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(C|D, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(A|C, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(B|C, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(B|D, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(B|C|D, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(A|B|C|D, truthTable))

	truthTable = traceevent.ExportedTruthTableForOr([]uint64{A, B}, []uint64{C | D})
	require.True(t, traceevent.ExportedCheckTruthTableBool(A, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(B, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(C, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(D, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(A|D, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(A|B, truthTable))

	truthTable = traceevent.ExportedTruthTableForOr([]uint64{A | C}, []uint64{B | D})
	require.False(t, traceevent.ExportedCheckTruthTableBool(A, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(B, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(C, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(D, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(A|D, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(A|C, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(B|C, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(B|D, truthTable))
	require.False(t, traceevent.ExportedCheckTruthTableBool(C|D, truthTable))
	require.True(t, traceevent.ExportedCheckTruthTableBool(A|C|D, truthTable))
}
