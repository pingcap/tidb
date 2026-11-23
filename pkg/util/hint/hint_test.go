// Copyright 2026 PingCAP, Inc.
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

package hint

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

type testHintWarningRecorder struct{}

func (testHintWarningRecorder) SetHintWarning(string) {}

func (testHintWarningRecorder) SetHintWarningFromError(error) {}

func TestParseSkipScanHints(t *testing.T) {
	hints, errs := parser.ParseHint("/*+ USE_INDEX(employee_salary, idx_use) IGNORE_INDEX(employee_salary, idx_ignore) SKIP_SCAN(employee_salary, idx_dept_salary) NO_SKIP_SCAN(employee_salary, idx_dept_salary) */", mysql.ModeNone, parser.Pos{Line: 1})
	require.Empty(t, errs)

	planHints, _, err := ParsePlanHints(hints, 0, "test", NewQBHintHandler(nil), false, false, false, false, testHintWarningRecorder{})
	require.NoError(t, err)
	require.Len(t, planHints.SkipScan, 1)
	require.Equal(t, "employee_salary", planHints.SkipScan[0].TblName.L)
	require.Equal(t, "idx_dept_salary", planHints.SkipScan[0].IndexHint.IndexNames[0].L)
	require.Len(t, planHints.NoSkipScan, 1)
	require.Equal(t, []string{
		"use_index(test.employee_salary, idx_use) is inapplicable, check whether the table(test.employee_salary) exists",
		"ignore_index(test.employee_salary, idx_ignore) is inapplicable, check whether the table(test.employee_salary) exists",
		"skip_scan(test.employee_salary, idx_dept_salary) is inapplicable, check whether the table(test.employee_salary) exists",
		"no_skip_scan(test.employee_salary, idx_dept_salary) is inapplicable, check whether the table(test.employee_salary) exists",
	}, CollectUnmatchedHintWarnings(planHints))
}
