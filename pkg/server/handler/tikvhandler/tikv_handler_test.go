// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikvhandler

import (
	"net/http"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/stretchr/testify/require"
)

func TestParseAdminCheckIndexLimit(t *testing.T) {
	limit, err := parseAdminCheckIndexLimit(&http.Request{})
	require.NoError(t, err)
	require.Equal(t, adminCheckIndexDefaultLimit, limit)

	limit, err = parseAdminCheckIndexLimit(&http.Request{Form: map[string][]string{handler.Limit: {"200"}}})
	require.NoError(t, err)
	require.Equal(t, 200, limit)

	_, err = parseAdminCheckIndexLimit(&http.Request{Form: map[string][]string{handler.Limit: {"abc"}}})
	require.ErrorContains(t, err, "invalid limit")

	_, err = parseAdminCheckIndexLimit(&http.Request{Form: map[string][]string{handler.Limit: {"0"}}})
	require.ErrorContains(t, err, "greater than 0")
}

func TestTruncateAdminCheckIndexSummaryRows(t *testing.T) {
	summary := &executor.AdminCheckIndexInconsistentSummary{
		InconsistentRowCount: 4,
		Rows: []executor.AdminCheckIndexInconsistentRow{
			{Handle: "h1", MismatchType: executor.AdminCheckIndexRowWithoutIndex},
			{Handle: "h2", MismatchType: executor.AdminCheckIndexIndexWithoutRow},
			{Handle: "h3", MismatchType: executor.AdminCheckIndexRowIndexMismatch},
			{Handle: "h4", MismatchType: executor.AdminCheckIndexRowWithoutIndex},
		},
	}

	truncateAdminCheckIndexSummaryRows(summary, 2)
	require.Equal(t, uint64(4), summary.InconsistentRowCount)
	require.Len(t, summary.Rows, 2)
	require.Equal(t, "h1", summary.Rows[0].Handle)
	require.Equal(t, "h2", summary.Rows[1].Handle)

	truncateAdminCheckIndexSummaryRows(summary, 10)
	require.Len(t, summary.Rows, 2)

	truncateAdminCheckIndexSummaryRows(nil, 1)
}
