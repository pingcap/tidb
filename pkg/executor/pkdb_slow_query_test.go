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

package executor

import (
	"bufio"
	"context"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestPKDBParseSlowLogEscapedNewlines(t *testing.T) {
	const query = `select 1,\n  'a', -- comment\n  100;`
	slowLog := `# Time: 2026-08-17T14:09:13.980688+08:00
# Query_time: 1
` + query

	sctx := mock.NewContext()
	retriever, err := newSlowQueryRetriever()
	require.NoError(t, err)
	require.NoError(t, retriever.initialize(context.Background(), sctx))

	rows, err := parseLog(retriever, sctx, bufio.NewReader(strings.NewReader(slowLog)))
	require.NoError(t, err)
	require.Len(t, rows, 1)

	queryColumn := -1
	for i, column := range retriever.outputCols {
		if column.Name.L == strings.ToLower(variable.SlowLogQuerySQLStr) {
			queryColumn = i
			break
		}
	}
	require.NotEqual(t, -1, queryColumn)

	parsedQuery, err := rows[0][queryColumn].ToString()
	require.NoError(t, err)
	require.Equal(t, query, parsedQuery)
}
