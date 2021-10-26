// Copyright 2021 PingCAP, Inc.
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

package parser_test

import (
	"runtime"
	"strings"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/stretchr/testify/require"
)

func TestInsertStatementMemoryAllocation(t *testing.T) {
	sql := "insert t values (1)" + strings.Repeat(",(1)", 1000)
	var oldStats, newStats runtime.MemStats
	runtime.ReadMemStats(&oldStats)
	_, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	runtime.ReadMemStats(&newStats)
	require.Less(t, int(newStats.TotalAlloc-oldStats.TotalAlloc), 1024*500)
}
