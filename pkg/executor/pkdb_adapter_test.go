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

package executor_test

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestPKDBSlowLogEscapeNewline(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set session tidb_slow_log_rules="Succ:true"`)

	core, recorded := observer.New(zap.WarnLevel)
	previousLogger := logutil.SlowQueryLogger
	logutil.SlowQueryLogger = zap.New(core)
	previousMaxQueryLength := variable.QueryLogMaxLen.Load()
	previousEscapeNewline := variable.SlowLogEscapeNewline.Load()
	variable.QueryLogMaxLen.Store(0)
	variable.SlowLogEscapeNewline.Store(false)
	t.Cleanup(func() {
		logutil.SlowQueryLogger = previousLogger
		variable.QueryLogMaxLen.Store(previousMaxQueryLength)
		variable.SlowLogEscapeNewline.Store(previousEscapeNewline)
	})

	loggedSQL := func(sql string) string {
		t.Helper()
		recorded.TakeAll()
		tk.MustQuery(sql).Check(testkit.Rows("1 a 100"))

		entries := recorded.All()
		require.Len(t, entries, 1)
		sqlStart := strings.Index(entries[0].Message, "select 1,")
		require.NotEqual(t, -1, sqlStart)
		return entries[0].Message[sqlStart:]
	}

	multilineSQL := "select 1,\n  'a', -- test slow log\n  100;"
	require.False(t, variable.DefTiDBSlowLogEscapeNewline)
	tk.MustQuery("select @@global.tidb_slow_log_escape_newline").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_slow_log_escape_newline = OFF")
	require.Equal(t, executor.FormatSQL(multilineSQL).String(), loggedSQL(multilineSQL))

	tk.MustExec("set global tidb_slow_log_escape_newline = ON")
	tk.MustQuery("select @@global.tidb_slow_log_escape_newline").Check(testkit.Rows("1"))
	require.Equal(t, `select 1,\n  'a', -- test slow log\n  100;`, loggedSQL(multilineSQL))

	crlfSQL := strings.ReplaceAll(multilineSQL, "\n", "\r\n")
	require.Equal(t, `select 1,\n  'a', -- test slow log\n  100;`, loggedSQL(crlfSQL))
}
