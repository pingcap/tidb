// Copyright 2022 PingCAP, Inc.
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

package util

import (
	"bufio"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogFormat(t *testing.T) {
	mem := memory.NewTracker(-1, -1)
	mem.Consume(1<<30 + 1<<29 + 1<<28 + 1<<27)
	mockTooLongQuery := make([]byte, 1024*9)

	var refCount stmtctx.ReferenceCount = 0
	info := &ProcessInfo{
		ID:            233,
		User:          "PingCAP",
		Host:          "127.0.0.1",
		DB:            "Database",
		Info:          "select * from table where a > 1",
		CurTxnStartTS: 23333,
		StatsInfo: func(any) map[string]uint64 {
			return nil
		},
		StmtCtx:           stmtctx.NewStmtCtx(),
		RefCountOfStmtCtx: &refCount,
		MemTracker:        mem,
		RedactSQL:         "",
		SessionAlias:      "alias123",
	}
	costTime := time.Second * 233
	logSQLTruncateLen := 1024 * 8
	logFields := GenLogFields(costTime, info, true)

	assert.Len(t, logFields, 9)
	assert.Equal(t, "cost_time", logFields[0].Key)
	assert.Equal(t, "233s", logFields[0].String)
	assert.Equal(t, "conn", logFields[1].Key)
	assert.Equal(t, int64(233), logFields[1].Integer)
	assert.Equal(t, "user", logFields[2].Key)
	assert.Equal(t, "PingCAP", logFields[2].String)
	assert.Equal(t, "database", logFields[3].Key)
	assert.Equal(t, "Database", logFields[3].String)
	assert.Equal(t, "txn_start_ts", logFields[4].Key)
	assert.Equal(t, int64(23333), logFields[4].Integer)
	assert.Equal(t, "mem_max", logFields[5].Key)
	assert.Equal(t, "2013265920 Bytes (1.88 GB)", logFields[5].String)
	assert.Equal(t, "sql", logFields[6].Key)
	assert.Equal(t, "select * from table where a > 1", logFields[6].String)

	info.RedactSQL = errors.RedactLogMarker
	logFields = GenLogFields(costTime, info, true)
	assert.Equal(t, "select * from table where `a` > ‹1›", logFields[6].String)
	info.RedactSQL = ""

	logFields = GenLogFields(costTime, info, true)
	assert.Equal(t, "select * from table where a > 1", logFields[6].String)
	info.Info = string(mockTooLongQuery)
	logFields = GenLogFields(costTime, info, true)
	assert.Equal(t, len(logFields[6].String), logSQLTruncateLen+10)
	logFields = GenLogFields(costTime, info, false)
	assert.Equal(t, len(logFields[6].String), len(mockTooLongQuery))
	assert.Equal(t, logFields[7].String, "alias123")
}

func TestReadLine(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader(`line1
line2
line3`))
	line, err := ReadLine(reader, 1024)
	require.NoError(t, err)
	require.Equal(t, "line1", string(line))
	line, err = ReadLine(reader, 1024)
	require.NoError(t, err)
	require.Equal(t, "line2", string(line))
	line, err = ReadLine(reader, 1024)
	require.NoError(t, err)
	require.Equal(t, "line3", string(line))
	line, err = ReadLine(reader, 1024)
	require.Equal(t, io.EOF, err)
	require.Len(t, line, 0)
}

func TestIsInCorrectIdentifierName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		correct bool
	}{
		{"Empty identifier", "", true},
		{"Ending space", "test ", true},
		{"Correct identifier", "test", false},
		{"Other correct Identifier", "aaa --\n\txyz", false},
	}

	for _, tc := range tests {
		got := IsInCorrectIdentifierName(tc.input)
		require.Equalf(t, tc.correct, got, "IsInCorrectIdentifierName(%v) != %v", tc.name, tc.correct)
	}
}
