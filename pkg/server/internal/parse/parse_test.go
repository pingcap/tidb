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

package parse

import (
	"bytes"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func TestParseStmtFetchCmd(t *testing.T) {
	tests := []struct {
		arg       []byte
		stmtID    uint32
		fetchSize uint32
		err       error
	}{
		{[]byte{3, 0, 0, 0, 50, 0, 0, 0}, 3, 50, nil},
		{[]byte{5, 0, 0, 0, 232, 3, 0, 0}, 5, 1000, nil},
		{[]byte{5, 0, 0, 0, 0, 8, 0, 0}, 5, maxFetchSize, nil},
		{[]byte{5, 0, 0}, 0, 0, mysql.ErrMalformPacket},
		{[]byte{1, 0, 0, 0, 3, 2, 0, 0, 3, 5, 6}, 0, 0, mysql.ErrMalformPacket},
		{[]byte{}, 0, 0, mysql.ErrMalformPacket},
	}

	for _, tc := range tests {
		stmtID, fetchSize, err := StmtFetchCmd(tc.arg)
		require.Equal(t, tc.stmtID, stmtID)
		require.Equal(t, tc.fetchSize, fetchSize)
		require.Equal(t, tc.err, err)
	}
}

func TestParseAttrsUnderscoreWarning(t *testing.T) {
	origSize := vardef.ConnectAttrsSize.Load()
	defer vardef.ConnectAttrsSize.Store(origSize)
	vardef.ConnectAttrsSize.Store(-1)

	buildAttrsPayload := func(kvs [][2]string) []byte {
		var buf bytes.Buffer
		for _, kv := range kvs {
			buf.WriteByte(byte(len(kv[0])))
			buf.WriteString(kv[0])
			buf.WriteByte(byte(len(kv[1])))
			buf.WriteString(kv[1])
		}
		return buf.Bytes()
	}

	t.Run("warn for custom underscore attrs", func(t *testing.T) {
		payload := buildAttrsPayload([][2]string{
			{"_client_name", "libmysql"},
			{"_custom", "val"},
			{"_program_name", "mysql"},
			{"app_name", "myapp"},
		})

		attrs, warning, err := parseAttrs(payload)
		require.NoError(t, err)
		require.Equal(t, "libmysql", attrs["_client_name"])
		require.Equal(t, "val", attrs["_custom"])
		require.Equal(t, "mysql", attrs["_program_name"])
		require.Equal(t, "myapp", attrs["app_name"])
		require.Contains(t, warning, "custom connection attributes with leading underscore are deprecated and will be rejected in a future release")
	})

	t.Run("no warning for standard underscore attrs", func(t *testing.T) {
		payload := buildAttrsPayload([][2]string{
			{"_client_name", "libmysql"},
			{"_client_version", "8.0.33"},
			{"_os", "linux"},
			{"_pid", "123"},
			{"_platform", "x86_64"},
			{"app_name", "myapp"},
		})

		_, warning, err := parseAttrs(payload)
		require.NoError(t, err)
		require.Empty(t, warning)
	})

	t.Run("server may overwrite client _truncated on truncation", func(t *testing.T) {
		origLost := vardef.ConnectAttrsLost.Load()
		defer vardef.ConnectAttrsLost.Store(origLost)
		vardef.ConnectAttrsLost.Store(0)
		vardef.ConnectAttrsSize.Store(20)

		payload := buildAttrsPayload([][2]string{
			{"_truncated", "client-value"},
			{"app_name", "my_service"},
		})

		attrs, warning, err := parseAttrs(payload)
		require.NoError(t, err)
		require.Contains(t, warning, "session connection attributes truncated")
		require.NotEqual(t, "client-value", attrs["_truncated"])
		require.Equal(t, int64(1), vardef.ConnectAttrsLost.Load())
	})
}
