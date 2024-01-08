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
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
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
