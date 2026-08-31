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

package server

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestTimeColumnComparedWithBinaryTemporalParameter(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	srv := CreateMockServer(t, store)
	srv.SetDomain(dom)
	defer srv.Close()

	c := CreateMockConn(t, srv).(*mockConn)
	c.capability = mysql.ClientProtocol41
	tk := testkit.NewTestKitWithSession(t, store, c.Context().Session)
	tk.MustExec("use test")
	tk.MustExec("set @@sql_mode = ''")
	tk.MustExec("create table t_time_binary (id bigint primary key, trx_time time(0), key(trx_time))")
	tk.MustExec("insert into t_time_binary values (1, '19:10:07'), (2, '19:10:07'), (3, '18:00:00'), (4, '18:00:00'), (5, '20:00:00')")

	stmt, _, _, err := c.Context().Prepare("select count(*) from t_time_binary where trx_time = ?")
	require.NoError(t, err)

	executeAndFetchCount := func(stmtID uint32, param []byte, expectedCount int64) {
		t.Helper()
		req := append(
			binary.LittleEndian.AppendUint32([]byte{mysql.ComStmtExecute}, stmtID),
			mysql.CursorTypeReadOnly, 0x1, 0x0, 0x0, 0x0,
			0x0, 0x1,
		)
		req = append(req, param...)
		require.NoError(t, c.Dispatch(context.Background(), req))

		out := c.GetOutput()
		expected := expectedLonglongFetchResult(t, c, expectedCount)
		binary.LittleEndian.PutUint16(expected[len(expected)-4:], 0)
		binary.LittleEndian.PutUint16(expected[len(expected)-2:], mysql.ServerStatusLastRowSend|mysql.ServerStatusAutocommit)
		fetch := binary.LittleEndian.AppendUint32(
			binary.LittleEndian.AppendUint32([]byte{mysql.ComStmtFetch}, stmtID), 1)
		require.NoError(t, c.Dispatch(context.Background(), fetch))
		require.NoError(t, c.flush(context.Background()))
		require.Equal(t, expected, out.Bytes())
	}

	tk.MustExec("set time_zone = '+08:00'")
	timestampParam := buildDatetimeParam(1970, 1, 1, 19, 10, 6, 600000)
	timestampParam[0] = mysql.TypeTimestamp
	executeAndFetchCount(uint32(stmt.ID()), timestampParam, 2)
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	timestampParam = buildDatetimeParam(2030, 12, 31, 20, 0, 0, 0)
	timestampParam[0] = mysql.TypeTimestamp
	executeAndFetchCount(uint32(stmt.ID()), timestampParam, 1)
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	datetimeParam := buildDatetimeParam(1970, 1, 1, 19, 10, 7, 0)
	executeAndFetchCount(uint32(stmt.ID()), datetimeParam, 2)

	invalidDatetimeParam := buildDatetimeParam(1970, 0, 1, 19, 10, 7, 0)
	executeAndFetchCount(uint32(stmt.ID()), invalidDatetimeParam, 0)
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	invalidTimestampParam := buildDatetimeParam(1970, 1, 0, 19, 10, 7, 0)
	invalidTimestampParam[0] = mysql.TypeTimestamp
	executeAndFetchCount(uint32(stmt.ID()), invalidTimestampParam, 0)
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)

	durationParam := []byte{mysql.TypeDuration, 0, 8, 0}
	durationParam = binary.LittleEndian.AppendUint32(durationParam, 0)
	durationParam = append(durationParam, 18, 0, 0)
	executeAndFetchCount(uint32(stmt.ID()), durationParam, 2)
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
}
