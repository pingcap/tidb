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

package integration_test

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestTiDBRowIDDiagnostics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table locator_int (a bigint primary key clustered)")
	tk.MustExec("insert into locator_int values (7)")

	intToken := tk.MustQuery("select tidb_row_id() from locator_int").Rows()[0][0].(string)
	intPhysicalID, _, _, _, err := tablecodec.DecodeTiDBRowID(intToken)
	require.NoError(t, err)
	intDecoded := decodeTiDBKeyJSON(t, tk, intToken)
	require.Equal(t, strconv.FormatInt(intPhysicalID, 10), intDecoded["table_id"])
	require.Equal(t, float64(7), intDecoded["a"])

	tk.MustExec("create table locator_common (a bigint, b bigint, primary key (a, b) clustered)")
	tk.MustExec("insert into locator_common values (8, 9)")
	commonToken := tk.MustQuery("select tidb_row_id() from locator_common").Rows()[0][0].(string)
	commonPhysicalID, _, _, _, err := tablecodec.DecodeTiDBRowID(commonToken)
	require.NoError(t, err)
	commonDecoded := decodeTiDBKeyJSON(t, tk, commonToken)
	require.Equal(t, float64(commonPhysicalID), commonDecoded["table_id"])
	require.Equal(t, map[string]any{"a": "8", "b": "9"}, commonDecoded["handle"])
}

func decodeTiDBKeyJSON(t *testing.T, tk *testkit.TestKit, token string) map[string]any {
	t.Helper()
	decoded := tk.MustQuery("select tidb_decode_key(?)", token).Rows()[0][0].(string)
	result := make(map[string]any)
	require.NoError(t, json.Unmarshal([]byte(decoded), &result), decoded)
	return result
}
