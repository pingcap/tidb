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

package servermemorylimit

import (
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestMemoryUsageOpsHistory(t *testing.T) {
	info := util.ProcessInfo{}
	genInfo := func(i int) {
		info.ID = uint64(i)
		info.DB = strconv.Itoa(2 * i)
		info.User = strconv.Itoa(3 * i)
		info.Host = strconv.Itoa(4 * i)
		info.Digest = strconv.Itoa(5 * i)
		info.Info = strconv.Itoa(6 * i)
	}

	for i := 0; i < 3; i++ {
		genInfo(i)
		GlobalMemoryOpsHistoryManager.recordOne(&info, time.Now(), uint64(i), uint64(2*i))
	}

	checkResult := func(datums []types.Datum, i int) {
		require.Equal(t, datums[1].GetString(), "SessionKill")
		require.Equal(t, datums[2].GetInt64(), int64(i))
		require.Equal(t, datums[3].GetInt64(), int64(2*i))
		require.Equal(t, datums[4].GetInt64(), int64(i))
		require.Equal(t, datums[7].GetString(), strconv.Itoa(4*i))
		require.Equal(t, datums[8].GetString(), strconv.Itoa(2*i))
		require.Equal(t, datums[9].GetString(), strconv.Itoa(3*i))
		require.Equal(t, datums[10].GetString(), strconv.Itoa(5*i))
		require.Equal(t, datums[11].GetString(), strconv.Itoa(6*i))
	}

	rows := GlobalMemoryOpsHistoryManager.GetRows()
	require.Equal(t, 3, len(rows))
	for i := 0; i < 3; i++ {
		checkResult(rows[i], i)
	}
	// Test evict
	for i := 3; i < 53; i++ {
		genInfo(i)
		GlobalMemoryOpsHistoryManager.recordOne(&info, time.Now(), uint64(i), uint64(2*i))
	}
	rows = GlobalMemoryOpsHistoryManager.GetRows()
	require.Equal(t, 50, len(rows))
	for i := 3; i < 53; i++ {
		checkResult(rows[i-3], i)
	}
	require.Equal(t, GlobalMemoryOpsHistoryManager.offsets, 3)
}
