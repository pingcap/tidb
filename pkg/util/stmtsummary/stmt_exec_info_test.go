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

package stmtsummary

import (
	"testing"

	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
)

func TestStmtExecInfoSetTiKVExecDetails(t *testing.T) {
	source := &tikvutil.ExecDetails{
		BackoffCount:       1,
		BackoffDuration:    2,
		WaitKVRespDuration: 3,
		WaitPDRespDuration: 4,
		TrafficDetails: tikvutil.TrafficDetails{
			UnpackedBytesSentKVTotal: 5,
		},
	}
	info := &StmtExecInfo{}
	info.SetTiKVExecDetails(source)
	require.Equal(t, source, info.TiKVExecDetails)
	require.NotSame(t, source, info.TiKVExecDetails)
	snapshot := info.TiKVExecDetails

	source.BackoffCount = 10
	source.UnpackedBytesSentKVTotal = 50
	require.Equal(t, int64(1), info.TiKVExecDetails.BackoffCount)
	require.Equal(t, int64(5), info.TiKVExecDetails.UnpackedBytesSentKVTotal)

	info.SetTiKVExecDetails(nil)
	require.Equal(t, &tikvutil.ExecDetails{}, info.TiKVExecDetails)
	require.Same(t, snapshot, info.TiKVExecDetails)
}
