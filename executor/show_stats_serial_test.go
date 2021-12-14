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

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestShowAnalyzeStatus(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	statistics.ClearHistoryJobs()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, primary key(a), index idx(b))")
	tk.MustExec(`insert into t values (1, 1), (2, 2)`)

	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("analyze table t")
	result := tk.MustQuery("show analyze status").Sort()
	require.Len(t, result.Rows(), 1)
	require.Equal(t, "test", result.Rows()[0][0])
	require.Equal(t, "t", result.Rows()[0][1])
	require.Equal(t, "", result.Rows()[0][2])
	require.Equal(t, "analyze table", result.Rows()[0][3])
	require.Equal(t, "2", result.Rows()[0][4])
	require.NotNil(t, result.Rows()[0][5])
	require.NotNil(t, result.Rows()[0][6])
	require.Equal(t, "finished", result.Rows()[0][7])

	statistics.ClearHistoryJobs()

	tk.MustExec("set @@tidb_analyze_version=1")
	tk.MustExec("analyze table t")
	result = tk.MustQuery("show analyze status").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "test", result.Rows()[0][0])
	require.Equal(t, "t", result.Rows()[0][1])
	require.Equal(t, "", result.Rows()[0][2])
	require.Equal(t, "analyze columns", result.Rows()[0][3])
	require.Equal(t, "2", result.Rows()[0][4])
	require.NotNil(t, result.Rows()[0][5])
	require.NotNil(t, result.Rows()[0][6])
	require.Equal(t, "finished", result.Rows()[0][7])

	require.Len(t, result.Rows(), 2)
	require.Equal(t, "test", result.Rows()[1][0])
	require.Equal(t, "t", result.Rows()[1][1])
	require.Equal(t, "", result.Rows()[1][2])
	require.Equal(t, "analyze index idx", result.Rows()[1][3])
	require.Equal(t, "2", result.Rows()[1][4])
	require.NotNil(t, result.Rows()[1][5])
	require.NotNil(t, result.Rows()[1][6])
	require.Equal(t, "finished", result.Rows()[1][7])
}
