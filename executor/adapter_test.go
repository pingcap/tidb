// Copyright 2019 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestQueryTime(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	costTime := time.Since(tk.Session().GetSessionVars().StartTime)
	require.Less(t, costTime, time.Second)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1), (1), (1), (1), (1)")
	tk.MustExec("select * from t t1 join t t2 on t1.a = t2.a")

	costTime = time.Since(tk.Session().GetSessionVars().StartTime)
	require.Less(t, costTime, time.Second)
}

func TestFormatSQL(t *testing.T) {
	val := executor.FormatSQL("aaaa")
	require.Equal(t, "aaaa", val.String())
	variable.QueryLogMaxLen.Store(0)
	val = executor.FormatSQL("aaaaaaaaaaaaaaaaaaaa")
	require.Equal(t, "aaaaaaaaaaaaaaaaaaaa", val.String())
	variable.QueryLogMaxLen.Store(5)
	val = executor.FormatSQL("aaaaaaaaaaaaaaaaaaaa")
	require.Equal(t, "\"aaaaa\"(len:20)", val.String())
}
