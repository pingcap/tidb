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

package brietest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

// MockGC is used to make GC work in the test environment.
func MockGC(tk *testkit.TestKit) (string, string, string, func()) {
	originGC := ddlutil.IsEmulatorGCEnable()
	resetGC := func() {
		if originGC {
			ddlutil.EmulatorGCEnable()
		} else {
			ddlutil.EmulatorGCDisable()
		}
	}

	// disable emulator GC.
	// Otherwise emulator GC will delete table record as soon as possible after execute drop table ddl.
	ddlutil.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	timeAfterDrop := time.Now().Add(48 * 60 * 60 * time.Second).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	// clear GC variables first.
	tk.MustExec("delete from mysql.tidb where variable_name in ( 'tikv_gc_safe_point','tikv_gc_enable' )")
	return timeBeforeDrop, timeAfterDrop, safePointSQL, resetGC
}

func TestFlashback(t *testing.T) {
	if *realtikvtest.WithRealTiKV {
		store := realtikvtest.CreateMockStoreAndSetup(t)

		tk := testkit.NewTestKit(t, store)

		timeBeforeDrop, _, safePointSQL, resetGC := MockGC(tk)
		defer resetGC()

		tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, index i(a))")
		tk.MustExec("insert t values (1), (2), (3)")

		time.Sleep(1 * time.Second)

		ts, err := tk.Session().GetStore().GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
		require.NoError(t, err)

		injectSafeTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(100 * time.Second))
		require.NoError(t, failpoint.Enable("tikvclient/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/expression/injectSafeTS",
			fmt.Sprintf("return(%v)", injectSafeTS)))

		tk.MustExec("insert t values (4), (5), (6)")
		tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", oracle.GetTimeFromTS(ts)))

		tk.MustExec("admin check table t")
		require.Equal(t, tk.MustQuery("select max(a) from t").Rows()[0][0], "3")
		require.Equal(t, tk.MustQuery("select max(a) from t use index(i)").Rows()[0][0], "3")

		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/expression/injectSafeTS"))
		require.NoError(t, failpoint.Disable("tikvclient/injectSafeTS"))
	}
}
