// Copyright 2025 PingCAP, Inc.
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

package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestBindUsageInfo(t *testing.T) {
	checklist := []string{
		"5ce1df6eadf8b24222668b1bd2e995b72d4c88e6fe9340d8b13e834703e28c32",
		"5d3975ef2160c1e0517353798dac90a9914095d82c025e7cd97bd55aeb804798",
		"9d3995845aef70ba086d347f38a4e14c9705e966f7c5793b9fa92194bca2bbef",
		"aa3c510b94b9d680f729252ca88415794c8a4f52172c5f9e06c27bee57d08329",
	}
	bindinfo.UpdateBindingUsageInfoBatchSize = 2
	bindinfo.MaxWriteInterval = 100 * time.Microsecond
	store, dom := testkit.CreateMockStoreAndDomain(t)
	bindingHandle := dom.BindingHandle()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use test`)
	tk.MustExec(`set @@tidb_opt_enable_fuzzy_binding=1`)
	tk.MustExec("create table t1(a int, b int, c int, key idx_b(b), key idx_c(c))")
	tk.MustExec("create table t2(a int, b int, c int, key idx_b(b), key idx_c(c))")
	tk.MustExec("create table t3(a int, b int, c int, key idx_b(b), key idx_c(c))")
	tk.MustExec("create table t4(a int, b int, c int, key idx_b(b), key idx_c(c))")
	tk.MustExec("create table t5(a int, b int, c int, key idx_b(b), key idx_c(c))")

	tk.MustExec("prepare stmt1 from 'delete from t1 where b = 1 and c > 1';")
	tk.MustExec("prepare stmt2 from 'delete t1, t2 from t1 inner join t2 on t1.b = t2.b';")
	tk.MustExec("prepare stmt3 from 'update t1 set a = 1 where b = 1 and c > 1';")
	tk.MustExec("execute stmt1;")
	tk.MustExec("create global binding for delete from t1 where b = 1 and c > 1 using delete /*+ use_index(t1,idx_c) */ from t1 where b = 1 and c > 1")
	tk.MustExec("create global binding for delete t1, t2 from t1 inner join t2 on t1.b = t2.b using delete /*+ inl_join(t1) */ t1, t2 from t1 inner join t2 on t1.b = t2.b")
	tk.MustExec("create global binding for update t1 set a = 1 where b = 1 and c > 1 using update /*+ use_index(t1,idx_c) */ t1 set a = 1 where b = 1 and c > 1")
	// cross database binding
	tk.MustExec(`create global binding using select /*+ leading(t1, t2, t3, t4, t5) */ * from *.t1, *.t2, *.t3, *.t4, *.t5`)
	tk.MustExec("select * from t1, t2, t3, t4, t5")
	tk.MustExec("execute stmt1;")
	origin := tk.MustQuery(fmt.Sprintf(`select sql_digest,last_used_date from mysql.bind_info where original_sql != '%s' order by sql_digest`, bindinfo.BuiltinPseudoSQL4BindLock))
	origin.Check(testkit.Rows(
		"5ce1df6eadf8b24222668b1bd2e995b72d4c88e6fe9340d8b13e834703e28c32 <nil>",
		"5d3975ef2160c1e0517353798dac90a9914095d82c025e7cd97bd55aeb804798 <nil>",
		"9d3995845aef70ba086d347f38a4e14c9705e966f7c5793b9fa92194bca2bbef <nil>",
		"aa3c510b94b9d680f729252ca88415794c8a4f52172c5f9e06c27bee57d08329 <nil>"))
	time.Sleep(50 * time.Microsecond)
	require.NoError(t, bindingHandle.UpdateBindingUsageInfoToStorage())
	result := tk.MustQuery(fmt.Sprintf(`select sql_digest,last_used_date from mysql.bind_info where original_sql != '%s' order by sql_digest`, bindinfo.BuiltinPseudoSQL4BindLock))
	t.Log("result:", result.Rows())
	// The last_used_date should be updated.
	require.True(t, !origin.Equal(result.Rows()))
	var first *testkit.Result
	for range 5 {
		tk.MustExec("execute stmt1;")
		tk.MustExec("execute stmt2;")
		tk.MustExec("execute stmt3;")
		tk.MustExec("select * from t1, t2, t3, t4, t5")
		time.Sleep(1 * time.Second)
		// Set all last_used_date to null to simulate that the bindinfo in storage is not updated.
		resetAllLastUsedData(tk)
		require.NoError(t, bindingHandle.UpdateBindingUsageInfoToStorage())
		checkBindinfoInMemory(t, bindingHandle, checklist)
		tk.MustQuery(fmt.Sprintf(`select last_used_date from mysql.bind_info where original_sql != '%s' and last_used_date is null`, bindinfo.BuiltinPseudoSQL4BindLock)).Check(testkit.Rows())
		result := tk.MustQuery(fmt.Sprintf(`select sql_digest,last_used_date from mysql.bind_info where original_sql != '%s' order by sql_digest`, bindinfo.BuiltinPseudoSQL4BindLock))
		t.Log("result:", result.Rows())
		if first == nil {
			first = result
		} else {
			// in fact, The result of each for-loop should be the same.
			require.True(t, first.Equal(result.Rows()))
		}
	}
	// Set all last_used_date to null to simulate that the bindinfo in storage is not updated.
	resetAllLastUsedData(tk)
	for range 5 {
		time.Sleep(1 * time.Second)
		// No used, so last_used_date should not be updated.
		require.NoError(t, bindingHandle.UpdateBindingUsageInfoToStorage())
		tk.MustQuery(`select last_used_date from mysql.bind_info where last_used_date is not null`).Check(testkit.Rows())
	}
	tk.MustExec("execute stmt1;")
	tk.MustExec("execute stmt2;")
	tk.MustExec("execute stmt3;")
	time.Sleep(1 * time.Second)
	require.NoError(t, bindingHandle.UpdateBindingUsageInfoToStorage())
	// it has been updated again.
	rows := tk.MustQuery(
		fmt.Sprintf(`select * from mysql.bind_info where original_sql != '%s' and last_used_date is not null`, bindinfo.BuiltinPseudoSQL4BindLock)).Rows()
	require.Len(t, rows, 3)
	// Set all last_used_date to null to simulate that the bindinfo in storage is not updated.
	resetAllLastUsedData(tk)
	tk.MustExec(`set @@global.tidb_enable_binding_usage=0;`)
	for range 5 {
		tk.MustExec("execute stmt1;")
		tk.MustExec("execute stmt2;")
		tk.MustExec("execute stmt3;")
		tk.MustExec("select * from t1, t2, t3, t4, t5")
		time.Sleep(1 * time.Second)
		require.NoError(t, bindingHandle.UpdateBindingUsageInfoToStorage())
		tk.MustQuery(fmt.Sprintf(`select last_used_date from mysql.bind_info where original_sql != '%s' and last_used_date is not null`, bindinfo.BuiltinPseudoSQL4BindLock)).
			Check(testkit.Rows())
	}
}

func resetAllLastUsedData(tk *testkit.TestKit) {
	tk.MustExec(fmt.Sprintf(`update mysql.bind_info set last_used_date = null where original_sql != '%s'`, bindinfo.BuiltinPseudoSQL4BindLock))
}

func checkBindinfoInMemory(t *testing.T, bindingHandle bindinfo.BindingHandle, checklist []string) {
	for _, digest := range checklist {
		binding := bindingHandle.GetBinding(digest)
		require.NotNil(t, binding)
		lastSaved := binding.UsageInfo.LastSavedAt.Load()
		if lastSaved != nil {
			require.GreaterOrEqual(t, *binding.UsageInfo.LastSavedAt.Load(), *binding.UsageInfo.LastUsedAt.Load())
		}
	}
}
