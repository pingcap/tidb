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

package recovertest

import (
	"fmt"
	"sync"
	"testing"
	"time"

	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	tikvutil "github.com/tikv/client-go/v2/util"
)

func TestFlashbackSchemaWithManyTables(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/meta/autoid/mockAutoIDChange", `return(true)`)

	backup := kv.TxnEntrySizeLimit.Load()
	kv.TxnEntrySizeLimit.Store(50000)
	t.Cleanup(func() {
		kv.TxnEntrySizeLimit.Store(backup)
	})

	store := testkit.CreateMockStore(t, mockstore.WithStoreType(mockstore.EmbedUnistore))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 2")
	tk.MustExec("set @@global.tidb_enable_fast_create_table=ON")
	tk.MustExec("drop database if exists many_tables")
	tk.MustExec("create database if not exists many_tables")
	tk.MustExec("use many_tables")

	timeBeforeDrop, _, safePointSQL, resetGC := mockGC(tk)
	defer resetGC()

	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	require.NoError(t, gcutil.EnableGC(tk.Session()))

	var wg util.WaitGroupWrapper
	for i := range 10 {
		idx := i
		wg.Run(func() {
			tkit := testkit.NewTestKit(t, store)
			tkit.MustExec("use many_tables")
			for j := range 70 {
				tkit.MustExec(fmt.Sprintf("create table t_%d_%d (a int)", idx, j))
			}
		})
	}
	wg.Wait()

	tk.MustExec("drop database many_tables")
	tk.MustExec("flashback database many_tables")
	tk.MustQuery("select count(*) from many_tables.t_0_0").Check(testkit.Rows("0"))
}

func TestFlashbackClusterWithManyDBs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	timeBeforeDrop, _, safePointSQL, resetGC := mockGC(tk)
	defer resetGC()

	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	backup := kv.TxnEntrySizeLimit.Load()
	kv.TxnEntrySizeLimit.Store(50000)
	t.Cleanup(func() {
		kv.TxnEntrySizeLimit.Store(backup)
	})

	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 2")
	tk.MustExec("set @@global.tidb_enable_fast_create_table=ON")

	var wg sync.WaitGroup
	dbPerWorker := 10
	for i := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk2 := testkit.NewTestKit(t, store)
			for j := range dbPerWorker {
				dbName := fmt.Sprintf("db_%d", i*dbPerWorker+j)
				tk2.MustExec(fmt.Sprintf("create database %s", dbName))
			}
		}()
	}

	wg.Wait()

	ts, _ := store.CurrentVersion(oracle.GlobalTxnScope)
	flashbackTs := oracle.GetTimeFromTS(ts.Ver)

	injectSafeTS := oracle.GoTimeToTS(flashbackTs.Add(10 * time.Second))
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockFlashbackTest", `return(true)`)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/injectSafeTS",
		fmt.Sprintf("return(%v)", injectSafeTS))

	tk.MustExec(fmt.Sprintf("flashback cluster to timestamp '%s'", flashbackTs))
}

func mockGC(tk *testkit.TestKit) (string, string, string, func()) {
	originGC := ddlutil.IsEmulatorGCEnable()
	resetGC := func() {
		if originGC {
			ddlutil.EmulatorGCEnable()
		} else {
			ddlutil.EmulatorGCDisable()
		}
	}

	ddlutil.EmulatorGCDisable()
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(tikvutil.GCTimeFormat)
	timeAfterDrop := time.Now().Add(48 * 60 * 60 * time.Second).Format(tikvutil.GCTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	tk.MustExec("delete from mysql.tidb where variable_name in ( 'tikv_gc_safe_point','tikv_gc_enable' )")
	return timeBeforeDrop, timeAfterDrop, safePointSQL, resetGC
}
