// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddltest_test

import (
	"fmt"
	"testing"
	"time"

	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
)

func TestSoftDeleteRecoverTable(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Setup GC: disable emulator GC and set a safe point in the past
	// so that RECOVER TABLE can find the dropped table data.
	originGC := ddlutil.IsEmulatorGCEnable()
	ddlutil.EmulatorGCDisable()
	defer func() {
		if originGC {
			ddlutil.EmulatorGCEnable()
		} else {
			ddlutil.EmulatorGCDisable()
		}
	}()
	timeBeforeDrop := time.Now().Add(-48 * time.Hour).Format(tikvutil.GCTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	tk.MustExec("delete from mysql.tidb where variable_name in ('tikv_gc_safe_point','tikv_gc_enable')")
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))

	// Create soft-delete table and insert data
	tk.MustExec("drop table if exists message")
	tk.MustExec("create table message (id int primary key, text varchar(10)) softdelete retention 7 day")
	tk.MustExec("set @@tidb_translate_softdelete_sql = true")
	tk.MustExec("insert into message values (1, '24h'), (2, '24h')")
	tk.MustQuery("select * from message order by id").Check(testkit.Rows("1 24h", "2 24h"))

	// Record SHOW CREATE TABLE before dropping
	createTableBefore := tk.MustQuery("show create table message").Rows()
	require.Len(t, createTableBefore, 1)

	// Drop and recover
	tk.MustExec("drop table message")
	tk.MustExec("recover table message")

	// Verify data is recovered
	tk.MustQuery("select * from message order by id").Check(testkit.Rows("1 24h", "2 24h"))

	// Verify SHOW CREATE TABLE is consistent with before dropping
	createTableAfter := tk.MustQuery("show create table message").Rows()
	require.Len(t, createTableAfter, 1)
	require.Equal(t, createTableBefore[0][1], createTableAfter[0][1])

	// Clean up
	tk.MustExec("drop table if exists message")
}
