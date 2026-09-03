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

package tiflashtest

import (
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestCTASWithMPP verifies that CREATE TABLE ... AS SELECT whose SELECT is
// executed through MPP works when the submitting session has no process info.
//
// The CTAS data-loading phase runs in an internal session created by
// `executor.CreateSession`, which never calls `SetProcessInfo`. When the
// source table has an available TiFlash replica and the optimizer picks an MPP
// plan for the full table scan, the old code read the session alias via
// `sessionCtx.ShowProcess().SessionAlias` which panicked with a nil pointer
// dereference because `ShowProcess()` returned nil. The fix reads the alias
// from session variables instead.
func TestCTASWithMPP(t *testing.T) {
	s, teardown := createTiFlashContext(t)
	defer teardown()
	sm := &testkit.MockSessionManager{}
	s.dom.InfoSyncer().SetSessionManager(sm)
	tk := testkit.NewTestKit(t, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists ctas_src")
	tk.MustExec("create table ctas_src(id int not null primary key, v varchar(64), n decimal(10,2))")
	tk.MustExec("alter table ctas_src set tiflash replica 1")
	// The CTAS read ts is the txn start ts of the DDL statement, so a fresh
	// transaction is started after the replica becomes available to make sure
	// the snapshot info schema observes the replica status schema change.
	WaitTablesAvailableWithTableName(s.dom, t, 1, nil, "test", []string{"ctas_src"}, 10*time.Second)
	tk.MustExec("begin")
	tk.MustExec("select * from ctas_src limit 1")
	tk.MustExec("commit")

	tk.MustExec("set @@session.sql_mode = ''")
	tk.MustExec("insert into ctas_src values (1,'a',10.10),(2,'b',20.20),(3,'c',30.30)")
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_mpp = 1")
	tk.MustExec("set @@session.tidb_enforce_mpp = 1")
	tk.MustExec("set @@session.tidb_opt_enable_late_materialization = 0")

	// make sure the plan is an MPP plan
	rows := tk.MustQuery("explain format='brief' select * from ctas_src").Rows()
	foundMPP := false
	for _, r := range rows {
		for _, cell := range r {
			if s, ok := cell.(string); ok && strings.Contains(s, "mpp[tiflash]") {
				foundMPP = true
			}
		}
	}
	require.True(t, foundMPP, "expected an MPP plan, got: %v", rows)

	tk.MustExec("drop table if exists ctas_dst")
	tk.MustExec("create table ctas_dst as select * from ctas_src")
	// reset isolation read engines so queries on the replica-less dst table work
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tikv,tiflash'")
	tk.MustExec("set @@session.tidb_enforce_mpp = 0")
	tk.MustQuery("select count(*) from ctas_dst").Check(testkit.Rows("3"))
	tk.MustQuery("select * from ctas_dst order by id").Check(testkit.Rows(
		"1 a 10.10", "2 b 20.20", "3 c 30.30"))
}
