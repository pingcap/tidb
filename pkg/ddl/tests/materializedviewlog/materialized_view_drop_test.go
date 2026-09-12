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

package materializedviewlog_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestDropMaterializedViewLogRecheckWithConcurrentCreateMaterializedView(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_recheck (a int not null, b int not null)")
	tk.MustExec("insert into t_drop_recheck values (1, 10), (1, 5), (2, 7)")
	tk.MustExec("create materialized view log on t_drop_recheck (a, b) purge next date_add(now(), interval 1 hour)")

	const pauseDropFailpoint = "github.com/pingcap/tidb/pkg/ddl/pauseDropMaterializedViewLogAfterCheck"
	const afterCheckDropFailpoint = "github.com/pingcap/tidb/pkg/ddl/afterCheckDropMaterializedViewLog"
	dropCheckDoneCh := make(chan struct{})
	var dropCheckDoneOnce sync.Once
	testfailpoint.EnableCall(t, afterCheckDropFailpoint, func() {
		dropCheckDoneOnce.Do(func() {
			close(dropCheckDoneCh)
		})
	})
	require.NoError(t, failpoint.Enable(pauseDropFailpoint, "pause"))
	enabled := true
	defer func() {
		if enabled {
			require.NoError(t, failpoint.Disable(pauseDropFailpoint))
		}
	}()

	dropErrCh := make(chan error, 1)
	go func() {
		tkDrop := newMViewTestKit(t, store)
		tkDrop.MustExec("use test")
		dropErrCh <- tkDrop.ExecToErr("drop materialized view log on t_drop_recheck")
	}()

	select {
	case <-dropCheckDoneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for DROP MATERIALIZED VIEW LOG precheck")
	}
	tk.MustExec("create materialized view mv_drop_dep (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_drop_recheck group by a")

	require.NoError(t, failpoint.Disable(pauseDropFailpoint))
	enabled = false

	err := <-dropErrCh
	require.ErrorContains(t, err, "dependent materialized views exist")
	tk.MustQuery("show tables like '$mlog$t_drop_recheck'").Check(testkit.Rows("$mlog$t_drop_recheck"))

	tk.MustExec("drop materialized view mv_drop_dep")
	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("$mlog$t_drop_recheck"))
	require.NoError(t, err)
	require.Empty(t, mlogTable.Meta().MaterializedViewLog.DependentMViewIDs)
	tk.MustExec("drop materialized view log on t_drop_recheck")

	is = dom.InfoSchema()
	baseTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_drop_recheck"))
	require.NoError(t, err)
	require.True(t, baseTable.Meta().MaterializedViewBase == nil || (baseTable.Meta().MaterializedViewBase.MLogID == 0 && len(baseTable.Meta().MaterializedViewBase.MViewIDs) == 0))
}

func TestDropMaterializedViewLogPurgeInfoFailureRollsBackMetadata(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_mlog_atomic (a int)")
	tk.MustExec("create materialized view log on t_drop_mlog_atomic (a)")

	is := dom.InfoSchema()
	mlogTable, err := is.TableByName(context.Background(), ast.NewCIStr("test"), model.MaterializedViewLogTableName(ast.NewCIStr("t_drop_mlog_atomic")))
	require.NoError(t, err)
	mlogID := mlogTable.Meta().ID

	const cleanupErrFP = "github.com/pingcap/tidb/pkg/ddl/mockDeleteMaterializedViewLogPurgeInfoErr"
	require.NoError(t, failpoint.Enable(cleanupErrFP, `1*return("mock purge info delete error")`))
	defer func() { require.NoError(t, failpoint.Disable(cleanupErrFP)) }()

	retryStarted := make(chan struct{})
	allowRetry := make(chan struct{})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionDropMaterializedViewLog && job.TableID == mlogID && job.SchemaState == model.StateDeleteOnly && job.ErrorCount > 0 {
			select {
			case <-retryStarted:
			default:
				close(retryStarted)
			}
			<-allowRetry
		}
	})

	tkInspect := newMViewTestKit(t, store)
	tkInspect.MustExec("use test")
	dropErrCh := make(chan error, 1)
	go func() { dropErrCh <- tk.ExecToErr("drop materialized view log on t_drop_mlog_atomic") }()

	select {
	case <-retryStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for DROP MATERIALIZED VIEW LOG retry")
	}
	tkInspect.MustQuery("show tables like '$mlog$t_drop_mlog_atomic'").Check(testkit.Rows("$mlog$t_drop_mlog_atomic"))
	tkInspect.MustQuery(fmt.Sprintf("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = %d", mlogID)).Check(testkit.Rows("1"))

	require.NoError(t, failpoint.Disable(cleanupErrFP))
	close(allowRetry)
	require.NoError(t, <-dropErrCh)
}

func TestDropMaterializedViewLogPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newMViewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_drop_mlog_priv (a int)")
	tk.MustExec("create materialized view log on t_drop_mlog_priv (a)")
	tk.MustExec("create user 'u_drop_mlog_select'@'%'")
	tk.MustExec("create user 'u_drop_mlog_ok'@'%'")
	defer tk.MustExec("drop user 'u_drop_mlog_select'@'%'")
	defer tk.MustExec("drop user 'u_drop_mlog_ok'@'%'")
	tk.MustExec("grant select on test.t_drop_mlog_priv to 'u_drop_mlog_select'@'%'")
	tk.MustExec("grant drop on test.`$mlog$t_drop_mlog_priv` to 'u_drop_mlog_ok'@'%'")

	tkSelect := newMViewTestKit(t, store)
	require.NoError(t, tkSelect.Session().Auth(&auth.UserIdentity{Username: "u_drop_mlog_select", Hostname: "%"}, nil, nil, nil))
	err := tkSelect.ExecToErr("drop materialized view log on test.t_drop_mlog_priv")
	require.ErrorContains(t, err, "DROP MATERIALIZED VIEW LOG command denied")
	require.ErrorContains(t, err, "for table 't_drop_mlog_priv'")

	tkDrop := newMViewTestKit(t, store)
	require.NoError(t, tkDrop.Session().Auth(&auth.UserIdentity{Username: "u_drop_mlog_ok", Hostname: "%"}, nil, nil, nil))
	tkDrop.MustExec("drop materialized view log on test.t_drop_mlog_priv")
}
