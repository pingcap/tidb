// Copyright 2026 PingCAP, Inc.

package executor_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/stretchr/testify/require"
)

func TestExplainForRemoteBriefBinaryPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tkRemote := testkit.NewTestKit(t, store)
	tkExplain := testkit.NewTestKit(t, store)

	tkRemote.MustExec("use test")
	tkExplain.MustExec("use test")
	tkRemote.MustExec("set @@tidb_enable_collect_execution_info=1;")
	tkRemote.MustExec("drop table if exists t_remote")
	tkRemote.MustExec("create table t_remote(id int)")
	tkRemote.MustQuery("select * from t_remote")

	remoteProcess := tkRemote.Session().ShowProcess()
	remotePlan, ok := remoteProcess.Plan.(base.Plan)
	require.True(t, ok)
	stmtCtx := tkRemote.Session().GetSessionVars().StmtCtx
	runtimeStatsColl := remoteProcess.RuntimeStatsColl
	require.NotNil(t, runtimeStatsColl)
	stmtCtx.RuntimeStatsColl = nil
	// The explicit collector must be used instead of the plan context's current statement context.
	regeneratedBriefBinaryPlan := core.GetBriefBinaryPlan(remotePlan, runtimeStatsColl)
	stmtCtx.RuntimeStatsColl = runtimeStatsColl
	regeneratedPlan, err := plancodec.ParseBinaryPlan(regeneratedBriefBinaryPlan)
	require.NoError(t, err)
	require.True(t, regeneratedPlan.WithRuntimeStats)
	remoteProcess.BriefBinaryPlan = regeneratedBriefBinaryPlan
	localSM := &testkit.MockSessionManager{PS: []*util.ProcessInfo{remoteProcess}}
	tkRemote.Session().SetSessionManager(localSM)

	// Simulate a remote node that only returns the minimal explain payload.
	remoteSM := &testkit.MockSessionManager{
		GetProcessInfoForExplainFn: func(_ context.Context, _ util.ExplainProcessInfoContext, id uint64) (*util.ProcessInfo, error) {
			if id != remoteProcess.ID {
				return nil, nil
			}
			return &util.ProcessInfo{
				ID:              remoteProcess.ID,
				User:            remoteProcess.User,
				Host:            remoteProcess.Host,
				BriefBinaryPlan: regeneratedBriefBinaryPlan,
			}, nil
		},
	}
	tkExplain.Session().SetSessionManager(remoteSM)

	// The remote brief binary plan should render the same verbose result as the target session locally.
	expectedRows := tkRemote.MustQuery(fmt.Sprintf("explain format = 'verbose' for connection %d", remoteProcess.ID)).Rows()
	actualRows := tkExplain.MustQuery(fmt.Sprintf("explain format = 'verbose' for connection %d", remoteProcess.ID)).Rows()
	requireExplainVerboseStableColumnsEqual(t, expectedRows, actualRows)
}

func requireExplainVerboseStableColumnsEqual(t *testing.T, expectedRows, actualRows [][]any) {
	t.Helper()
	// EXPLAIN FOR CONNECTION now prefers live runtime stats when available locally,
	// while remote explain-for-connection may only have the stored brief binary snapshot.
	// Compare all verbose columns except memory/disk, which can still differ between live and snapshot rendering.
	stableColumnIndexes := []int{0, 1, 2, 3, 4, 5, 6, 7}
	require.Len(t, actualRows, len(expectedRows))
	for i := range expectedRows {
		require.Len(t, actualRows[i], len(expectedRows[i]))
		for _, idx := range stableColumnIndexes {
			require.Equalf(t, expectedRows[i][idx], actualRows[i][idx], "row %d column %d", i, idx)
		}
	}
}

func TestExplainForRemoteProcessPrivilege(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tkRoot := testkit.NewTestKit(t, store)
	tkUser := testkit.NewTestKit(t, store)

	tkRoot.MustExec("use test")
	tkRoot.MustExec("create user tu_remote@'%'")
	tkRoot.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)
	tkUser.Session().Auth(&auth.UserIdentity{Username: "tu_remote", Hostname: "localhost", CurrentUser: true, AuthUsername: "tu_remote", AuthHostname: "%"}, nil, []byte("012345678901234567890"), nil)

	tkRoot.MustExec("set @@tidb_enable_collect_execution_info=1;")
	tkRoot.MustExec("drop table if exists t_remote_deny")
	tkRoot.MustExec("create table t_remote_deny(id int)")
	tkRoot.MustQuery("select * from t_remote_deny")

	remoteProcess := tkRoot.Session().ShowProcess()
	remotePlan, ok := remoteProcess.Plan.(base.Plan)
	require.True(t, ok)
	remoteBriefBinaryPlan := core.GetBriefBinaryPlan(remotePlan)
	require.NotEmpty(t, remoteBriefBinaryPlan)
	tkUser.Session().SetSessionManager(&testkit.MockSessionManager{
		// Simulate cross-node explain lookup for another user's connection.
		GetProcessInfoForExplainFn: func(_ context.Context, _ util.ExplainProcessInfoContext, id uint64) (*util.ProcessInfo, error) {
			if id != remoteProcess.ID {
				return nil, nil
			}
			return &util.ProcessInfo{
				ID:              remoteProcess.ID,
				User:            remoteProcess.User,
				Host:            remoteProcess.Host,
				BriefBinaryPlan: remoteBriefBinaryPlan,
			}, nil
		},
	})

	// Cross-user access should still require PROCESS or SUPER on the remote explain path.
	err := tkUser.ExecToErr(fmt.Sprintf("explain for connection %d", remoteProcess.ID))
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the SUPER or PROCESS privilege(s) for this operation")
	tkRoot.MustExec("grant process on *.* to tu_remote@'%'")
	tkUser.MustQuery(fmt.Sprintf("explain for connection %d", remoteProcess.ID))
}

func TestExplainForConnectionLocalTemporaryTableNoPanic(t *testing.T) {
	testCases := []struct {
		name         string
		createTable  string
		insertRows   string
		query        string
		expectedPlan string
	}{
		{
			name:         "TableReader",
			createTable:  "create temporary table tmp_t(a int primary key, b int)",
			insertRows:   "insert into tmp_t values (1, 1), (2, 2)",
			query:        "select * from tmp_t where b > 0",
			expectedPlan: "TableReader",
		},
		{
			name:         "IndexReader",
			createTable:  "create temporary table tmp_t(a int primary key, b int, key idx_b(b))",
			insertRows:   "insert into tmp_t values (1, 1), (2, 2)",
			query:        "select b from tmp_t use index(idx_b) where b > 0",
			expectedPlan: "IndexReader",
		},
		{
			name:         "IndexLookUpReader",
			createTable:  "create temporary table tmp_t(a int primary key, b int, c int, key idx_b(b))",
			insertRows:   "insert into tmp_t values (1, 1, 1), (2, 2, 2)",
			query:        "select c from tmp_t use index(idx_b) where b > 0",
			expectedPlan: "IndexLookUp",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk1 := testkit.NewTestKit(t, store)
			tk2 := testkit.NewTestKit(t, store)
			tk1.MustExec("use test")
			tk2.MustExec("use test")

			tk1.MustExec(testCase.createTable)
			tk1.MustExec(testCase.insertRows)
			plan := tk1.MustQuery("explain format = 'brief' " + testCase.query)
			require.Contains(t, fmt.Sprint(plan.Rows()), testCase.expectedPlan)
			tk1.MustQuery(testCase.query)
			tk1Process := tk1.Session().ShowProcess()
			tk2.Session().SetSessionManager(&testkit.MockSessionManager{PS: []*util.ProcessInfo{tk1Process}})

			err := tk2.ExecToErr(fmt.Sprintf("explain for connection %d", tk1Process.ID))
			require.Error(t, err)
			require.Contains(t, err.Error(), "is not found in current session infoschema")
		})
	}
}
