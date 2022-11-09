package domain_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestPlanReplayerHandleCollectTask(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	prHandle := dom.GetPlanReplayerHandle()

	// assert 1 task
	tk.MustExec("delete from mysql.plan_replayer_task")
	tk.MustExec("delete from mysql.plan_replayer_status")
	tk.MustExec("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('123','123');")
	err := prHandle.CollectPlanReplayerTask(context.Background())
	require.NoError(t, err)
	require.Len(t, prHandle.GetTasks(), 1)

	// assert no task
	tk.MustExec("delete from mysql.plan_replayer_task")
	tk.MustExec("delete from mysql.plan_replayer_status")
	err = prHandle.CollectPlanReplayerTask(context.Background())
	require.NoError(t, err)
	require.Len(t, prHandle.GetTasks(), 0)

	// assert 1 unhandled task
	tk.MustExec("delete from mysql.plan_replayer_task")
	tk.MustExec("delete from mysql.plan_replayer_status")
	tk.MustExec("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('123','123');")
	tk.MustExec("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('345','345');")
	tk.MustExec("insert into mysql.plan_replayer_status(sql_digest, plan_digest, token, instance) values ('123','123','123','123')")
	err = prHandle.CollectPlanReplayerTask(context.Background())
	require.NoError(t, err)
	require.Len(t, prHandle.GetTasks(), 1)

	// assert 2 unhandled task
	tk.MustExec("delete from mysql.plan_replayer_task")
	tk.MustExec("delete from mysql.plan_replayer_status")
	tk.MustExec("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('123','123');")
	tk.MustExec("insert into mysql.plan_replayer_task (sql_digest, plan_digest) values ('345','345');")
	tk.MustExec("insert into mysql.plan_replayer_status(sql_digest, plan_digest, fail_reason, instance) values ('123','123','123','123')")
	err = prHandle.CollectPlanReplayerTask(context.Background())
	require.NoError(t, err)
	require.Len(t, prHandle.GetTasks(), 2)
}
