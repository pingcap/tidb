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
