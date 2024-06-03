// Copyright 2023 PingCAP, Inc.
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

package testutil

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

// CreateSubTask adds a new task to subtask table.
// used for testing.
func CreateSubTask(t *testing.T, gm *storage.TaskManager, taskID int64, step proto.Step, execID string, meta []byte, tp proto.TaskType, concurrency int) int64 {
	return InsertSubtask(t, gm, taskID, step, execID, meta, proto.SubtaskStatePending, tp, concurrency)
}

// InsertSubtask adds a new subtask of any state to subtask table.
func InsertSubtask(t *testing.T, gm *storage.TaskManager, taskID int64, step proto.Step, execID string, meta []byte, state proto.SubtaskState, tp proto.TaskType, concurrency int) int64 {
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "table_test")
	var id int64
	require.NoError(t, gm.WithNewSession(func(se sessionctx.Context) error {
		_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
			insert into mysql.tidb_background_subtask(`+storage.InsertSubtaskColumns+`) values`+
			`(%?, %?, %?, %?, %?, %?, %?, NULL, CURRENT_TIMESTAMP(), '{}', '{}')`,
			step, taskID, execID, meta, state, proto.Type2Int(tp), concurrency)
		if err != nil {
			return err
		}
		rs, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), "select @@last_insert_id")
		if err != nil {
			return err
		}
		id = rs[0].GetInt64(0)
		return nil
	}))
	return id
}
