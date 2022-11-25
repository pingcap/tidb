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

package runcontrol

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	mysql "github.com/pingcap/tidb/errno"
	res "github.com/pingcap/tidb/resourcemanager"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/dbterror"
)

const (
	// BackTaskOpsTable represent backend tasks operation table.
	BackTaskOpsTable = "tidb_backend_task_operation"
	// BackTaskOpsHistoryTable represent backend tasks operation history table.
	BackTaskOpsHistoryTable = "tidb_backend_task_operation_history"
)

var (
	errUnsupportedOpType = dbterror.ClassResource.NewStd(mysql.ErrUnsupportedBackendOpType)
	errDupPauseOp        = dbterror.ClassResource.NewStd(mysql.ErrDuplicatePauseOperation)
	errOnExecution       = dbterror.ClassResource.NewStd(mysql.ErrOnExecOperation)
	errNoPause           = dbterror.ClassResource.NewStd(mysql.ErrNoCounterPauseForResume)
)

// PauseBackendTask function to finished first stage of pause DDL tasks function.
func PauseBackendTask(sess sessionctx.Context, taskType string, isForce bool, stmtSQL string) error {
	// It inserts one row that means DDL tasks pause function into correlated
	// meta table, then all TiDB node will do second stage of pause DDL tasks
	return res.RunInTxn(res.NewSession(sess), func(se *res.Session) error {
		sqlStmt, err := genPauseTaskStatement(taskType, isForce)
		if err != nil {
			err = errUnsupportedOpType.GenWithStack("%s, %s is not supported operation type", stmtSQL, taskType)
			se.GetSessionVars().StmtCtx.AppendWarning(err)
			return err
		}
		_, err = se.Execute(context.Background(), sqlStmt, "pause_operation")
		failpoint.Inject("executionPauseOnError", func(val failpoint.Value) {
			if val.(bool) {
				err := errOnExecution.GenWithStack("%s, an error occurs, %s, during execution", stmtSQL, "Mock Pause error.")
				failpoint.Return(err)
			}
		})
		if err != nil {
			err = errOnExecution.GenWithStack("%s, %s, run Pause statement error", err.Error(), stmtSQL)
			se.GetSessionVars().StmtCtx.AppendWarning(err)
		}
		return err
	})
}

// ResumeBackendTask will resume Backend tasks execution according to before pause tasks Type.
func ResumeBackendTask(sess sessionctx.Context, taskType string, stmtSQL string) error {
	return res.RunInTxn(res.NewSession(sess), func(se *res.Session) error {
		// firstly, it will check if there is a counterpart pause row in meta table;
		sqlStmt, err := genGetPauseTaskStatement(taskType)
		if err != nil {
			err = errUnsupportedOpType.GenWithStack("%s, %s is not supported PAUSE type", stmtSQL, taskType)
			se.GetSessionVars().StmtCtx.AppendWarning(err)
			return err
		}
		rows, err := se.Execute(context.Background(), sqlStmt, "get_pause_operation")
		if err != nil {
			err = errOnExecution.GenWithStack("%s, %s, run get PAUSE command error", err.Error(), stmtSQL)
			se.GetSessionVars().StmtCtx.AppendWarning(err)
			return err
		}
		if len(rows) == 0 {
			err = errNoPause.GenWithStack("'%s', no PAUSE statement issued before.", stmtSQL)
			sess.GetSessionVars().StmtCtx.AppendWarning(err)
			return err
		}
		failpoint.Inject("executionGetPauseOnError", func(val failpoint.Value) {
			if val.(bool) {
				err := errOnExecution.GenWithStack("%s, an error occurs, %s, during execution", stmtSQL, "Mock Get Pause error.")
				failpoint.Return(err)
			}
		})
		// Secondly, it will move the pause row into history table.
		sqlStmts, err := genResumeTaskStatement(taskType)
		if err != nil {
			err = errUnsupportedOpType.GenWithStack("%s, %s is not supported RESUME type", stmtSQL, taskType)
			se.GetSessionVars().StmtCtx.AppendWarning(err)
			return err
		}
		// Move the pause operation record to tidb_backend_tasks_schedule_history table.
		for _, query := range sqlStmts {
			_, err = se.Execute(context.Background(), query, "Resume_operation")
			if err != nil {
				err = errOnExecution.GenWithStack("%s, %s, run Pause statement error", err.Error(), stmtSQL)
				se.GetSessionVars().StmtCtx.AppendWarning(err)
			}
		}
		failpoint.Inject("executionResumeOnError", func(val failpoint.Value) {
			if val.(bool) {
				err := errOnExecution.GenWithStack("%s, an error occurs, %s, during execution", stmtSQL, "Mock Resume error.")
				failpoint.Return(err)
			}
		})
		return err
	})
}

func genPauseTaskStatement(taskType string, isForce bool) (string, error) {
	var sql string
	if !isSupportedTaskType(taskType) {
		return "", errors.New(taskType + " is not a supported type for pause backend task")
	}
	if isForce {
		sql = "insert into mysql." + BackTaskOpsTable + "(is_force, op_type, start_time) values('1', "
	} else {
		sql = "insert into mysql." + BackTaskOpsTable + "(is_force, op_type, start_time) values('0', "
	}
	sql += fmt.Sprintf("'%s', current_timestamp);", taskType)
	return sql, nil
}

func genGetPauseTaskStatement(taskType string) (string, error) {
	var sql string
	if !isSupportedTaskType(taskType) {
		return "", errors.New(taskType + " is not a supported type for pause backend task")
	}
	sql += fmt.Sprintf("select * from mysql."+BackTaskOpsTable+" where op_type ='%s'", taskType)
	return sql, nil
}

func genResumeTaskStatement(taskType string) ([]string, error) {
	var (
		sqlInsert   string
		sqlDelete   string
		whereClause string
		sqls        []string
	)
	if !isSupportedTaskType(taskType) {
		return nil, errors.New(taskType + " is not a supported type for Resume backend task")
	}
	whereClause = " where op_type ='" + taskType + "'; "
	sqlInsert = "insert into mysql." + BackTaskOpsHistoryTable + " select op_id, is_force, op_type, op_meta, start_time, current_timestamp from "
	sqlInsert += " mysql." + BackTaskOpsTable + whereClause
	sqlDelete += "delete from mysql." + BackTaskOpsTable + whereClause
	sqls = append(sqls, sqlInsert, sqlDelete)
	return sqls, nil
}

func isSupportedTaskType(taskType string) bool {
	switch taskType {
	case "DDL":
		return true
	default:
		return false
	}
}
