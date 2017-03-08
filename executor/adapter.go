// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"fmt"
	"math"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
)

type processinfoSetter interface {
	SetProcessInfo(string)
}

// recordSet wraps an executor, implements ast.RecordSet interface
type recordSet struct {
	fields      []*ast.ResultField
	executor    Executor
	stmt        *statement
	processinfo processinfoSetter
	err         error
}

func (a *recordSet) Fields() ([]*ast.ResultField, error) {
	if len(a.fields) == 0 {
		for _, col := range a.executor.Schema().Columns {
			rf := &ast.ResultField{
				ColumnAsName: col.ColName,
				TableAsName:  col.TblName,
				DBName:       col.DBName,
				Column: &model.ColumnInfo{
					FieldType: *col.RetType,
					Name:      col.ColName,
				},
			}
			a.fields = append(a.fields, rf)
		}
	}
	return a.fields, nil
}

func (a *recordSet) Next() (*ast.Row, error) {
	row, err := a.executor.Next()
	if err != nil || row == nil {
		return nil, errors.Trace(err)
	}
	return &ast.Row{Data: row.Data}, nil
}

func (a *recordSet) Close() error {
	err := a.executor.Close()
	a.stmt.logSlowQuery()
	if a.processinfo != nil {
		a.processinfo.SetProcessInfo("")
	}
	return errors.Trace(err)
}

// statement implements the ast.Statement interface, it builds a plan.Plan to an ast.Statement.
type statement struct {
	// The InfoSchema cannot change during execution, so we hold a reference to it.
	is        infoschema.InfoSchema
	ctx       context.Context
	text      string
	plan      plan.Plan
	startTime time.Time
}

func (a *statement) OriginText() string {
	return a.text
}

// Exec implements the ast.Statement Exec interface.
// This function builds an Executor from a plan. If the Executor doesn't return result,
// like the INSERT, UPDATE statements, it executes in this function, if the Executor returns
// result, execution is done after this function returns, in the returned ast.RecordSet Next method.
func (a *statement) Exec(ctx context.Context) (ast.RecordSet, error) {
	a.startTime = time.Now()
	a.ctx = ctx
	if _, ok := a.plan.(*plan.Execute); !ok {
		// Do not sync transaction for Execute statement, because the real optimization work is done in
		// "ExecuteExec.Build".
		var err error
		if IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, a.plan) {
			log.Debugf("[%d][InitTxnWithStartTS] %s", ctx.GetSessionVars().ConnectionID, a.text)
			err = ctx.InitTxnWithStartTS(math.MaxUint64)
		} else {
			log.Debugf("[%d][ActivePendingTxn] %s", ctx.GetSessionVars().ConnectionID, a.text)
			err = ctx.ActivePendingTxn()
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	b := newExecutorBuilder(ctx, a.is)
	e := b.build(a.plan)
	if b.err != nil {
		return nil, errors.Trace(b.err)
	}

	// ExecuteExec is not a real Executor, we only use it to build another Executor from a prepared statement.
	if executorExec, ok := e.(*ExecuteExec); ok {
		err := executorExec.Build()
		if err != nil {
			return nil, errors.Trace(err)
		}
		a.text = executorExec.Stmt.Text()
		a.plan = executorExec.Plan
		e = executorExec.StmtExec
	}

	var pi processinfoSetter
	if raw, ok := ctx.(processinfoSetter); ok {
		pi = raw
		// Update processinfo, ShowProcess() will use it.
		pi.SetProcessInfo(a.OriginText())
	}

	// Fields or Schema are only used for statements that return result set.
	if e.Schema().Len() == 0 {
		// Check if "tidb_snapshot" is set for the write executors.
		// In history read mode, we can not do write operations.
		switch e.(type) {
		case *DeleteExec, *InsertExec, *UpdateExec, *ReplaceExec, *LoadData, *DDLExec:
			snapshotTS := ctx.GetSessionVars().SnapshotTS
			if snapshotTS != 0 {
				return nil, errors.New("can not execute write statement when 'tidb_snapshot' is set")
			}
		}

		defer func() {
			if pi != nil {
				pi.SetProcessInfo("")
			}
			e.Close()
			a.logSlowQuery()
		}()
		for {
			row, err := e.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			// Even though there isn't any result set, the row is still used to indicate if there is
			// more work to do.
			// For example, the UPDATE statement updates a single row on a Next call, we keep calling Next until
			// There is no more rows to update.
			if row == nil {
				return nil, nil
			}
		}
	}

	return &recordSet{
		executor:    e,
		stmt:        a,
		processinfo: pi,
	}, nil
}

const (
	queryLogMaxLen = 2048
	slowThreshold  = 300 * time.Millisecond
)

func (a *statement) logSlowQuery() {
	costTime := time.Since(a.startTime)
	sql := a.text
	if len(sql) > queryLogMaxLen {
		sql = sql[:queryLogMaxLen] + fmt.Sprintf("(len:%d)", len(sql))
	}
	connID := a.ctx.GetSessionVars().ConnectionID
	if costTime < slowThreshold {
		log.Debugf("[%d][TIME_QUERY] %v %s", connID, costTime, sql)
	} else {
		log.Warnf("[%d][TIME_QUERY] %v %s", connID, costTime, sql)
	}
}

// IsPointGetWithPKOrUniqueKeyByAutoCommit returns true when meets following conditions:
//  1. ctx is auto commit tagged
//  2. txn is nil
//  2. plan is point get by pk or unique key
func IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx context.Context, p plan.Plan) bool {
	// check auto commit
	if !ctx.GetSessionVars().IsAutocommit() {
		return false
	}

	// check txn
	if ctx.Txn() != nil {
		return false
	}

	// check plan
	if proj, ok := p.(*plan.Projection); ok {
		if len(proj.Children()) != 1 {
			return false
		}
		p = proj.Children()[0]
	}

	// get by index key
	if indexScan, ok := p.(*plan.PhysicalIndexScan); ok {
		return indexScan.IsPointGetByUniqueKey(ctx.GetSessionVars().StmtCtx)
	}

	// get by primary key
	if tableScan, ok := p.(*plan.PhysicalTableScan); ok {
		return len(tableScan.Ranges) == 1 && tableScan.Ranges[0].IsPoint()
	}

	return false
}
