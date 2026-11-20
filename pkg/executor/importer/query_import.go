// Copyright 2019 PingCAP, Inc.
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

package importer

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/expression"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// QueryImportRequest describes an IMPORT INTO ... FROM SELECT whose target and
// source tables are already known, so that it needs no statement planning.
type QueryImportRequest struct {
	// SQL is the IMPORT INTO statement. A worker compiles its SELECT later.
	SQL    string
	DBName string
	DBID   int64
	Table  table.Table
	// SourceTableIDs must include all table IDs involved in the query.
	SourceTableIDs []int64
}

// BuildQueryImportPlan builds and checks an import plan without planning its SELECT.
func BuildQueryImportPlan(ctx context.Context, sctx sessionctx.Context, req *QueryImportRequest) (*Plan, error) {
	stmt, err := ParseImportQuery(sctx, req.SQL)
	if err != nil {
		return nil, err
	}
	options, err := resolveStmtOptions(sctx, stmt.Options)
	if err != nil {
		return nil, err
	}
	plan, err := NewImportPlanFromInput(ctx, sctx, &Input{
		DBName:             req.DBName,
		DBID:               req.DBID,
		TableInfo:          req.Table.Meta(),
		Format:             stmt.Format,
		DataSourceType:     DataSourceTypeQuery,
		ColumnsAndUserVars: stmt.ColumnsAndUserVars,
		ColumnAssignments:  stmt.ColumnAssignments,
		Options:            options,
	})
	if err != nil {
		return nil, err
	}
	controller, err := NewLoadDataController(plan, req.Table, &ASTArgs{
		FileLocRef:         ast.FileLocServerOrRemote,
		ColumnsAndUserVars: stmt.ColumnsAndUserVars,
		ColumnAssignments:  stmt.ColumnAssignments,
		OnDuplicate:        ast.OnDuplicateKeyHandlingReplace,
	})
	if err != nil {
		return nil, err
	}
	if controller.IsLocalSort() {
		return nil, plannererrors.ErrNotSupportedYet.GenWithStackByArgs("IMPORT INTO FROM SELECT without global sort storage")
	}
	version, err := sctx.GetStore().CurrentVersion(tidbkv.GlobalTxnScope)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Capture the session before the checks below run statements in it.
	query, err := newQuerySourceAtSnapshot(ctx, sctx, version.Ver, req.DBID, req.SourceTableIDs)
	if err != nil {
		return nil, err
	}
	if err := controller.InitQuerySource(ctx, query); err != nil {
		return nil, err
	}
	if err := controller.CheckRequirements(ctx, sctx); err != nil {
		return nil, err
	}
	if err := controller.InitTiKVConfigs(ctx, sctx); err != nil {
		return nil, err
	}
	return plan, nil
}

// InitQuerySource sets the query source and initializes the node and thread
// limits for running it on a DXF worker.
func (e *LoadDataController) InitQuerySource(ctx context.Context, query *QuerySource) error {
	e.Plan.QuerySource = query
	// TODO: split I/O-bound single-table queries into range subtasks, and remove
	// this cap, which also keeps the later steps on one node.
	e.Plan.MaxNodeCnt = 1
	// The query takes all slots of a DXF node.
	cpuCnt, err := handle.GetCPUCountOfNode(ctx)
	if err != nil {
		return err
	}
	e.Plan.ThreadCnt = cpuCnt
	return nil
}

func resolveStmtOptions(sctx sessionctx.Context, stmtOptions []*ast.LoadDataOpt) ([]*ImportOption, error) {
	options := make([]*ImportOption, 0, len(stmtOptions))
	for _, opt := range stmtOptions {
		option := &ImportOption{Name: opt.Name}
		if opt.Value != nil {
			value, err := plannerutil.RewriteAstExprWithPlanCtx(sctx.GetPlanCtx(), opt.Value, nil, nil, false)
			if err != nil {
				return nil, err
			}
			constant, ok := value.(*expression.Constant)
			if !ok {
				return nil, exeerrors.ErrInvalidOptionVal.GenWithStackByArgs(opt.Name)
			}
			option.Value = constant
		}
		options = append(options, option)
	}
	return options, nil
}
