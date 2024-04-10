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

package querywatch

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/resourcegroup"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

// setWatchOption is used to set the QuarantineRecord with specific QueryWatchOption ast node.
func setWatchOption(ctx context.Context,
	sctx, newSctx sessionctx.Context,
	record *resourcegroup.QuarantineRecord,
	op *ast.QueryWatchOption,
) error {
	switch op.Tp {
	case ast.QueryWatchResourceGroup:
		if op.ExprValue != nil {
			expr, err := plannerutil.RewriteAstExprWithPlanCtx(sctx.GetPlanCtx(), op.ExprValue, nil, nil, false)
			if err != nil {
				return err
			}
			name, isNull, err := expr.EvalString(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
			if err != nil {
				return err
			}
			if isNull {
				return errors.Errorf("invalid resource group name expression")
			}
			record.ResourceGroupName = name
		} else {
			record.ResourceGroupName = op.StrValue.L
		}
	case ast.QueryWatchAction:
		record.Action = rmpb.RunawayAction(op.IntValue)
	case ast.QueryWatchType:
		expr, err := plannerutil.RewriteAstExprWithPlanCtx(sctx.GetPlanCtx(), op.ExprValue, nil, nil, false)
		if err != nil {
			return err
		}
		strval, isNull, err := expr.EvalString(sctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		if err != nil {
			return err
		}
		if isNull {
			return errors.Errorf("invalid watch text expression")
		}
		record.Watch = rmpb.RunawayWatchType(op.IntValue)
		if op.BoolValue {
			p := parser.New()
			stmts, _, err := p.ParseSQL(strval)
			if err != nil {
				return err
			}
			if len(stmts) != 1 {
				return errors.Errorf("only support one SQL")
			}
			sql := stmts[0].Text()
			switch model.RunawayWatchType(op.IntValue) {
			case model.WatchNone:
				return errors.Errorf("watch type must be specified")
			case model.WatchExact:
				record.WatchText = sql
			case model.WatchSimilar:
				_, digest := parser.NormalizeDigest(sql)
				record.WatchText = digest.String()
			case model.WatchPlan:
				sqlExecutor := newSctx.GetSQLExecutor()
				if _, err := sqlExecutor.ExecuteInternal(ctx, fmt.Sprintf("explain %s", stmts[0].Text())); err != nil {
					return err
				}
				_, digest := newSctx.GetSessionVars().StmtCtx.GetPlanDigest()
				if digest == nil {
					return errors.Errorf("no plan digest")
				}
				record.WatchText = digest.String()
			}
		} else {
			if len(strval) != 64 {
				return errors.Errorf("digest format error")
			}
			record.WatchText = strval
		}
	}
	return nil
}

// fromQueryWatchOptionList is used to create a QuarantineRecord with some QueryWatchOption ast nodes.
func fromQueryWatchOptionList(ctx context.Context, sctx, newSctx sessionctx.Context, optionList []*ast.QueryWatchOption) (*resourcegroup.QuarantineRecord, error) {
	record := &resourcegroup.QuarantineRecord{
		Source:    resourcegroup.ManualSource,
		StartTime: time.Now(),
		EndTime:   resourcegroup.NullTime,
	}
	for _, op := range optionList {
		if err := setWatchOption(ctx, sctx, newSctx, record, op); err != nil {
			return nil, err
		}
	}
	return record, nil
}

// validateWatchRecord follows several designs:
//  1. If no resource group is set, the default resource group is used
//  2. If no action is specified, the action of the resource group is used. If no, an error message is displayed.
func validateWatchRecord(record *resourcegroup.QuarantineRecord, client *rmclient.ResourceGroupsController) error {
	if len(record.ResourceGroupName) == 0 {
		record.ResourceGroupName = resourcegroup.DefaultResourceGroupName
	}
	rg, err := client.GetResourceGroup(record.ResourceGroupName)
	if err != nil {
		return err
	}
	if rg == nil {
		return infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(record.ResourceGroupName)
	}
	if record.Action == rmpb.RunawayAction_NoneAction {
		if rg.RunawaySettings == nil {
			return errors.Errorf("must set runaway config for resource group `%s`", record.ResourceGroupName)
		}
		record.Action = rg.RunawaySettings.Action
	}
	if record.Watch == rmpb.RunawayWatchType_NoneWatch {
		return errors.Errorf("must specify watch type")
	}
	return nil
}

// AddExecutor is used as executor of add query watch.
type AddExecutor struct {
	QueryWatchOptionList []*ast.QueryWatchOption
	exec.BaseExecutor
	done bool
}

// Next implements the interface of Executor.
func (e *AddExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	newSctx, err := e.GetSysSession()
	if err != nil {
		return err
	}
	record, err := fromQueryWatchOptionList(ctx, e.Ctx(), newSctx, e.QueryWatchOptionList)
	if err != nil {
		return err
	}
	do := domain.GetDomain(e.Ctx())
	if err := validateWatchRecord(record, do.ResourceGroupsController()); err != nil {
		return err
	}
	id, err := do.AddRunawayWatch(record)
	if err != nil {
		return err
	}
	req.AppendUint64(0, id)
	return nil
}

// ExecDropQueryWatch is use to exec DropQueryWatchStmt.
func ExecDropQueryWatch(sctx sessionctx.Context, id int64) error {
	do := domain.GetDomain(sctx)
	err := do.RemoveRunawayWatch(id)
	return err
}
