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

package executor

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemactx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/plannersession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/hint"
)

func newImportQuerySchema(
	sctx sessionctx.Context,
	q *importer.QueryPlan,
	readTS uint64,
) (infoschema.InfoSchema, error) {
	dbs := make([]*model.DBInfo, 0, len(q.Databases))
	for _, db := range q.Databases {
		dbInfo := db.Clone()
		for _, tbl := range q.Tables[db.ID] {
			tableInfo := tbl.Clone()
			tableInfo.DBID = db.ID
			dbInfo.Deprecated.Tables = append(dbInfo.Deprecated.Tables, tableInfo)
		}
		dbs = append(dbs, dbInfo)
	}

	parent := sctx.GetLatestInfoSchema().(infoschema.InfoSchema)
	b := infoschema.NewBuilder(parent.GetAutoIDRequirement(), 0, nil, infoschema.NewData(), false).
		WithCrossKS(true)
	if err := b.InitWithDBInfos(dbs, parent.AllPlacementPolicies(), nil, nil, 0); err != nil {
		return nil, err
	}
	return b.Build(readTS), nil
}

// The planner uses the submitted source schema for this query attempt.
type importQuerySession struct {
	sessionctx.Context
	*plannersession.PlanCtxExtended
	schema infoschema.InfoSchema
}

func (c *importQuerySession) GetLatestInfoSchema() infoschemactx.MetaOnlyInfoSchema {
	return c.schema
}

func (c *importQuerySession) GetLatestISWithoutSessExt() infoschemactx.MetaOnlyInfoSchema {
	return c.schema
}

func (c *importQuerySession) GetInfoSchema() infoschemactx.MetaOnlyInfoSchema {
	return c.schema
}

func (c *importQuerySession) GetPlanCtx() base.PlanContext { return c }

// newImportQuerySession prepares and wraps the caller-owned session for one query attempt.
func newImportQuerySession(
	ctx context.Context, sctx sessionctx.Context,
	q *importer.QueryPlan, memoryLimit int64,
) (*importQuerySession, ast.StmtNode, error) {
	if sctx.GetStore().GetKeyspace() != q.Keyspace {
		return nil, nil, errors.New("import query runtime keyspace mismatch")
	}
	vars := sctx.GetSessionVars()
	varNames := slices.Sorted(maps.Keys(q.SessionVars))
	for _, name := range varNames {
		if err := vars.SetSystemVar(name, q.SessionVars[name]); err != nil {
			return nil, nil, err
		}
	}
	vars.MemQuotaQuery = memoryLimit
	vars.MemTracker.SetBytesLimit(memoryLimit)
	vars.CurrentDB = q.CurrentDB
	vars.PartitionPruneMode.Store("static")
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	ver, err := sctx.GetStore().CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return nil, nil, err
	}
	readTS := ver.Ver
	is, err := newImportQuerySchema(sctx, q, readTS)
	if err != nil {
		return nil, nil, err
	}
	vars.InRestrictedSQL, vars.InternalSQLScanUserTable = true, false
	vars.RequestSourceType = kv.InternalDistTask
	node, err := parseImportQuery(sctx, q.SQL)
	if err != nil {
		return nil, nil, err
	}
	ast.Walk(node, importQueryMemoryHint(memoryLimit))
	if err := ResetContextOfStmt(sctx, node); err != nil {
		return nil, nil, err
	}
	if err := sessiontxn.GetTxnManager(sctx).EnterNewTxn(
		ctx, &sessiontxn.EnterNewTxnRequest{Type: sessiontxn.EnterNewTxnBeforeStmt},
	); err != nil {
		return nil, nil, err
	}
	vars.SnapshotTS = readTS
	vars.SnapshotInfoschema = is
	vars.StmtCtx.InitFromPBFlagAndTz(q.PushDownFlags, vars.Location())
	querySession := &importQuerySession{Context: sctx, schema: is}
	querySession.PlanCtxExtended = plannersession.NewPlanCtxExtended(querySession)
	return querySession, node, nil
}

// importQueryMemoryHint keeps the worker resource budget while preserving query hints.
type importQueryMemoryHint int64

func (limit importQueryMemoryHint) Enter(node ast.Node) bool {
	if h, ok := node.(*ast.TableOptimizerHint); ok {
		switch h.HintName.L {
		case hint.HintMemoryQuota:
			h.HintData = int64(limit)
		case "set_var":
			setting := h.HintData.(ast.HintSetVar)
			if strings.EqualFold(setting.VarName, vardef.TiDBMemQuotaQuery) {
				setting.Value = strconv.FormatInt(int64(limit), 10)
				h.HintData = setting
			}
		}
	}
	return false
}

func (importQueryMemoryHint) Leave(ast.Node) bool { return true }
