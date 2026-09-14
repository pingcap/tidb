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
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/hint"
)

func newImportQuerySchema(sctx sessionctx.Context, q *importer.QueryPlan, readTS uint64) (infoschema.InfoSchema, error) {
	dbs := make([]*model.DBInfo, len(q.Databases))
	for i, db := range q.Databases {
		// The builder mutates database metadata, so each attempt owns its copies.
		dbInfo := db.Clone()
		dbInfo.Deprecated.Tables = nil
		for _, tbl := range q.Tables[db.ID] {
			tableInfo := tbl.Clone()
			tableInfo.DBID = db.ID
			dbInfo.Deprecated.Tables = append(dbInfo.Deprecated.Tables, tableInfo)
		}
		dbs[i] = dbInfo
	}
	parent := sctx.GetInfoSchema().(infoschema.InfoSchema)
	b := infoschema.NewBuilder(parent.GetAutoIDRequirement(), 0, nil, infoschema.NewData(), false).
		WithCrossKS(true)
	// This schema contains the submitted table definitions, not a Domain cache version.
	if err := b.InitWithDBInfos(dbs, parent.AllPlacementPolicies(), parent.AllResourceGroups(), parent.AllMaskingPolicies(), 0); err != nil {
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
func (c *importQuerySession) GetInfoSchema() infoschemactx.MetaOnlyInfoSchema { return c.schema }
func (c *importQuerySession) GetPlanCtx() base.PlanContext                    { return c }

// newImportQuerySession prepares and wraps the caller-owned session for one query attempt.
func newImportQuerySession(
	ctx context.Context, sctx sessionctx.Context, q *importer.QueryPlan, memoryLimit int64,
) (*importQuerySession, ast.StmtNode, error) {
	if sctx.GetStore().GetKeyspace() != q.Keyspace {
		return nil, nil, errors.New("import query runtime keyspace mismatch")
	}
	vars := sctx.GetSessionVars()
	// Apply charset before collation; the charset setter also updates collation.
	varNames := slices.Sorted(maps.Keys(q.SessionVars))
	for _, name := range varNames {
		if err := vars.SetSystemVar(name, q.SessionVars[name]); err != nil {
			return nil, nil, err
		}
	}
	vars.MemQuotaQuery = memoryLimit
	vars.MemTracker.SetBytesLimit(memoryLimit)
	vars.CurrentDB = q.CurrentDB
	// Without a statistics provider, this worker uses static partition pruning.
	vars.PartitionPruneMode.Store("static")
	// Each attempt reads data at execution time using the submitted table definitions.
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
	hint.BindHint(node, &hint.HintsSet{})
	if err := ResetContextOfStmt(sctx, node); err != nil {
		return nil, nil, err
	}
	if err := sessiontxn.GetTxnManager(sctx).EnterNewTxn(ctx, &sessiontxn.EnterNewTxnRequest{Type: sessiontxn.EnterNewTxnBeforeStmt}); err != nil {
		return nil, nil, err
	}
	vars.SnapshotTS, vars.SnapshotInfoschema = readTS, is
	vars.StmtCtx.InitFromPBFlagAndTz(q.PushDownFlags, vars.Location())
	querySession := &importQuerySession{Context: sctx, schema: is}
	querySession.PlanCtxExtended = plannersession.NewPlanCtxExtended(querySession)
	return querySession, node, nil
}
