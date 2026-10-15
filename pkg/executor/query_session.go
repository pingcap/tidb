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
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
)

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

func newImportQuerySession(
	ctx context.Context, sctx sessionctx.Context,
	q *importer.QueryPlan, sql string,
) (*importQuerySession, ast.StmtNode, error) {
	if sctx.GetStore().GetKeyspace() != q.Keyspace {
		return nil, nil, errors.New("import query runtime keyspace mismatch")
	}
	vars := sctx.GetSessionVars()
	if err := initImportQuerySessionVars(q, vars); err != nil {
		return nil, nil, err
	}
	is, err := buildQueryInfoSchema(q, sctx.GetLatestInfoSchema().(infoschema.InfoSchema))
	if err != nil {
		return nil, nil, err
	}
	importStmt, node, err := parseImportQuery(sctx, sql)
	if err != nil {
		return nil, nil, err
	}
	// Reset with the IMPORT INTO statement, so the SELECT is evaluated under the
	// same statement flags as classic IMPORT INTO.
	if err := ResetContextOfStmt(sctx, importStmt); err != nil {
		return nil, nil, err
	}
	if err := sessiontxn.GetTxnManager(sctx).EnterNewTxn(
		ctx, &sessiontxn.EnterNewTxnRequest{Type: sessiontxn.EnterNewTxnBeforeStmt},
	); err != nil {
		return nil, nil, err
	}
	vars.SnapshotTS = q.ReadTS
	vars.SnapshotInfoschema = is
	querySession := &importQuerySession{Context: sctx, schema: is}
	querySession.PlanCtxExtended = plannersession.NewPlanCtxExtended(querySession)
	return querySession, node, nil
}

func initImportQuerySessionVars(q *importer.QueryPlan, vars *variable.SessionVars) error {
	for _, name := range slices.Sorted(maps.Keys(q.SessionVars)) {
		if err := vars.SetSystemVar(name, q.SessionVars[name]); err != nil {
			return err
		}
	}
	vars.CurrentDB = q.CurrentDB
	// Without a statistics provider, this worker uses static partition pruning.
	vars.PartitionPruneMode.Store("static")
	// The submitter runs in MV maintenance, which may read MVs whose initial
	// build is not ready yet.
	vars.InMViewMaintenance = true
	vars.InRestrictedSQL = true
	vars.InternalSQLScanUserTable = false
	vars.RequestSourceType = kv.InternalDistTask
	return nil
}

func buildQueryInfoSchema(q *importer.QueryPlan, parent infoschema.InfoSchema) (infoschema.InfoSchema, error) {
	dbs := make([]*model.DBInfo, 0, len(q.Databases))
	for _, db := range q.Databases {
		dbInfo := db.Clone()
		for _, tbl := range q.Tables[db.ID] {
			tableInfo := tbl.Clone()
			tableInfo.DBID = db.ID
			tableInfo.TableCacheStatusType = model.TableCacheStatusDisable
			dbInfo.Deprecated.Tables = append(dbInfo.Deprecated.Tables, tableInfo)
		}
		dbs = append(dbs, dbInfo)
	}
	b := infoschema.NewBuilder(parent.GetAutoIDRequirement(), 0, nil, infoschema.NewData(), false).
		WithCrossKS(true)
	if err := b.InitWithDBInfos(dbs, parent.AllPlacementPolicies(), nil, nil, q.SchemaVersion); err != nil {
		return nil, err
	}
	return b.Build(q.ReadTS), nil
}
