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
	"cmp"
	"context"
	"maps"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
)

// newImportQuerySession prepares sctx to run the SELECT of an import query at
// q.ReadTS. sctx gets an InfoCache that holds only the submitted source
// schema, so it must not go back to its pool afterwards.
func newImportQuerySession(
	ctx context.Context, sctx sessionctx.Context,
	q *importer.QueryPlan, sql string,
) (infoschema.InfoSchema, ast.StmtNode, error) {
	if sctx.GetStore().GetKeyspace() != q.Keyspace {
		return nil, nil, errors.New("import query runtime keyspace mismatch")
	}
	setter, ok := sctx.(interface{ SetInfoCache(*infoschema.InfoCache) })
	if !ok {
		return nil, nil, errors.Errorf("import query cannot set the InfoCache of %T", sctx)
	}
	vars := sctx.GetSessionVars()
	if err := initImportQuerySessionVars(q, vars); err != nil {
		return nil, nil, err
	}
	is, err := buildQueryInfoSchema(q, sctx.GetLatestInfoSchema().(infoschema.InfoSchema))
	if err != nil {
		return nil, nil, err
	}
	cache := infoschema.NewCache(sctx.GetStore(), 1)
	cache.Insert(is, q.ReadTS)
	setter.SetInfoCache(cache)
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
	return is, node, nil
}

func initImportQuerySessionVars(q *importer.QueryPlan, vars *variable.SessionVars) error {
	names := slices.Sorted(maps.Keys(q.SessionVars))
	// Depended variables, such as tidb_enable_noop_functions, go first. The rest
	// stay sorted, so a charset is set before its collation, which it resets.
	slices.SortStableFunc(names, func(a, b string) int {
		return cmp.Compare(importQueryVarRank(a), importQueryVarRank(b))
	})
	for _, name := range names {
		if err := vars.SetSystemVar(name, q.SessionVars[name]); err != nil {
			return err
		}
	}
	vars.SQLMode = q.SQLMode
	vars.CurrentDB = q.CurrentDB
	if q.ResourceGroupName != "" {
		vars.SetResourceGroupName(q.ResourceGroupName)
	}
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

func importQueryVarRank(name string) int {
	if sv := variable.GetSysVar(name); sv != nil && sv.Depended {
		return 0
	}
	return 1
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
