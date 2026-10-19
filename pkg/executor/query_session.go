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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
)

// newImportQuerySession prepares sctx to run the SELECT of an import query at
// q.ReadTS. sctx gets an InfoCache that holds only the source tables at
// q.ReadTS, so it must not go back to its pool afterwards.
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
	if err := initImportQuerySessionVars(ctx, sctx, q); err != nil {
		return nil, nil, err
	}
	is, err := buildQueryInfoSchema(sctx.GetStore(), q, sctx.GetLatestInfoSchema().(infoschema.InfoSchema))
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

// importQueryWorkerVars are decided by the worker instead of the submitter:
// the snapshot and transaction it reads in, and its memory budget.
var importQueryWorkerVars = map[string]struct{}{
	vardef.TiDBSnapshot:             {},
	vardef.TiDBReadStaleness:        {},
	vardef.TiDBTxnReadTS:            {},
	vardef.TiDBEnableExternalTSRead: {},
	vardef.AutoCommit:               {},
	vardef.TiDBTxnMode:              {},
	vardef.TxnIsolation:             {},
	vardef.TransactionIsolation:     {},
	vardef.TxReadOnly:               {},
	vardef.TransactionReadOnly:      {},
	vardef.TiDBMemQuotaQuery:        {},
}

func initImportQuerySessionVars(ctx context.Context, sctx sessionctx.Context, q *importer.QueryPlan) error {
	vars := sctx.GetSessionVars()
	states := *q.SessionStates
	states.SystemVars = make(map[string]string, len(q.SessionStates.SystemVars))
	for name, value := range q.SessionStates.SystemVars {
		if _, ok := importQueryWorkerVars[name]; ok {
			continue
		}
		// Internal session variables cannot be set in a session.
		if sv := variable.GetSysVar(name); sv != nil && sv.InternalSessionVariable {
			continue
		}
		states.SystemVars[name] = value
	}
	// The status tells whether the submitter is in a transaction, which the
	// worker's session is not.
	states.Status = uint32(vars.Status())
	if err := sctx.DecodeStates(ctx, &states); err != nil {
		return err
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

func buildQueryInfoSchema(
	store kv.Storage, q *importer.QueryPlan, parent infoschema.InfoSchema,
) (infoschema.InfoSchema, error) {
	m := meta.NewReader(store.GetSnapshot(kv.NewVersion(q.ReadTS)))
	dbByID := make(map[int64]*model.DBInfo)
	dbs := make([]*model.DBInfo, 0, 1)
	for _, src := range q.Tables {
		db := dbByID[src.DBID]
		if db == nil {
			var err error
			if db, err = m.GetDatabase(src.DBID); err != nil {
				return nil, err
			}
			if db == nil {
				return nil, errors.Errorf("source database %d not found at the import query snapshot", src.DBID)
			}
			dbByID[src.DBID] = db
			dbs = append(dbs, db)
		}
		tbl, err := m.GetTable(src.DBID, src.ID)
		if err != nil {
			return nil, err
		}
		// The submitter resolved the query with this revision.
		if tbl == nil || tbl.Revision != src.Revision {
			return nil, errors.Errorf("source table %d changed before the import query snapshot", src.ID)
		}
		infoschema.ConvertOldVersionUTF8ToUTF8MB4IfNeed(tbl)
		infoschema.ConvertCharsetCollateToLowerCaseIfNeed(tbl)
		tbl.DBID = db.ID
		tbl.TableCacheStatusType = model.TableCacheStatusDisable
		db.Deprecated.Tables = append(db.Deprecated.Tables, tbl)
	}
	b := infoschema.NewBuilder(parent.GetAutoIDRequirement(), 0, nil, infoschema.NewData(), false).
		WithCrossKS(true)
	if err := b.InitWithDBInfos(dbs, parent.AllPlacementPolicies(), nil, nil, q.SchemaVersion); err != nil {
		return nil, err
	}
	return b.Build(q.ReadTS), nil
}
