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
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemactx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/statistics"
	statsstorage "github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/util/hint"
)

func newImportQuerySchema(sctx sessionctx.Context, q *importer.QueryPlan, readTS uint64) (infoschema.InfoSchema, error) {
	dbs := make([]*model.DBInfo, len(q.Databases))
	for i, db := range q.Databases {
		// The builder mutates database metadata, so each attempt owns its copies.
		copy := db.Clone()
		copy.Deprecated.Tables = nil
		for _, tbl := range q.Tables[db.ID] {
			tableInfo := tbl.Clone()
			tableInfo.DBID = db.ID
			copy.Deprecated.Tables = append(copy.Deprecated.Tables, tableInfo)
		}
		dbs[i] = copy
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

// The SELECT uses a query-owned Domain and fully loaded tenant statistics.
// Storage reads for statistics still run as internal SQL on the underlying session.
type importQuerySession struct {
	sessionctx.Context
	schema infoschema.InfoSchema
	domain *domain.QueryDomain
}

func (c *importQuerySession) GetLatestInfoSchema() infoschemactx.MetaOnlyInfoSchema {
	return c.schema
}
func (c *importQuerySession) GetLatestISWithoutSessExt() infoschemactx.MetaOnlyInfoSchema {
	return c.schema
}
func (c *importQuerySession) GetInfoSchema() infoschemactx.MetaOnlyInfoSchema { return c.schema }
func (c *importQuerySession) GetDomain() any                                  { return c.domain.Domain }

func (s *importQuerySession) GetPlanCtx() base.PlanContext { return s }
func (s *importQuerySession) GetNullRejectCheckExprCtx() exprctx.ExprContext {
	return s.Context.GetPlanCtx().GetNullRejectCheckExprCtx()
}
func (s *importQuerySession) AdviseTxnWarmup() error { return s.Context.GetPlanCtx().AdviseTxnWarmup() }
func (s *importQuerySession) SetReadonlyUserVarMap(m map[string]struct{}) {
	s.Context.GetPlanCtx().SetReadonlyUserVarMap(m)
}
func (s *importQuerySession) GetReadonlyUserVarMap() map[string]struct{} {
	return s.Context.GetPlanCtx().GetReadonlyUserVarMap()
}
func (s *importQuerySession) Reset() { s.Context.GetPlanCtx().Reset() }

func loadImportQueryStats(ctx context.Context, sctx sessionctx.Context, tableInfos map[int64][]*model.TableInfo, budget int64) ([]*statistics.Table, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStatsForegroundPriority)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, "begin"); err != nil {
		return nil, err
	}
	defer sctx.RollbackTxn(context.Background())
	var result []*statistics.Table
	var bytes int64
	for dbID, tables := range tableInfos {
		for _, tbl := range tables {
			ids := []int64{tbl.ID}
			if part := tbl.GetPartitionInfo(); part != nil {
				for _, def := range part.Definitions {
					ids = append(ids, def.ID)
				}
			}
			for _, id := range ids {
				// The storage helper uses an internal context. Cancellation is
				// observed between loads, not while an individual load is running.
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				stats, err := statsstorage.TableStatsFromStorage(sctx, 0, tbl, id, true, 0, nil)
				if err != nil {
					return nil, errors.Annotatef(err, "load import query statistics for %s (database ID %d)", tbl.Name.O, dbID)
				}
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if stats == nil || !stats.IsAnalyzed() || !stats.IsInitialized() {
					return nil, errors.Errorf("import query requires analyzed statistics for %s (physical ID %d); run ANALYZE TABLE first", tbl.Name.O, id)
				}
				stats.CanNotTriggerLoad = true
				bytes += stats.MemoryUsage().TotalMemUsage
				if bytes > budget {
					return nil, errors.New("import query statistics exceed the memory budget")
				}
				result = append(result, stats)
			}
		}
	}
	return result, nil
}

// newImportQuerySession prepares and wraps the caller-owned session for one query attempt.
func newImportQuerySession(
	ctx context.Context, q *importer.QueryPlan, runtime *importer.QueryRuntime,
) (*importQuerySession, ast.StmtNode, error) {
	if q == nil || q.SQL == "" || runtime.Session == nil {
		return nil, nil, errors.New("invalid import query runtime")
	}
	sctx := runtime.Session
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
	totalLimit := runtime.TotalMemoryLimit
	if totalLimit <= 0 {
		totalLimit = runtime.MemoryLimit
	}
	if q.MemoryQuota > 0 {
		totalLimit = min(totalLimit, q.MemoryQuota)
	}
	if totalLimit <= 0 {
		return nil, nil, errors.New("import query requires a memory budget")
	}
	runtime.TotalMemoryLimit = totalLimit
	runtime.MemoryLimit = min(runtime.MemoryLimit, totalLimit/2)
	vars.MemQuotaQuery = totalLimit
	vars.MemTracker.SetBytesLimit(totalLimit)
	vars.CurrentDB = q.CurrentDB
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
	stats, err := loadImportQueryStats(ctx, sctx, q.Tables, totalLimit/4)
	if err != nil {
		return nil, nil, err
	}
	vars.InRestrictedSQL, vars.InternalSQLScanUserTable = false, false
	// Synchronous checks discard loaded items instead of enqueueing tenant IDs
	// in the process-global async loading queue.
	vars.StatsLoadSyncWait.Store(1)
	// Plans use a task-local schema rather than the process schema cache.
	vars.EnableNonPreparedPlanCache = false
	vars.RequestSourceType = kv.InternalDistTask
	node, err := parseImportQuery(sctx, q.SQL)
	if err != nil {
		return nil, nil, err
	}
	// Worker optimization does not inherit query or index hints from the submitter.
	hint.BindHint(node, &hint.HintsSet{})
	if err := ResetContextOfStmt(sctx, node); err != nil {
		return nil, nil, err
	}
	if err := sessiontxn.GetTxnManager(sctx).EnterNewTxn(ctx, &sessiontxn.EnterNewTxnRequest{Type: sessiontxn.EnterNewTxnBeforeStmt}); err != nil {
		return nil, nil, err
	}
	vars.SnapshotTS, vars.SnapshotInfoschema = readTS, is
	vars.StmtCtx.InitFromPBFlagAndTz(q.PushDownFlags, vars.Location())
	serverID, err := getMPPServerID(sctx)
	if err != nil {
		return nil, nil, err
	}
	queryDomain, err := domain.NewQueryDomain(ctx, sctx.GetStore(), is, stats, runtime.SessionPool, serverID)
	if err != nil {
		return nil, nil, err
	}
	return &importQuerySession{Context: sctx, schema: is, domain: queryDomain}, node, nil
}
