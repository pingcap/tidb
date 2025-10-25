// Copyright 2025 PingCAP, Inc.
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

package planscache

import (
	"context"

	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/build"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
)

// PlanCacheStmt store prepared ast from PrepareExec and other related fields
type PlanCacheStmt struct {
	PreparedAst *ast.Prepared
	ResolveCtx  *resolve.Context
	StmtDB      string // which DB the statement will be processed over
	VisitInfos  []build.VisitInfo
	Params      []ast.ParamMarkerExpr

	PointGet PointGetExecutorCache

	// below fields are for PointGet short path
	SchemaVersion int64

	// RelateVersion stores the true cache plan table schema version, since each table schema can be updated separately in transaction.
	RelateVersion map[int64]uint64

	StmtCacheable     bool   // Whether this stmt is cacheable.
	UncacheableReason string // Why this stmt is uncacheable.

	// Limits is plan cache limit.
	Limits []*ast.Limit
	// HasSubquery indicate whether there is a subquery inside.
	HasSubquery bool
	// Tables is used to catch the table stats change.
	Tables []table.Table // to capture table stats changes

	NormalizedSQL       string
	NormalizedPlan      string
	SQLDigest           *parser.Digest
	PlanDigest          *parser.Digest
	ForUpdateRead       bool
	SnapshotTSEvaluator func(context.Context, sessionctx.Context) (uint64, error)

	BindingInfo bindinfo.BindingMatchInfo

	// the different between NormalizedSQL, NormalizedSQL4PC and StmtText:
	//  for the query `select * from t where a>1 and b<?`, then
	//  NormalizedSQL: select * from `t` where `a` > ? and `b` < ? --> constants are normalized to '?',
	//  NormalizedSQL4PC: select * from `test` . `t` where `a` > ? and `b` < ? --> schema name is added,
	//  StmtText: select * from t where a>1 and b <? --> just format the original query;
	StmtText string

	// DBName and Tbls are used to add metadata lock.
	DBName []ast.CIStr
	Tbls   []table.Table
}

// PointGetExecutorCache caches the PointGetExecutor to further improve its performance.
// Don't forget to reset this executor when the prior plan is invalid.
type PointGetExecutorCache struct {
	ColumnInfos any
	// Executor is only used for point get scene.
	// Notice that we should only cache the PointGetExecutor that have a snapshot with MaxTS in it.
	// If the current plan is not PointGet or does not use MaxTS optimization, this value should be nil here.
	Executor any

	// FastPlan is only used for instance plan cache.
	// To ensure thread-safe, we have to clone each plan before reusing if using instance plan cache.
	// To reduce the memory allocation and increase performance, we cache the FastPlan here.
	FastPlan *physicalop.PointGetPlan
}
