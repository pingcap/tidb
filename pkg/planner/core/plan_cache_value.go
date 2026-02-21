// Copyright 2024 PingCAP, Inc.
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

package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/zeropool"
)

// PlanCacheValue stores the cached Statement and StmtNode.
type PlanCacheValue struct {
	// Meta Info, all are READ-ONLY once initialized.
	SQLDigest        string
	SQLText          string
	StmtType         string             // select, update, insert, delete, etc.
	ParseUser        string             // the user who parses/compiles this plan.
	Binding          string             // the binding of this plan.
	OptimizerEnvHash string             // other environment information that might affect the plan like "time_zone", "sql_mode".
	ParseValues      string             // the actual values used when parsing/compiling this plan.
	PlanDigest       string             // digest of the plan, used to identify the plan in the cache.
	BinaryPlan       string             // binary of this Plan, use tidb_decode_binary_plan to decode this.
	Memory           int64              // the memory usage of this plan, in bytes.
	LoadTime         time.Time          // the time when this plan is loaded into the cache.
	Plan             base.Plan          // READ-ONLY for Instance Cache, READ-WRITE for Session Cache.
	OutputColumns    types.NameSlice    // output column names of this plan
	ParamTypes       []*types.FieldType // all parameters' types, different parameters may share same plan
	StmtHints        *hint.StmtHints    // related hints of this plan, like 'max_execution_time'.

	// Runtime Info, all are READ-WRITE, use UpdateRuntimeInfo() and RuntimeInfo() to access them.
	executions         int64 // the execution times.
	processedKeys      int64 // the total number of processed keys in TiKV.
	totalKeys          int64 // the total number of returned keys in TiKV.
	sumLatency         int64 // the total latency of this plan, in nanoseconds.
	lastUsedTimeInUnix int64 // the last time when this plan is used, in Unix timestamp.

	testKey int64 // test-only
}

// unKnownMemoryUsage represent the memory usage of uncounted structure, maybe need implement later
// 100 KiB is approximate consumption of a plan from our internal tests
const unKnownMemoryUsage = int64(50 * size.KB)

// UpdateRuntimeInfo accumulates the runtime information of the plan.
func (v *PlanCacheValue) UpdateRuntimeInfo(proKeys, totKeys, latency int64) {
	atomic.AddInt64(&v.executions, 1)
	atomic.AddInt64(&v.processedKeys, proKeys)
	atomic.AddInt64(&v.totalKeys, totKeys)
	atomic.AddInt64(&v.sumLatency, latency)
	atomic.StoreInt64(&v.lastUsedTimeInUnix, time.Now().Unix())
}

// RuntimeInfo returns the runtime information of the plan.
func (v *PlanCacheValue) RuntimeInfo() (exec, procKeys, totKeys, sumLat int64, lastUsedTime time.Time) {
	exec = atomic.LoadInt64(&v.executions)
	procKeys = atomic.LoadInt64(&v.processedKeys)
	totKeys = atomic.LoadInt64(&v.totalKeys)
	sumLat = atomic.LoadInt64(&v.sumLatency)
	lastUsedTime = time.Unix(atomic.LoadInt64(&v.lastUsedTimeInUnix), 0)
	return
}

// MemoryUsage return the memory usage of PlanCacheValue
func (v *PlanCacheValue) MemoryUsage() (sum int64) {
	if v == nil {
		return
	}

	if v.Memory > 0 {
		return v.Memory
	}
	switch x := v.Plan.(type) {
	case base.PhysicalPlan:
		sum = x.MemoryUsage()
	case *physicalop.Insert:
		sum = x.MemoryUsage()
	case *physicalop.Update:
		sum = x.MemoryUsage()
	case *physicalop.Delete:
		sum = x.MemoryUsage()
	default:
		sum = unKnownMemoryUsage
	}

	sum += size.SizeOfInterface + size.SizeOfSlice*2 + int64(cap(v.OutputColumns))*size.SizeOfPointer +
		size.SizeOfMap + size.SizeOfInt64*2
	if v.ParamTypes != nil {
		sum += int64(cap(v.ParamTypes)) * size.SizeOfPointer
		for _, ft := range v.ParamTypes {
			sum += ft.MemoryUsage()
		}
	}

	for _, name := range v.OutputColumns {
		sum += name.MemoryUsage()
	}
	sum += int64(len(v.SQLDigest)) + int64(len(v.SQLText)) + int64(len(v.StmtType)) + int64(len(v.BinaryPlan)) +
		int64(len(v.ParseUser)) + int64(len(v.Binding)) + int64(len(v.OptimizerEnvHash)) + int64(len(v.ParseValues))

	// Runtime Info Size
	sum += size.SizeOfFloat64 * 7

	v.Memory = sum
	return
}

var planCacheHasherPool = sync.Pool{
	New: func() any {
		return sha256.New()
	},
}

// dirtyTableIDsPool is a pool for int64 slices used in NewPlanCacheKey.
var dirtyTableIDsPool = zeropool.New[[]int64](func() []int64 {
	return make([]int64, 0, 8)
})

// NewPlanCacheValue creates a SQLCacheValue.
func NewPlanCacheValue(
	sctx sessionctx.Context,
	stmt *PlanCacheStmt,
	cacheKey string,
	binding string,
	plan base.Plan, // the cached plan,
	names []*types.FieldName, // output column names of this plan,
	paramTypes []*types.FieldType, // corresponding parameter types of this plan,
	stmtHints *hint.StmtHints, // corresponding hints of this plan,
) *PlanCacheValue {
	userParamTypes := make([]*types.FieldType, len(paramTypes))
	for i, tp := range paramTypes {
		userParamTypes[i] = tp.Clone()
	}
	var userName string
	if sctx.GetSessionVars().User != nil { // might be nil if in test
		userName = sctx.GetSessionVars().User.AuthUsername
	}

	flat := FlattenPhysicalPlan(plan, false)
	binaryPlan := BinaryPlanStrFromFlatPlan(sctx.GetPlanCtx(), flat, false)

	// calculate opt env hash using cacheKey and paramTypes
	// (cacheKey, paramTypes) contains all factors that can affect the plan
	// use the same hash algo with SQLDigest: sha256 + hex
	hasher := planCacheHasherPool.Get().(hash.Hash)
	hasher.Write(hack.Slice(cacheKey))
	for _, tp := range paramTypes {
		hasher.Write(hack.Slice(tp.String()))
	}
	optEnvHash := hex.EncodeToString(hasher.Sum(nil))
	hasher.Reset()
	planCacheHasherPool.Put(hasher)

	pcv := &PlanCacheValue{
		SQLDigest:        stmt.SQLDigest.String(),
		SQLText:          stmt.StmtText,
		StmtType:         stmt.PreparedAst.StmtType,
		ParseUser:        userName,
		Binding:          binding,
		OptimizerEnvHash: optEnvHash,
		ParseValues:      types.DatumsToStrNoErr(sctx.GetSessionVars().PlanCacheParams.AllParamValues()),
		PlanDigest:       stmt.PlanDigest.String(),
		BinaryPlan:       binaryPlan,

		LoadTime:      time.Now(),
		Plan:          plan,
		OutputColumns: names,
		ParamTypes:    userParamTypes,
		StmtHints:     stmtHints.Clone(),
	}
	pcv.MemoryUsage() // initialize the memory usage field
	return pcv
}

// planCacheStmtProcessor records all query features which may affect plan selection.
type planCacheStmtProcessor struct {
	ctx  context.Context
	is   infoschema.InfoSchema
	stmt *PlanCacheStmt
}

// Enter implements Visitor interface.
func (f *planCacheStmtProcessor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.Limit:
		f.stmt.limits = append(f.stmt.limits, node)
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
		f.stmt.hasSubquery = true
	case *ast.TableName:
		t, err := f.is.TableByName(f.ctx, node.Schema, node.Name)
		if err == nil {
			f.stmt.tables = append(f.stmt.tables, t)
		}
	}
	return in, false
}

// Leave implements Visitor interface.
func (*planCacheStmtProcessor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
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

// PlanCacheStmt store prepared ast from PrepareExec and other related fields
type PlanCacheStmt struct {
	PreparedAst *ast.Prepared
	ResolveCtx  *resolve.Context
	StmtDB      string // which DB the statement will be processed over
	VisitInfos  []visitInfo
	Params      []ast.ParamMarkerExpr

	PointGet PointGetExecutorCache

	// below fields are for PointGet short path
	SchemaVersion int64

	// RelateVersion stores the true cache plan table schema version, since each table schema can be updated separately in transaction.
	RelateVersion map[int64]uint64

	StmtCacheable     bool   // Whether this stmt is cacheable.
	UncacheableReason string // Why this stmt is uncacheable.

	limits      []*ast.Limit
	hasSubquery bool
	tables      []table.Table // to capture table stats changes

	NormalizedSQL       string
	NormalizedPlan      string
	SQLDigest           *parser.Digest
	PlanDigest          *parser.Digest
	ForUpdateRead       bool
	SnapshotTSEvaluator func(context.Context, sessionctx.Context) (uint64, error)

	// BindingInfo caches normalization results for binding matching across executions.
	BindingInfo bindinfo.BindingMatchInfo

	// the different between NormalizedSQL, NormalizedSQL4PC and StmtText:
	//  for the query `select * from t where a>1 and b<?`, then
	//  NormalizedSQL: select * from `t` where `a` > ? and `b` < ? --> constants are normalized to '?',
	//  NormalizedSQL4PC: select * from `test` . `t` where `a` > ? and `b` < ? --> schema name is added,
	//  StmtText: select * from t where a>1 and b <? --> just format the original query;
	StmtText string

	// dbName and tbls are used to add metadata lock.
	dbName []ast.CIStr
	tbls   []table.Table
}

// GetPreparedStmt extract the prepared statement from the execute statement.
func GetPreparedStmt(stmt *ast.ExecuteStmt, vars *variable.SessionVars) (*PlanCacheStmt, error) {
	if stmt.PrepStmt != nil {
		return stmt.PrepStmt.(*PlanCacheStmt), nil
	}
	if stmt.Name != "" {
		prepStmt, err := vars.GetPreparedStmtByName(stmt.Name)
		if err != nil {
			return nil, err
		}
		stmt.PrepStmt = prepStmt
		return prepStmt.(*PlanCacheStmt), nil
	}
	return nil, plannererrors.ErrStmtNotFound
}

// CheckTypesCompatibility4PC compares FieldSlice with []*types.FieldType
// Currently this is only used in plan cache to check whether the types of parameters are compatible.
// If the types of parameters are compatible, we can use the cached plan.
// tpsExpected is types from cached plan
func checkTypesCompatibility4PC(expected, actual any) bool {
	if expected == nil || actual == nil {
		return true // no need to compare types
	}
	tpsExpected := expected.([]*types.FieldType)
	tpsActual := actual.([]*types.FieldType)
	if len(tpsExpected) != len(tpsActual) {
		return false
	}
	for i := range tpsActual {
		// We only use part of logic of `func (ft *FieldType) Equal(other *FieldType)` here because (1) only numeric and
		// string types will show up here, and (2) we don't need flen and decimal to be matched exactly to use plan cache
		tpEqual := (tpsExpected[i].GetType() == tpsActual[i].GetType()) ||
			(tpsExpected[i].GetType() == mysql.TypeVarchar && tpsActual[i].GetType() == mysql.TypeVarString) ||
			(tpsExpected[i].GetType() == mysql.TypeVarString && tpsActual[i].GetType() == mysql.TypeVarchar)
		if !tpEqual || tpsExpected[i].GetCharset() != tpsActual[i].GetCharset() || tpsExpected[i].GetCollate() != tpsActual[i].GetCollate() ||
			(tpsExpected[i].EvalType() == types.ETInt && mysql.HasUnsignedFlag(tpsExpected[i].GetFlag()) != mysql.HasUnsignedFlag(tpsActual[i].GetFlag())) {
			return false
		}
		// When the type is decimal, we should compare the Flen and Decimal.
		// We can only use the plan when both Flen and Decimal should less equal than the cached one.
		// We assume here that there is no correctness problem when the precision of the parameters is less than the precision of the parameters in the cache.
		if tpEqual && tpsExpected[i].GetType() == mysql.TypeNewDecimal && !(tpsExpected[i].GetFlen() >= tpsActual[i].GetFlen() && tpsExpected[i].GetDecimal() >= tpsActual[i].GetDecimal()) {
			return false
		}
	}
	return true
}

func isSafePointGetPath4PlanCache(sctx base.PlanContext, path *util.AccessPath) bool {
	// PointGet might contain some over-optimized assumptions, like `a>=1 and a<=1` --> `a=1`, but
	// these assumptions may be broken after parameters change.

	if isSafePointGetPath4PlanCacheScenario1(path) {
		return true
	}

	// TODO: enable this fix control switch by default after more test cases are added.
	if sctx != nil && sctx.GetSessionVars() != nil && sctx.GetSessionVars().OptimizerFixControl != nil {
		fixControlOK := fixcontrol.GetBoolWithDefault(sctx.GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix44830, false)
		if fixControlOK && (isSafePointGetPath4PlanCacheScenario2(path) || isSafePointGetPath4PlanCacheScenario3(path)) {
			return true
		}
	}

	return false
}

func isSafePointGetPath4PlanCacheScenario1(path *util.AccessPath) bool {
	// safe scenario 1: each column corresponds to a single EQ, `a=1 and b=2 and c=3` --> `[1, 2, 3]`
	if len(path.Ranges) <= 0 || path.Ranges[0].Width() != len(path.AccessConds) {
		return false
	}
	for _, accessCond := range path.AccessConds {
		f, ok := accessCond.(*expression.ScalarFunction)
		if !ok || f.FuncName.L != ast.EQ { // column = constant
			return false
		}
	}
	return true
}

func isSafePointGetPath4PlanCacheScenario2(path *util.AccessPath) bool {
	// safe scenario 2: this Batch or PointGet is simply from a single IN predicate, `key in (...)`
	if len(path.Ranges) <= 0 || len(path.AccessConds) != 1 {
		return false
	}
	f, ok := path.AccessConds[0].(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.In {
		return false
	}
	return len(path.Ranges) == len(f.GetArgs())-1 // no duplicated values in this in-list for safety.
}

func isSafePointGetPath4PlanCacheScenario3(path *util.AccessPath) bool {
	// safe scenario 3: this Batch or PointGet is simply from a simple DNF like `key=? or key=? or key=?`
	if len(path.Ranges) <= 0 || len(path.AccessConds) != 1 {
		return false
	}
	f, ok := path.AccessConds[0].(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.LogicOr {
		return false
	}

	dnfExprs := expression.FlattenDNFConditions(f)
	if len(path.Ranges) != len(dnfExprs) {
		// no duplicated values in this in-list for safety.
		// e.g. `k=1 or k=2 or k=1` --> [[1, 1], [2, 2]]
		return false
	}

	for _, expr := range dnfExprs {
		f, ok := expr.(*expression.ScalarFunction)
		if !ok {
			return false
		}
		switch f.FuncName.L {
		case ast.EQ: // (k=1 or k=2) --> [k=1, k=2]
		case ast.LogicAnd: // ((k1=1 and k2=1) or (k1=2 and k2=2)) --> [k1=1 and k2=1, k2=2 and k2=2]
			cnfExprs := expression.FlattenCNFConditions(f)
			if path.Ranges[0].Width() != len(cnfExprs) { // not all key columns are specified
				return false
			}
			for _, expr := range cnfExprs { // k1=1 and k2=1
				f, ok := expr.(*expression.ScalarFunction)
				if !ok || f.FuncName.L != ast.EQ {
					return false
				}
			}
		default:
			return false
		}
	}
	return true
}

// parseParamTypes get parameters' types in PREPARE statement
func parseParamTypes(sctx sessionctx.Context, params []expression.Expression) (paramTypes []*types.FieldType) {
	ectx := sctx.GetExprCtx().GetEvalCtx()
	paramTypes = make([]*types.FieldType, 0, len(params))
	for _, param := range params {
		if c, ok := param.(*expression.Constant); ok { // from binary protocol
			paramTypes = append(paramTypes, c.GetType(ectx))
			continue
		}

		// from text protocol, there must be a GetVar function
		name := param.(*expression.ScalarFunction).GetArgs()[0].StringWithCtx(ectx, errors.RedactLogDisable)
		tp, ok := sctx.GetSessionVars().GetUserVarType(name)
		if !ok {
			tp = types.NewFieldType(mysql.TypeNull)
		}
		paramTypes = append(paramTypes, tp)
	}
	return
}
