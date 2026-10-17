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

package core

import (
	"context"
	"strings"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

const nonPreparedASTValidationVersion uint32 = 2

type nonPreparedASTPolicyFingerprint struct {
	enablePlanCacheForSubquery     bool
	enablePlanCacheForParamLimit   bool
	maxParamCount                  int
	dynamicPartitionPruning        bool
	rejectPartitionForFix33031     bool
	enableGeneratedColumnPlanCache bool
	currentDB                      string
	charsetConnection              string
	collationConnection            string
	characterSetClient             string
	sqlMode                        uint64
	parserConfig                   parser.ParserConfig
}

type nonPreparedASTValidationState struct {
	version                 uint32
	policy                  nonPreparedASTPolicyFingerprint
	schemaMetaVersion       int64
	hadLocalTemporaryTables bool
}

func nonPreparedASTPolicy(sctx base.PlanContext) nonPreparedASTPolicyFingerprint {
	vars := sctx.GetSessionVars()
	charsetConnection, collationConnection := vars.GetCharsetInfo()
	characterSetClient, ok := vars.GetSystemVar(variable.CharacterSetClient)
	if !ok && vars.GlobalVarsAccessor != nil {
		var err error
		characterSetClient, err = vars.GetSessionOrGlobalSystemVar(context.Background(), variable.CharacterSetClient)
		ok = err == nil
	}
	if !ok {
		characterSetClient = variable.GetSysVar(variable.CharacterSetClient).Value
	}
	return nonPreparedASTPolicyFingerprint{
		enablePlanCacheForSubquery:     vars.EnablePlanCacheForSubquery,
		enablePlanCacheForParamLimit:   vars.EnablePlanCacheForParamLimit,
		maxParamCount:                  getMaxParamLimit(sctx),
		dynamicPartitionPruning:        vars.IsDynamicPartitionPruneEnabled(),
		rejectPartitionForFix33031:     fixcontrol.GetBoolWithDefault(vars.GetOptimizerFixControlMap(), fixcontrol.Fix33031, false),
		enableGeneratedColumnPlanCache: enablePlanCacheForGeneratedCols(sctx),
		currentDB:                      strings.ToLower(vars.CurrentDB),
		charsetConnection:              charsetConnection,
		collationConnection:            collationConnection,
		characterSetClient:             characterSetClient,
		sqlMode:                        uint64(vars.SQLMode),
		parserConfig:                   vars.BuildParserConfig(),
	}
}

func hasLocalTemporaryTables(is infoschema.InfoSchema) bool {
	extended, ok := is.(*infoschema.SessionExtendedInfoSchema)
	return ok && extended.LocalTemporaryTables != nil && extended.LocalTemporaryTables.Count() > 0
}

func setNonPreparedASTValidationState(
	sctx base.PlanContext,
	stmt *PlanCacheStmt,
	is infoschema.InfoSchema,
) {
	stmt.nonPreparedASTValidation = nonPreparedASTValidationState{
		version:                 nonPreparedASTValidationVersion,
		policy:                  nonPreparedASTPolicy(sctx),
		schemaMetaVersion:       is.SchemaMetaVersion(),
		hadLocalTemporaryTables: hasLocalTemporaryTables(is),
	}
}

// NonPreparedPlanCacheStmtDBMatches reports whether stmt was built for the
// current database. A legacy carrier without validation provenance can still be
// upgraded when its existing StmtDB agrees with the session.
func NonPreparedPlanCacheStmtDBMatches(sctx base.PlanContext, stmt *PlanCacheStmt) bool {
	currentDB := strings.ToLower(sctx.GetSessionVars().CurrentDB)
	state := stmt.nonPreparedASTValidation
	if state.version == 0 {
		return strings.ToLower(stmt.StmtDB) == currentDB
	}
	return state.policy.currentDB == currentDB
}

// RecordNonPreparedPlanCacheASTValidation records the inputs under which the
// unified non-prepared AST checker accepted stmt.
func RecordNonPreparedPlanCacheASTValidation(
	sctx base.PlanContext,
	stmt *PlanCacheStmt,
	is infoschema.InfoSchema,
) {
	if !stmt.StmtCacheable {
		return
	}
	setNonPreparedASTValidationState(sctx, stmt, is)
}

// ValidateNonPreparedPlanCacheAST reuses a prior AST-cacheability decision
// only when every dynamic input is unchanged. reusable reports that the AST
// visitor was skipped; false means the helper reran it under current inputs.
func ValidateNonPreparedPlanCacheAST(
	ctx context.Context,
	sctx base.PlanContext,
	stmt *PlanCacheStmt,
	is infoschema.InfoSchema,
) (cacheable bool, reason string, reusable bool) {
	state := stmt.nonPreparedASTValidation
	policy := nonPreparedASTPolicy(sctx)
	hasLocalTables := hasLocalTemporaryTables(is)
	forceCheck := ctx != nil && ctx.Value(PlanCacheKeyTestIssue46760{}) != nil
	if !forceCheck && stmt.StmtCacheable && state.version == nonPreparedASTValidationVersion &&
		state.policy == policy && state.schemaMetaVersion == is.SchemaMetaVersion() &&
		!state.hadLocalTemporaryTables && !hasLocalTables {
		notifyNonPreparedASTValidation(ctx, true)
		return true, "", true
	}

	notifyNonPreparedASTValidation(ctx, false)
	cacheable, reason = IsASTCacheable(ctx, sctx, stmt.PreparedAst.Stmt, is)
	if cacheable && stmt.StmtCacheable {
		setNonPreparedASTValidationState(sctx, stmt, is)
	}
	return cacheable, reason, false
}

func notifyNonPreparedASTValidation(ctx context.Context, reused bool) {
	if ctx == nil {
		return
	}
	if callback, ok := ctx.Value(PlanCacheKeyTestNonPreparedASTValidation{}).(func(bool)); ok {
		callback(reused)
	}
}
