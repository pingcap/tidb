// Copyright 2015 PingCAP, Inc.
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
	"fmt"
	"regexp"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

func (b *PlanBuilder) buildDropBindPlan(v *ast.DropBindingStmt) (base.Plan, error) {
	var p *SQLBindPlan
	if v.OriginNode != nil {
		normdOrigSQL, sqlDigestWithDB := bindinfo.NormalizeStmtForBinding(v.OriginNode, b.ctx.GetSessionVars().CurrentDB, false)
		p = &SQLBindPlan{
			IsGlobal:  v.GlobalScope,
			SQLBindOp: OpSQLBindDrop,
			Details: []*SQLBindOpDetail{{
				NormdOrigSQL: normdOrigSQL,
				Db:           utilparser.GetDefaultDB(v.OriginNode, b.ctx.GetSessionVars().CurrentDB),
				SQLDigest:    sqlDigestWithDB,
			}},
		}
		if v.HintedNode != nil {
			p.Details[0].BindSQL = bindinfo.RestoreDBForBinding(v.HintedNode, b.ctx.GetSessionVars().CurrentDB)
		}
	} else {
		sqlDigests, err := collectStrOrUserVarList(b.ctx, v.SQLDigests)
		if err != nil {
			return nil, err
		}
		if len(sqlDigests) == 0 {
			return nil, errors.New("sql digest is empty")
		}
		details := make([]*SQLBindOpDetail, 0, len(sqlDigests))
		for _, sqlDigest := range sqlDigests {
			details = append(details, &SQLBindOpDetail{SQLDigest: sqlDigest})
		}
		p = &SQLBindPlan{
			SQLBindOp: OpSQLBindDropByDigest,
			IsGlobal:  v.GlobalScope,
			Details:   details,
		}
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return p, nil
}
func (b *PlanBuilder) buildSetBindingStatusPlan(v *ast.SetBindingStmt) (base.Plan, error) {
	var p *SQLBindPlan
	if v.OriginNode != nil {
		normdSQL, _ := bindinfo.NormalizeStmtForBinding(v.OriginNode, b.ctx.GetSessionVars().CurrentDB, false)
		p = &SQLBindPlan{
			SQLBindOp: OpSetBindingStatus,
			Details: []*SQLBindOpDetail{{
				NormdOrigSQL: normdSQL,
				Db:           utilparser.GetDefaultDB(v.OriginNode, b.ctx.GetSessionVars().CurrentDB),
			}},
		}
	} else if v.SQLDigest != "" {
		p = &SQLBindPlan{
			SQLBindOp: OpSetBindingStatusByDigest,
			Details: []*SQLBindOpDetail{{
				SQLDigest: v.SQLDigest,
			}},
		}
	} else {
		return nil, errors.New("sql digest is empty")
	}
	switch v.BindingStatusType {
	case ast.BindingStatusTypeEnabled:
		p.Details[0].NewStatus = bindinfo.StatusEnabled
	case ast.BindingStatusTypeDisabled:
		p.Details[0].NewStatus = bindinfo.StatusDisabled
	}
	if v.HintedNode != nil {
		p.Details[0].BindSQL = bindinfo.RestoreDBForBinding(v.HintedNode, b.ctx.GetSessionVars().CurrentDB)
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return p, nil
}

func checkHintedSQL(sql, charset, collation, db string) error {
	p := parser.New()
	hintsSet, _, warns, err := hint.ParseHintsSet(p, sql, charset, collation, db)
	if err != nil {
		return err
	}
	hintsStr, err := hintsSet.Restore()
	if err != nil {
		return err
	}
	// For `create global binding for select * from t using select * from t`, we allow it though hintsStr is empty.
	// For `create global binding for select * from t using select /*+ non_exist_hint() */ * from t`,
	// the hint is totally invalid, we escalate warning to error.
	if hintsStr == "" && len(warns) > 0 {
		return warns[0]
	}
	return nil
}

func fetchRecordFromClusterStmtSummary(sctx base.PlanContext, planDigest string) (schema, query, planHint, charset, collation string, err error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo)
	exec := sctx.GetSQLExecutor()
	prefix := "cluster_"
	if intest.InTest {
		prefix = ""
	}
	fields := "stmt_type, schema_name, digest_text, sample_user, prepared, query_sample_text, charset, collation, plan_hint, plan_digest"
	sql := fmt.Sprintf("select %s from information_schema.%stidb_statements_stats where plan_digest = '%s' union distinct ", fields, prefix, planDigest) +
		fmt.Sprintf("select %s from information_schema.%sstatements_summary_history where plan_digest = '%s' ", fields, prefix, planDigest) +
		"order by length(plan_digest) desc"
	rs, err := exec.ExecuteInternal(ctx, sql)
	if err != nil {
		return "", "", "", "", "", err
	}
	if rs == nil {
		return "", "", "", "", "",
			errors.New("can't find any records for '" + planDigest + "' in statement summary")
	}

	var rows []chunk.Row
	defer terror.Call(rs.Close)
	if rows, err = sqlexec.DrainRecordSet(ctx, rs, 8); err != nil {
		return "", "", "", "", "", err
	}

	for _, row := range rows {
		user := row.GetString(3)
		stmtType := row.GetString(0)
		if user != "" && (stmtType == "Select" || stmtType == "Delete" || stmtType == "Update" || stmtType == "Insert" || stmtType == "Replace") {
			// Empty auth users means that it is an internal queries.
			schema = row.GetString(1)
			query = row.GetString(5)
			planHint = row.GetString(8)
			charset = row.GetString(6)
			collation = row.GetString(7)
			// If it is SQL command prepare / execute, we should remove the arguments
			// If it is binary protocol prepare / execute, ssbd.normalizedSQL should be same as ssElement.sampleSQL.
			if row.GetInt64(4) == 1 {
				if idx := strings.LastIndex(query, "[arguments:"); idx != -1 {
					query = query[:idx]
				}
			}
			return
		}
	}
	return
}

func collectStrOrUserVarList(ctx base.PlanContext, list []*ast.StringOrUserVar) ([]string, error) {
	result := make([]string, 0, len(list))
	for _, single := range list {
		var str string
		if single.UserVar != nil {
			val, ok := ctx.GetSessionVars().GetUserVarVal(strings.ToLower(single.UserVar.Name))
			if !ok {
				return nil, errors.New("can't find specified user variable: " + single.UserVar.Name)
			}
			var err error
			str, err = val.ToString()
			if err != nil {
				return nil, err
			}
		} else {
			str = single.StringLit
		}
		split := strings.Split(str, ",")
		for _, single := range split {
			trimmed := strings.TrimSpace(single)
			if len(trimmed) > 0 {
				result = append(result, trimmed)
			}
		}
	}
	return result, nil
}

// constructSQLBindOPFromPlanDigest tries to construct a SQLBindOpDetail from plan digest by fetching the corresponding
// record from cluster_statements_summary or cluster_statements_summary_history.
// If it fails to construct the SQLBindOpDetail for any reason, it will return (nil, error).
// If the plan digest corresponds to the same SQL digest as another one in handledSQLDigests, it will append a warning
// then return (nil, nil).
func constructSQLBindOPFromPlanDigest(
	ctx base.PlanContext,
	planDigest string,
	handledSQLDigests map[string]struct{},
) (
	*SQLBindOpDetail,
	error,
) {
	// The warnings will be broken in fetchRecordFromClusterStmtSummary(), so we need to save and restore it to make the
	// warnings for repeated SQL Digest work.
	warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
	schema, query, planHint, charset, collation, err := fetchRecordFromClusterStmtSummary(ctx, planDigest)
	if err != nil {
		return nil, err
	}
	if query == "" {
		return nil, errors.New("can't find any plans for '" + planDigest + "'")
	}
	// Check if the SQL is truncated (ends with "(len:<num>)" pattern)
	if query[len(query)-1] == ')' {
		match, err2 := regexp.MatchString(`\(len:\d+\)$`, query)
		if match || err2 != nil {
			return nil, errors.NewNoStackErrorf("binding failed: SQL query is truncated due to tidb_stmt_summary_max_sql_length limit. "+
				"Please increase tidb_stmt_summary_max_sql_length. Plan Digest: %v", planDigest)
		}
	}
	ctx.GetSessionVars().StmtCtx.SetWarnings(warnings)
	parser4binding := parser.New()
	originNode, err := parser4binding.ParseOneStmt(query, charset, collation)
	if err != nil {
		return nil, errors.NewNoStackErrorf("binding failed: %v. Plan Digest: %v", err, planDigest)
	}
	complete, reason := hint.CheckBindingFromHistoryComplete(originNode, planHint)
	bindSQL := bindinfo.GenerateBindingSQL(originNode, planHint, schema)
	var hintNode ast.StmtNode
	hintNode, err = parser4binding.ParseOneStmt(bindSQL, charset, collation)
	if err != nil {
		return nil, errors.NewNoStackErrorf("binding failed: %v. Plan Digest: %v", err, planDigest)
	}
	bindSQL = bindinfo.RestoreDBForBinding(hintNode, schema)
	db := utilparser.GetDefaultDB(originNode, schema)
	normdOrigSQL, sqlDigestWithDBStr := bindinfo.NormalizeStmtForBinding(originNode, schema, false)
	if _, ok := handledSQLDigests[sqlDigestWithDBStr]; ok {
		ctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError(
			planDigest + " is ignored because it corresponds to the same SQL digest as another Plan Digest",
		))
		return nil, nil
	}
	handledSQLDigests[sqlDigestWithDBStr] = struct{}{}
	if !complete {
		ctx.GetSessionVars().StmtCtx.AppendWarning(
			errors.NewNoStackErrorf("%v. Plan Digest: %v", reason, planDigest),
		)
	}
	op := &SQLBindOpDetail{
		NormdOrigSQL: normdOrigSQL,
		BindSQL:      bindSQL,
		BindStmt:     hintNode,
		Db:           db,
		Charset:      charset,
		Collation:    collation,
		Source:       bindinfo.SourceHistory,
		SQLDigest:    sqlDigestWithDBStr,
		PlanDigest:   planDigest,
	}
	return op, nil
}

func (b *PlanBuilder) buildCreateBindPlanFromPlanDigest(v *ast.CreateBindingStmt) (base.Plan, error) {
	planDigests, err := collectStrOrUserVarList(b.ctx, v.PlanDigests)
	if err != nil {
		return nil, err
	}
	if len(planDigests) == 0 {
		return nil, errors.New("plan digest is empty")
	}
	handledSQLDigests := make(map[string]struct{}, len(planDigests))
	opDetails := make([]*SQLBindOpDetail, 0, len(planDigests))
	for _, planDigest := range planDigests {
		op, err2 := constructSQLBindOPFromPlanDigest(b.ctx, planDigest, handledSQLDigests)
		if err2 != nil {
			return nil, err2
		}
		if op == nil {
			continue
		}
		opDetails = append(opDetails, op)
	}

	p := &SQLBindPlan{
		IsGlobal:  v.GlobalScope,
		SQLBindOp: OpSQLBindCreate,
		Details:   opDetails,
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return p, nil
}

func (b *PlanBuilder) buildCreateBindPlan(v *ast.CreateBindingStmt) (base.Plan, error) {
	if v.OriginNode == nil {
		return b.buildCreateBindPlanFromPlanDigest(v)
	}
	charSet, collation := b.ctx.GetSessionVars().GetCharsetInfo()

	// Because we use HintedNode.Restore instead of HintedNode.Text, so we need do some check here
	// For example, if HintedNode.Text is `select /*+ non_exist_hint() */ * from t` and the current DB is `test`,
	// the HintedNode.Restore will be `select * from test . t`.
	// In other words, illegal hints will be deleted during restore. We can't check hinted SQL after restore.
	// So we need check here.
	if err := checkHintedSQL(v.HintedNode.Text(), charSet, collation, b.ctx.GetSessionVars().CurrentDB); err != nil {
		return nil, err
	}

	bindSQL := bindinfo.RestoreDBForBinding(v.HintedNode, b.ctx.GetSessionVars().CurrentDB)
	db := utilparser.GetDefaultDB(v.OriginNode, b.ctx.GetSessionVars().CurrentDB)
	normdOrigSQL, sqlDigestWithDB := bindinfo.NormalizeStmtForBinding(v.OriginNode, b.ctx.GetSessionVars().CurrentDB, false)
	p := &SQLBindPlan{
		IsGlobal:  v.GlobalScope,
		SQLBindOp: OpSQLBindCreate,
		Details: []*SQLBindOpDetail{{
			NormdOrigSQL: normdOrigSQL,
			BindSQL:      bindSQL,
			BindStmt:     v.HintedNode,
			Db:           db,
			Charset:      charSet,
			Collation:    collation,
			Source:       bindinfo.SourceManual,
			SQLDigest:    sqlDigestWithDB,
		}},
	}
	b.visitInfo = appendVisitInfo(b.visitInfo, mysql.SuperPriv, "", "", "", nil)
	return p, nil
}

