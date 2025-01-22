// Copyright 2019 PingCAP, Inc.
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

package bindinfo

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"unsafe"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/hint"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	// Enabled is the bind info's in enabled status.
	// It is the same as the previous 'Using' status.
	// Only use 'Enabled' status in the future, not the 'Using' status.
	// The 'Using' status is preserved for compatibility.
	Enabled = "enabled"
	// Disabled is the bind info's in disabled status.
	Disabled = "disabled"
	// Using is the bind info's in use status.
	// The 'Using' status is preserved for compatibility.
	Using = "using"
	// deleted is the bind info's deleted status.
	deleted = "deleted"
	// Manual indicates the binding is created by SQL like "create binding for ...".
	Manual = "manual"
	// Builtin indicates the binding is a builtin record for internal locking purpose. It is also the status for the builtin binding.
	Builtin = "builtin"
	// History indicate the binding is created from statement summary by plan digest
	History = "history"
)

// Binding stores the basic bind hint info.
type Binding struct {
	OriginalSQL string
	Db          string
	BindSQL     string
	// Status represents the status of the binding. It can only be one of the following values:
	// 1. deleted: Bindings is deleted, can not be used anymore.
	// 2. enabled, using: Binding is in the normal active mode.
	Status     string
	CreateTime types.Time
	UpdateTime types.Time
	Source     string
	Charset    string
	Collation  string
	// Hint is the parsed hints, it is used to bind hints to stmt node.
	Hint *hint.HintsSet `json:"-"`
	// ID is the string form of Hint. It would be non-empty only when the status is `Using` or `PendingVerify`.
	ID         string `json:"-"`
	SQLDigest  string
	PlanDigest string

	// TableNames records all schema and table names in this binding statement, which are used for cross-db matching.
	TableNames []*ast.TableName `json:"-"`
}

// IsBindingEnabled returns whether the binding is enabled.
func (b *Binding) IsBindingEnabled() bool {
	return b.Status == Enabled || b.Status == Using
}

// size calculates the memory size of a bind info.
func (b *Binding) size() float64 {
	res := len(b.OriginalSQL) + len(b.Db) + len(b.BindSQL) + len(b.Status) + 2*int(unsafe.Sizeof(b.CreateTime)) + len(b.Charset) + len(b.Collation) + len(b.ID)
	return float64(res)
}

var (
	// GetGlobalBindingHandle is a function to get the global binding handle.
	// It is mainly used to resolve cycle import issue.
	GetGlobalBindingHandle func(sctx sessionctx.Context) GlobalBindingHandle
)

// BindingMatchInfo records necessary information for cross-db binding matching.
// This is mainly for plan cache to avoid normalizing the same statement repeatedly.
type BindingMatchInfo struct {
	NoDBDigest string
	TableNames []*ast.TableName
}

// MatchSQLBindingForPlanCache matches binding for plan cache.
func MatchSQLBindingForPlanCache(sctx sessionctx.Context, stmtNode ast.StmtNode, info *BindingMatchInfo) (bindingSQL string, ignoreBinding bool) {
	binding, matched, _ := matchSQLBinding(sctx, stmtNode, info)
	if matched {
		bindingSQL = binding.BindSQL
		ignoreBinding = binding.Hint.ContainTableHint(hint.HintIgnorePlanCache)
	}
	return
}

// MatchSQLBinding returns the matched binding for this statement.
func MatchSQLBinding(sctx sessionctx.Context, stmtNode ast.StmtNode) (binding *Binding, matched bool, scope string) {
	return matchSQLBinding(sctx, stmtNode, nil)
}

func matchSQLBinding(sctx sessionctx.Context, stmtNode ast.StmtNode, info *BindingMatchInfo) (binding *Binding, matched bool, scope string) {
	useBinding := sctx.GetSessionVars().UsePlanBaselines
	if !useBinding || stmtNode == nil {
		return
	}
	// When the domain is initializing, the bind will be nil.
	if sctx.Value(SessionBindInfoKeyType) == nil {
		return
	}

	// record the normalization result into info to avoid repeat normalization next time.
	var noDBDigest string
	var tableNames []*ast.TableName
	if info == nil || info.TableNames == nil || info.NoDBDigest == "" {
		_, noDBDigest = NormalizeStmtForBinding(stmtNode, WithoutDB(true))
		tableNames = CollectTableNames(stmtNode)
		if info != nil {
			info.NoDBDigest = noDBDigest
			info.TableNames = tableNames
		}
	} else {
		noDBDigest = info.NoDBDigest
		tableNames = info.TableNames
	}

	sessionHandle := sctx.Value(SessionBindInfoKeyType).(SessionBindingHandle)
	if binding, matched := sessionHandle.MatchSessionBinding(sctx, noDBDigest, tableNames); matched {
		return binding, matched, metrics.ScopeSession
	}
	globalHandle := GetGlobalBindingHandle(sctx)
	if globalHandle == nil {
		return
	}
	binding, matched = globalHandle.MatchGlobalBinding(sctx, noDBDigest, tableNames)
	if matched {
		return binding, matched, metrics.ScopeGlobal
	}

	return
}

func noDBDigestFromBinding(binding *Binding) (string, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(binding.BindSQL, binding.Charset, binding.Collation)
	if err != nil {
		return "", err
	}
	_, bindingNoDBDigest := NormalizeStmtForBinding(stmt, WithoutDB(true))
	return bindingNoDBDigest, nil
}

func crossDBMatchBindings(sctx sessionctx.Context, tableNames []*ast.TableName, bindings []*Binding) (matchedBinding *Binding, isMatched bool) {
	leastWildcards := len(tableNames) + 1
	enableCrossDBBinding := sctx.GetSessionVars().EnableFuzzyBinding
	for _, binding := range bindings {
		numWildcards, matched := crossDBMatchBindingTableName(sctx.GetSessionVars().CurrentDB, tableNames, binding.TableNames)
		if matched && numWildcards > 0 && sctx != nil && !enableCrossDBBinding {
			continue // cross-db binding is disabled, skip this binding
		}
		if matched && numWildcards < leastWildcards {
			matchedBinding = binding
			isMatched = true
			leastWildcards = numWildcards
		}
	}
	return
}

func crossDBMatchBindingTableName(currentDB string, stmtTableNames, bindingTableNames []*ast.TableName) (numWildcards int, matched bool) {
	if len(stmtTableNames) != len(bindingTableNames) {
		return 0, false
	}
	for i := range stmtTableNames {
		if stmtTableNames[i].Name.L != bindingTableNames[i].Name.L {
			return 0, false
		}
		if bindingTableNames[i].Schema.L == "*" {
			numWildcards++
		}
		if bindingTableNames[i].Schema.L == stmtTableNames[i].Schema.L || // exactly same, or
			(stmtTableNames[i].Schema.L == "" && bindingTableNames[i].Schema.L == strings.ToLower(currentDB)) || // equal to the current DB, or
			bindingTableNames[i].Schema.L == "*" { // cross-db match successfully
			continue
		}
		return 0, false
	}
	return numWildcards, true
}

// isCrossDBBinding checks whether the stmtNode is a cross-db binding.
func isCrossDBBinding(stmt ast.Node) bool {
	for _, t := range CollectTableNames(stmt) {
		if t.Schema.L == "*" {
			return true
		}
	}
	return false
}

// CollectTableNames gets all table names from ast.Node.
// This function is mainly for binding cross-db matching.
// ** the return is read-only.
// For example:
//
//	`select * from t1 where a < 1` --> [t1]
//	`select * from db1.t1, t2 where a < 1` --> [db1.t1, t2]
//
// You can see more example at the TestExtractTableName.
func CollectTableNames(in ast.Node) []*ast.TableName {
	collector := tableNameCollectorPool.Get().(*tableNameCollector)
	defer func() {
		collector.tableNames = nil
		tableNameCollectorPool.Put(collector)
	}()
	in.Accept(collector)
	return collector.tableNames
}

var tableNameCollectorPool = sync.Pool{
	New: func() any {
		return newCollectTableName()
	},
}

type tableNameCollector struct {
	tableNames []*ast.TableName
}

func newCollectTableName() *tableNameCollector {
	return &tableNameCollector{
		tableNames: make([]*ast.TableName, 0, 4),
	}
}

// Enter implements Visitor interface.
func (c *tableNameCollector) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	if node, ok := in.(*ast.TableName); ok {
		c.tableNames = append(c.tableNames, node)
		return in, true
	}
	return in, false
}

// Leave implements Visitor interface.
func (*tableNameCollector) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

// prepareHints builds ID and Hint for Bindings. If sctx is not nil, we check if
// the BindSQL is still valid.
func prepareHints(sctx sessionctx.Context, binding *Binding) (rerr error) {
	defer func() {
		if r := recover(); r != nil {
			rerr = errors.Errorf("panic when preparing hints for binding %v, panic: %v", binding.BindSQL, r)
		}
	}()

	p := parser.New()
	if (binding.Hint != nil && binding.ID != "") || binding.Status == deleted {
		return nil
	}
	dbName := binding.Db
	bindingStmt, err := p.ParseOneStmt(binding.BindSQL, binding.Charset, binding.Collation)
	if err != nil {
		return err
	}
	tableNames := CollectTableNames(bindingStmt)
	isCrossDB := isCrossDBBinding(bindingStmt)
	if isCrossDB {
		dbName = "*" // ues '*' for universal bindings
	}

	hintsSet, stmt, warns, err := hint.ParseHintsSet(p, binding.BindSQL, binding.Charset, binding.Collation, dbName)
	if err != nil {
		return err
	}
	if !isCrossDB && !hasParam(stmt) {
		// TODO: how to check cross-db binding and bindings with parameters?
		if err = checkBindingValidation(sctx, binding.BindSQL); err != nil {
			return err
		}
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
	binding.Hint = hintsSet
	binding.ID = hintsStr
	binding.TableNames = tableNames
	return nil
}

// pickCachedBinding picks the best binding to cache.
func pickCachedBinding(cachedBinding *Binding, bindingsFromStorage ...*Binding) *Binding {
	bindings := make([]*Binding, 0, len(bindingsFromStorage)+1)
	bindings = append(bindings, cachedBinding)
	bindings = append(bindings, bindingsFromStorage...)

	// filter nil
	n := 0
	for _, binding := range bindings {
		if binding != nil {
			bindings[n] = binding
			n++
		}
	}
	if len(bindings) == 0 {
		return nil
	}

	// filter bindings whose update time is not equal to maxUpdateTime
	maxUpdateTime := bindings[0].UpdateTime
	for _, binding := range bindings {
		if binding.UpdateTime.Compare(maxUpdateTime) > 0 {
			maxUpdateTime = binding.UpdateTime
		}
	}
	n = 0
	for _, binding := range bindings {
		if binding.UpdateTime.Compare(maxUpdateTime) == 0 {
			bindings[n] = binding
			n++
		}
	}
	bindings = bindings[:n]

	// filter deleted bindings
	n = 0
	for _, binding := range bindings {
		if binding.Status != deleted {
			bindings[n] = binding
			n++
		}
	}
	bindings = bindings[:n]

	if len(bindings) == 0 {
		return nil
	}
	// should only have one binding.
	return bindings[0]
}

type option struct {
	specifiedDB string
	noDB        bool
}

type optionFunc func(*option)

// WithoutDB specifies whether to eliminate schema names.
func WithoutDB(noDB bool) optionFunc {
	return func(user *option) {
		user.noDB = noDB
	}
}

// WithSpecifiedDB specifies the specified DB name.
func WithSpecifiedDB(specifiedDB string) optionFunc {
	return func(user *option) {
		user.specifiedDB = specifiedDB
	}
}

// NormalizeStmtForBinding normalizes a statement for binding.
// when noDB is false, schema names will be completed automatically: `select * from t` --> `select * from db . t`.
// when noDB is true, schema names will be eliminated automatically: `select * from db . t` --> `select * from t`.
func NormalizeStmtForBinding(stmtNode ast.StmtNode, options ...optionFunc) (normalizedStmt, exactSQLDigest string) {
	opt := &option{}
	for _, option := range options {
		option(opt)
	}
	return normalizeStmt(stmtNode, opt.specifiedDB, opt.noDB)
}

// NormalizeStmtForBinding normalizes a statement for binding.
// This function skips Explain automatically, and literals in in-lists will be normalized as '...'.
// For normal bindings, DB name will be completed automatically:
//
//	e.g. `select * from t where a in (1, 2, 3)` --> `select * from test.t where a in (...)`
func normalizeStmt(stmtNode ast.StmtNode, specifiedDB string, noDB bool) (normalizedStmt, sqlDigest string) {
	normalize := func(n ast.StmtNode) (normalizedStmt, sqlDigest string) {
		eraseLastSemicolon(n)
		var digest *parser.Digest
		var normalizedSQL string
		if !noDB {
			normalizedSQL = utilparser.RestoreWithDefaultDB(n, specifiedDB, n.Text())
		} else {
			normalizedSQL = utilparser.RestoreWithoutDB(n)
		}
		normalizedStmt, digest = parser.NormalizeDigestForBinding(normalizedSQL)
		return normalizedStmt, digest.String()
	}

	switch x := stmtNode.(type) {
	case *ast.ExplainStmt:
		// This function is only used to find bind record.
		// For some SQLs, such as `explain select * from t`, they will be entered here many times,
		// but some of them do not want to obtain bind record.
		// The difference between them is whether len(x.Text()) is empty. They cannot be distinguished by stmt.restore.
		// For these cases, we need return "" as normalize SQL and hash.
		if len(x.Text()) == 0 {
			return "", ""
		}
		switch x.Stmt.(type) {
		case *ast.SelectStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.InsertStmt:
			normalizeSQL, digest := normalize(x.Stmt)
			return normalizeSQL, digest
		case *ast.SetOprStmt:
			normalizeExplainSQL, _ := normalize(x)

			idx := strings.Index(normalizeExplainSQL, "select")
			parenthesesIdx := strings.Index(normalizeExplainSQL, "(")
			if parenthesesIdx != -1 && parenthesesIdx < idx {
				idx = parenthesesIdx
			}
			// If the SQL is `EXPLAIN ((VALUES ROW ()) ORDER BY 1);`, the idx will be -1.
			if idx == -1 {
				hash := parser.DigestNormalized(normalizeExplainSQL)
				return normalizeExplainSQL, hash.String()
			}
			normalizeSQL := normalizeExplainSQL[idx:]
			hash := parser.DigestNormalized(normalizeSQL)
			return normalizeSQL, hash.String()
		}
	case *ast.SelectStmt, *ast.SetOprStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.InsertStmt:
		// This function is only used to find bind record.
		// For some SQLs, such as `explain select * from t`, they will be entered here many times,
		// but some of them do not want to obtain bind record.
		// The difference between them is whether len(x.Text()) is empty. They cannot be distinguished by stmt.restore.
		// For these cases, we need return "" as normalize SQL and hash.
		if len(x.Text()) == 0 {
			return "", ""
		}
		normalizedSQL, digest := normalize(x)
		return normalizedSQL, digest
	}
	return "", ""
}

func eraseLastSemicolon(stmt ast.StmtNode) {
	sql := stmt.Text()
	if len(sql) > 0 && sql[len(sql)-1] == ';' {
		stmt.SetText(nil, sql[:len(sql)-1])
	}
}

type paramChecker struct {
	hasParam bool
}

func (e *paramChecker) Enter(in ast.Node) (ast.Node, bool) {
	if _, ok := in.(*driver.ParamMarkerExpr); ok {
		e.hasParam = true
		return in, true
	}
	return in, false
}

func (*paramChecker) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// hasParam checks whether the statement contains any parameters.
// For example, `create binding using select * from t where a=?` contains a parameter '?'.
func hasParam(stmt ast.Node) bool {
	p := new(paramChecker)
	stmt.Accept(p)
	return p.hasParam
}

// CheckBindingStmt checks whether the statement is valid.
func checkBindingValidation(sctx sessionctx.Context, bindingSQL string) error {
	origVals := sctx.GetSessionVars().UsePlanBaselines
	sctx.GetSessionVars().UsePlanBaselines = false

	// Usually passing a sprintf to ExecuteInternal is not recommended, but in this case
	// it is safe because ExecuteInternal does not permit MultiStatement execution. Thus,
	// the statement won't be able to "break out" from EXPLAIN.
	rs, err := exec(sctx, fmt.Sprintf("EXPLAIN FORMAT='hint' %s", bindingSQL))
	sctx.GetSessionVars().UsePlanBaselines = origVals
	if rs != nil {
		defer func() {
			// Audit log is collected in Close(), set InRestrictedSQL to avoid 'create sql binding' been recorded as 'explain'.
			origin := sctx.GetSessionVars().InRestrictedSQL
			sctx.GetSessionVars().InRestrictedSQL = true
			if rerr := rs.Close(); rerr != nil {
				bindingLogger().Error("close result set failed", zap.Error(rerr), zap.String("binding_sql", bindingSQL))
			}
			sctx.GetSessionVars().InRestrictedSQL = origin
		}()
	}
	if err != nil {
		return err
	}
	chk := rs.NewChunk(nil)
	err = rs.Next(context.TODO(), chk)
	if err != nil {
		return err
	}
	return nil
}
