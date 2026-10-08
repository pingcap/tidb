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

package expression

import (
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/localfts"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var _ functionClass = &mysqlMatchAgainstFunctionClass{}
var _ builtinFunc = &builtinMysqlMatchAgainstSig{}

type mysqlMatchAgainstFunctionClass struct {
	baseFunctionClass
}

type builtinMysqlMatchAgainstSig struct {
	baseBuiltinFunc
	modifier        ast.FulltextSearchModifier
	localEvalInfo   *LocalMatchAgainstEvalInfo
	tiFlashEvalInfo *LocalMatchAgainstTiFlashEvalInfo

	// A prepared statement can reuse this signature with different search
	// strings. Key the compiled plan by the actual string and protect it when
	// executor workers share the signature.
	localPlanMu sync.Mutex
	localPlan   *localMatchAgainstEvalPlan
}

// LocalMatchAgainstEvalInfo is planner-validated metadata authorizing TiDB's
// row-wise, no-score MATCH ... AGAINST evaluator. The planner only attaches it in direct
// boolean predicate positions, so relevance-score positions never receive a
// 0/1 value.
type LocalMatchAgainstEvalInfo struct {
	AnalyzerConfig  localfts.AnalyzerConfig
	SelectivityTerm string
	MatchNothing    bool
}

// LocalMatchAgainstTiFlashEvalInfo carries the Boolean query AST for TiFlash's
// row-wise scalar MATCH expression. The query is serialized in Expr.val,
// independently of the SQL arguments in Expr.children. A planner-built
// expression can carry both this metadata and LocalMatchAgainstEvalInfo so TiDB
// can evaluate it if TiFlash pushdown is not selected.
type LocalMatchAgainstTiFlashEvalInfo struct {
	BooleanQuery *tipb.LocalMatchAgainstBooleanQuery
}

// localMatchAgainstProtocolVersion versions the Local MATCH semantics carried
// by Expr.val, including the built-in stopword set. TiFlash must implement and
// be deployed with a version before TiDB emits it. Never change the meaning of
// an existing version; reject unknown versions and add a new version instead.
const localMatchAgainstProtocolVersion uint32 = 1

// Clone returns an independent copy of the TiFlash evaluation metadata.
func (info *LocalMatchAgainstTiFlashEvalInfo) Clone() *LocalMatchAgainstTiFlashEvalInfo {
	if info == nil {
		return nil
	}
	cloned := &LocalMatchAgainstTiFlashEvalInfo{}
	if info.BooleanQuery != nil {
		cloned.BooleanQuery = proto.Clone(info.BooleanQuery).(*tipb.LocalMatchAgainstBooleanQuery)
	}
	return cloned
}

// Clone returns an independent copy of the local evaluation metadata.
func (info *LocalMatchAgainstEvalInfo) Clone() *LocalMatchAgainstEvalInfo {
	if info == nil {
		return nil
	}
	cloned := *info
	return &cloned
}

type localMatchAgainstEvalPlan struct {
	search   string
	query    *localfts.Query
	analyzer localfts.Analyzer
}

func (b *builtinMysqlMatchAgainstSig) Clone() builtinFunc {
	newSig := &builtinMysqlMatchAgainstSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.modifier = b.modifier
	newSig.localEvalInfo = b.localEvalInfo.Clone()
	newSig.tiFlashEvalInfo = b.tiFlashEvalInfo.Clone()
	return newSig
}

// SetMatchAgainstModifier sets the SQL modifier on the internal
// MATCH ... AGAINST builtin immediately after planner construction.
func SetMatchAgainstModifier(sf *ScalarFunction, modifier ast.FulltextSearchModifier) error {
	sig, ok := sf.Function.(*builtinMysqlMatchAgainstSig)
	if !ok {
		return errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	sig.modifier = modifier
	if modifier.IsBooleanMode() && !modifier.WithQueryExpansion() {
		sig.setPbCode(tipb.ScalarFuncSig_LocalMatchAgainstBoolean)
	} else {
		// Only Boolean mode has a TiFlash scalar protocol. Keep other MATCH
		// modifiers local instead of serializing them as a different Local MATCH opcode.
		sig.setPbCode(tipb.ScalarFuncSig_Unspecified)
	}
	return nil
}

// GetMatchAgainstModifier returns the modifier attached to the
// internal `MATCH ... AGAINST` builtin signature.
func GetMatchAgainstModifier(sf *ScalarFunction) (ast.FulltextSearchModifier, bool) {
	sig, ok := sf.Function.(*builtinMysqlMatchAgainstSig)
	if !ok {
		return ast.FulltextSearchModifierNaturalLanguageMode, false
	}
	return sig.modifier, true
}

// SetLocalMatchAgainstEvalInfo authorises local no-score evaluation.
func SetLocalMatchAgainstEvalInfo(sf *ScalarFunction, info *LocalMatchAgainstEvalInfo) error {
	sig, ok := sf.Function.(*builtinMysqlMatchAgainstSig)
	if !ok {
		return errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	sig.localEvalInfo = info.Clone()
	return nil
}

// GetLocalMatchAgainstEvalInfo returns attached local-evaluation metadata.
func GetLocalMatchAgainstEvalInfo(sf *ScalarFunction) (*LocalMatchAgainstEvalInfo, bool) {
	sig, ok := sf.Function.(*builtinMysqlMatchAgainstSig)
	if !ok || sig.localEvalInfo == nil {
		return nil, false
	}
	return sig.localEvalInfo, true
}

// SetLocalMatchAgainstTiFlashEvalInfo attaches the planner-validated
// Boolean query representation required by TiFlash's row-wise Local MATCH function.
func SetLocalMatchAgainstTiFlashEvalInfo(sf *ScalarFunction, info *LocalMatchAgainstTiFlashEvalInfo) error {
	sig, ok := sf.Function.(*builtinMysqlMatchAgainstSig)
	if !ok {
		return errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	sig.tiFlashEvalInfo = info.Clone()
	return nil
}

// GetLocalMatchAgainstTiFlashEvalInfo returns attached TiFlash-evaluation metadata.
func GetLocalMatchAgainstTiFlashEvalInfo(sf *ScalarFunction) (*LocalMatchAgainstTiFlashEvalInfo, bool) {
	sig, ok := sf.Function.(*builtinMysqlMatchAgainstSig)
	if !ok || sig.tiFlashEvalInfo == nil {
		return nil, false
	}
	return sig.tiFlashEvalInfo, true
}

// MatchAgainstModifierSupportedByLocalNoScore reports whether local Boolean matching can
// preserve the SQL modifier semantics. Natural-language relevance and query
// expansion are deliberately excluded.
func MatchAgainstModifierSupportedByLocalNoScore(modifier ast.FulltextSearchModifier) bool {
	return modifier.IsBooleanMode() && !modifier.WithQueryExpansion()
}

// CompileLocalMatchAgainstQuery compiles a stable search argument at
// plan time, surfacing syntax errors even for empty inputs or short-circuits.
func CompileLocalMatchAgainstQuery(ctx EvalContext, sf *ScalarFunction, config localfts.AnalyzerConfig) (*localfts.Query, error) {
	sig, ok := sf.Function.(*builtinMysqlMatchAgainstSig)
	if !ok {
		return nil, errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	if !MatchAgainstModifierSupportedByLocalNoScore(sig.modifier) {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("local MATCH ... AGAINST outside of IN BOOLEAN MODE")
	}
	search, isNull, err := sig.args[0].EvalString(ctx, chunk.Row{})
	if err != nil || isNull {
		return nil, err
	}
	return localfts.CompileBooleanQuery(search, config)
}

func (c *mysqlMatchAgainstFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	against, ok := args[0].(*Constant)
	if !ok {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("match against a non-constant string")
	}
	if against.Value.Kind() != types.KindString && !against.Value.IsNull() && against.ParamMarker == nil {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("match against a non-string constant")
	}

	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETString)
	for _, arg := range args[1:] {
		if _, ok := arg.(*Column); !ok {
			return nil, ErrNotSupportedYet.GenWithStackByArgs("not matching a column")
		}
		if arg.GetType(ctx.GetEvalCtx()).EvalType() != types.ETString {
			return nil, ErrNotSupportedYet.GenWithStackByArgs("Doesn't support match search on a non-string column without fulltext index")
		}
		argTps = append(argTps, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}

	sig := &builtinMysqlMatchAgainstSig{baseBuiltinFunc: bf}
	// The SQL modifier is attached after builtin construction. Until then this
	// function must not be pushable to a storage engine.
	sig.setPbCode(tipb.ScalarFuncSig_Unspecified)
	return sig, nil
}

func (b *builtinMysqlMatchAgainstSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	if b.localEvalInfo == nil {
		if constArg, ok := b.args[0].(*Constant); ok && constArg.Value.IsNull() {
			return 0, true, nil
		}
		return 0, false, errors.Errorf("cannot use 'MATCH ... AGAINST' outside of fulltext index")
	}
	if !MatchAgainstModifierSupportedByLocalNoScore(b.modifier) {
		return 0, false, errors.Errorf("local 'MATCH ... AGAINST' only supports IN BOOLEAN MODE")
	}

	search, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return 0, isNull, err
	}
	// Note there is deliberately no short-circuit on
	// localEvalInfo.MatchNothing here. That flag is derived from the search
	// string seen at plan time, and a plan can be re-executed with a different
	// one; MatchesNothing below is read from the query compiled for the search
	// string actually in hand, so it cannot go stale. The flag saves nothing at
	// runtime either, since the compiled query is cached per search string and
	// the check below short-circuits before any document is analyzed.
	plan, err := b.getOrBuildLocalNoScorePlan(search)
	if err != nil {
		return 0, false, err
	}
	if plan.query.MatchesNothing() {
		return 0, false, nil
	}
	columns, err := b.evalLocalMatchColumns(ctx, row)
	if err != nil {
		return 0, false, err
	}
	doc, err := localfts.BuildDocument(columns, plan.analyzer)
	if err != nil {
		return 0, false, err
	}
	if plan.query.Match(doc) {
		return 1, false, nil
	}
	return 0, false, nil
}

func (b *builtinMysqlMatchAgainstSig) getOrBuildLocalNoScorePlan(search string) (*localMatchAgainstEvalPlan, error) {
	b.localPlanMu.Lock()
	defer b.localPlanMu.Unlock()
	if b.localPlan != nil && b.localPlan.search == search {
		return b.localPlan, nil
	}
	analyzer, err := localfts.GetAnalyzer(b.localEvalInfo.AnalyzerConfig)
	if err != nil {
		return nil, err
	}
	query, err := localfts.CompileBooleanQuery(search, b.localEvalInfo.AnalyzerConfig)
	if err != nil {
		return nil, err
	}
	b.localPlan = &localMatchAgainstEvalPlan{search: search, query: query, analyzer: analyzer}

	// Re-derive the search-dependent metadata from the query just compiled, so
	// it describes the search string this signature last saw rather than the
	// one present when the plan was built. Only the planner reads it today, and
	// only for a stable constant, so this changes nothing in practice; it keeps
	// the two from being able to disagree if that ever stops holding.
	if b.localEvalInfo != nil {
		refreshed := *b.localEvalInfo
		refreshed.MatchNothing = query.MatchesNothing()
		refreshed.SelectivityTerm, _ = query.SelectivityTerm()
		b.localEvalInfo = &refreshed
	}
	return b.localPlan, nil
}

func (b *builtinMysqlMatchAgainstSig) evalLocalMatchColumns(ctx EvalContext, row chunk.Row) ([]localfts.ColumnInput, error) {
	columns := make([]localfts.ColumnInput, 0, len(b.args)-1)
	for _, arg := range b.args[1:] {
		text, isNull, err := arg.EvalString(ctx, row)
		if err != nil {
			return nil, err
		}
		if isNull {
			columns = append(columns, localfts.ColumnInput{IsNull: true})
			continue
		}
		columns = append(columns, localfts.ColumnInput{Text: text})
	}
	return columns, nil
}
