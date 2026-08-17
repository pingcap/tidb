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

package expression

import (
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &ftsMatchWordFunctionClass{}
	_ functionClass = &ftsMysqlMatchAgainstFunctionClass{}
)

var (
	_ builtinFunc = &builtinFtsMatchWordSig{}
	_ builtinFunc = &builtinFtsMysqlMatchAgainstSig{}
)

type ftsMatchWordFunctionClass struct {
	baseFunctionClass
	expropt.SessionVarsPropReader
}

type builtinFtsMatchWordSig struct {
	baseBuiltinFunc
}

type ftsMysqlMatchAgainstFunctionClass struct {
	baseFunctionClass
}

type builtinFtsMysqlMatchAgainstSig struct {
	baseBuiltinFunc
	modifier      ast.FulltextSearchModifier
	localEvalInfo *FTSLocalEvalInfo

	// The compiled query is cached across rows: analysis and boolean-query
	// compilation depend only on the search string, which is constant for the
	// whole scan. The mutex guards against sigs shared between executor
	// workers; Clone deliberately leaves the cache empty rather than copying it.
	localPlanMu sync.Mutex
	localPlan   *ftsLocalEvalPlan
}

// FTSLocalEvalInfo is planner-validated metadata for local no-score
// MATCH ... AGAINST evaluation. Its presence is what authorises the builtin to
// evaluate in TiDB instead of erroring; the planner attaches it only for
// direct-boolean predicate positions, so a relevance-score position can never
// silently receive a 0/1 result.
//
// AnalyzerConfig is resolved once at plan time rather than read per row: the
// analyzer sysvars are session-mutable, and re-reading them mid-scan could
// tokenize later rows differently from earlier ones.
type FTSLocalEvalInfo struct {
	AnalyzerConfig fulltext.AnalyzerConfig
	// SelectivityTerm is a single analyzed token that the stats engine can use
	// as an ILIKE proxy. Empty when the query has no such safe approximation.
	SelectivityTerm string
	// MatchNothing records that the compiled query provably matches no
	// document, so evaluation can skip tokenizing rows entirely.
	MatchNothing bool
}

// Clone returns a deep copy of the local evaluation metadata.
func (info *FTSLocalEvalInfo) Clone() *FTSLocalEvalInfo {
	if info == nil {
		return nil
	}
	cloned := *info
	return &cloned
}

func (b *builtinFtsMysqlMatchAgainstSig) hasLocalEvalInfo() bool {
	return b.localEvalInfo != nil
}

// ftsLocalEvalPlan is the per-search-string compiled state for local matching.
type ftsLocalEvalPlan struct {
	search   string
	query    *fulltext.Query
	analyzer fulltext.Analyzer
}

func (b *builtinFtsMatchWordSig) Clone() builtinFunc {
	newSig := &builtinFtsMatchWordSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinFtsMysqlMatchAgainstSig) Clone() builtinFunc {
	newSig := &builtinFtsMysqlMatchAgainstSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.modifier = b.modifier
	newSig.localEvalInfo = b.localEvalInfo.Clone()
	return newSig
}

func (b *builtinFtsMysqlMatchAgainstSig) SetModifier(modifier ast.FulltextSearchModifier) {
	b.modifier = modifier
}

// SetFTSMysqlMatchAgainstModifier sets the modifier for the internal `MATCH ... AGAINST` builtin signature.
// It is expected to be called by planner right after building the scalar function.
func SetFTSMysqlMatchAgainstModifier(sf *ScalarFunction, modifier ast.FulltextSearchModifier) error {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	sig.SetModifier(modifier)
	return nil
}

// SetFTSMysqlMatchAgainstLocalEvalInfo attaches planner-validated local
// no-score evaluation metadata to a `MATCH ... AGAINST` builtin, authorising it
// to evaluate in TiDB. It is expected to be called by the planner right after
// building the scalar function.
func SetFTSMysqlMatchAgainstLocalEvalInfo(sf *ScalarFunction, info *FTSLocalEvalInfo) error {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	sig.localEvalInfo = info.Clone()
	return nil
}

// FTSMysqlMatchAgainstLocalEvalInfo returns the local no-score evaluation
// metadata attached to `MATCH ... AGAINST`, if any.
func FTSMysqlMatchAgainstLocalEvalInfo(sf *ScalarFunction) (*FTSLocalEvalInfo, bool) {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok || sig.localEvalInfo == nil {
		return nil, false
	}
	return sig.localEvalInfo, true
}

// FTSModifierSupportedByLocalNoScore reports whether the modifier can be
// evaluated by the local no-score `MATCH ... AGAINST` path. Natural-language
// mode is excluded because it is defined in terms of relevance ranking, which
// the no-score path cannot produce; query expansion additionally requires a
// second retrieval pass over an index.
func FTSModifierSupportedByLocalNoScore(modifier ast.FulltextSearchModifier) bool {
	return modifier.IsBooleanMode() && !modifier.WithQueryExpansion()
}

// CompileFTSMysqlMatchAgainstLocalQuery compiles the search string against the
// supplied analyzer configuration. The planner calls this before execution
// starts so BOOLEAN-syntax and capability errors surface at plan time: deferring
// them to the first row would let an empty input, or an earlier false
// predicate, hide the error entirely.
func CompileFTSMysqlMatchAgainstLocalQuery(ctx EvalContext, sf *ScalarFunction, config fulltext.AnalyzerConfig) (*fulltext.Query, error) {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return nil, errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	if !FTSModifierSupportedByLocalNoScore(sig.modifier) {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("local MATCH ... AGAINST outside of IN BOOLEAN MODE")
	}
	search, isNull, err := sig.args[0].EvalString(ctx, chunk.Row{})
	if err != nil {
		return nil, err
	}
	if isNull {
		return nil, nil
	}
	return fulltext.CompileBooleanQuery(search, config)
}

func (c *ftsMatchWordFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	if !deploymode.IsStarter() {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("FTS_MATCH_WORD() is only supported in starter deployment mode")
	}

	argAgainst := args[0]
	argAgainstConstant, ok := argAgainst.(*Constant)
	if !ok {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("match against a non-constant string")
	}
	if argAgainstConstant.Value.Kind() != types.KindString {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("match against a non-constant string")
	}
	argsMatch := args[1:]
	for _, arg := range argsMatch {
		_, ok := arg.(*Column)
		if !ok {
			return nil, ErrNotSupportedYet.GenWithStackByArgs("not matching a column")
		}
	}

	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETString, types.ETString)

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}

	sessionVars, err := c.GetSessionVars(ctx.GetEvalCtx())
	if err != nil {
		return nil, err
	}
	sessionVars.StmtCtx.FTSFunctionIsUsed = true

	sig := &builtinFtsMatchWordSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_FTSMatchWord)
	return sig, nil
}

func (b *builtinFtsMatchWordSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	// Reject executing match against in TiDB side.
	return 0, false, errors.Errorf("cannot use 'FTS_MATCH_WORD()' outside of fulltext index")
}

func (c *ftsMysqlMatchAgainstFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argAgainst := args[0]
	argAgainstConstant, ok := argAgainst.(*Constant)
	if !ok {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("match against a non-constant string")
	}
	if argAgainstConstant.Value.Kind() != types.KindString && !argAgainstConstant.Value.IsNull() {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("match against a non-string constant")
	}

	argsMatch := args[1:]
	for _, arg := range argsMatch {
		_, ok := arg.(*Column)
		if !ok {
			return nil, ErrNotSupportedYet.GenWithStackByArgs("not matching a column")
		}
	}

	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETString)
	for _, arg := range argsMatch {
		if arg.GetType(ctx.GetEvalCtx()).EvalType() != types.ETString {
			return nil, ErrNotSupportedYet.GenWithStackByArgs("Doesn't support match search on a non-string column without fulltext index")
		}
		argTps = append(argTps, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}

	sig := &builtinFtsMysqlMatchAgainstSig{baseBuiltinFunc: bf}
	sig.setPbCode(tipb.ScalarFuncSig_FTSMatchExpression)
	return sig, nil
}

func (b *builtinFtsMysqlMatchAgainstSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	if b.localEvalInfo == nil {
		// args[0] is validated to be a *Constant by getFunction; guard
		// defensively since the sig may be reconstructed via the distsql path
		// without that check. Reading Value directly is only safe here because
		// the non-local path never resolves parameter markers.
		if constArg, ok := b.args[0].(*Constant); ok && constArg.Value.IsNull() {
			return 0, true, nil
		}
		return 0, false, errors.Errorf("cannot use 'MATCH ... AGAINST' outside of fulltext index")
	}
	if !FTSModifierSupportedByLocalNoScore(b.modifier) {
		return 0, false, errors.Errorf("local 'MATCH ... AGAINST' only supports IN BOOLEAN MODE")
	}

	// Evaluate rather than reading the constant's Value: a prepared statement
	// passes the search string through a parameter marker, whose value is not
	// held in Value at all.
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
	doc, err := fulltext.BuildDocument(columns, plan.analyzer)
	if err != nil {
		return 0, false, err
	}
	if plan.query.Match(doc) {
		return 1, false, nil
	}
	return 0, false, nil
}

// getOrBuildLocalNoScorePlan returns the compiled query for search, rebuilding
// it if the search string changed. A prepared statement re-executed with a new
// parameter reuses the same signature, so the cached plan is keyed by the
// search text rather than assumed valid for the lifetime of the sig.
func (b *builtinFtsMysqlMatchAgainstSig) getOrBuildLocalNoScorePlan(search string) (*ftsLocalEvalPlan, error) {
	b.localPlanMu.Lock()
	defer b.localPlanMu.Unlock()

	if b.localPlan != nil && b.localPlan.search == search {
		return b.localPlan, nil
	}

	config := b.localEvalInfo.AnalyzerConfig
	analyzer, err := fulltext.GetAnalyzer(config)
	if err != nil {
		return nil, err
	}
	query, err := fulltext.CompileBooleanQuery(search, config)
	if err != nil {
		return nil, err
	}
	b.localPlan = &ftsLocalEvalPlan{search: search, query: query, analyzer: analyzer}
	return b.localPlan, nil
}

// evalLocalMatchColumns materialises the MATCH column values for one row. A
// NULL column contributes no tokens rather than making the whole predicate
// NULL, matching MySQL's treatment of NULL columns in a fulltext match.
func (b *builtinFtsMysqlMatchAgainstSig) evalLocalMatchColumns(ctx EvalContext, row chunk.Row) ([]fulltext.ColumnInput, error) {
	columns := make([]fulltext.ColumnInput, 0, len(b.args)-1)
	for _, arg := range b.args[1:] {
		text, isNull, err := arg.EvalString(ctx, row)
		if err != nil {
			return nil, err
		}
		if isNull {
			columns = append(columns, fulltext.ColumnInput{IsNull: true})
			continue
		}
		columns = append(columns, fulltext.ColumnInput{Text: text})
	}
	return columns, nil
}
