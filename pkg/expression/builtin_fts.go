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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

var _ functionClass = &ftsMysqlMatchAgainstFunctionClass{}
var _ builtinFunc = &builtinFtsMysqlMatchAgainstSig{}

type ftsMysqlMatchAgainstFunctionClass struct {
	baseFunctionClass
}

type builtinFtsMysqlMatchAgainstSig struct {
	baseBuiltinFunc
	modifier      ast.FulltextSearchModifier
	localEvalInfo *FTSLocalEvalInfo

	// A prepared statement can reuse this signature with different search
	// strings. Key the compiled plan by the actual string and protect it when
	// executor workers share the signature.
	localPlanMu sync.Mutex
	localPlan   *ftsLocalEvalPlan
}

// FTSLocalEvalInfo is planner-validated metadata authorising local no-score
// MATCH ... AGAINST evaluation. The planner only attaches it in direct boolean
// predicate positions, so relevance-score positions never receive a 0/1 value.
type FTSLocalEvalInfo struct {
	AnalyzerConfig  fulltext.AnalyzerConfig
	SelectivityTerm string
	MatchNothing    bool
}

// Clone returns an independent copy of the local evaluation metadata.
func (info *FTSLocalEvalInfo) Clone() *FTSLocalEvalInfo {
	if info == nil {
		return nil
	}
	cloned := *info
	return &cloned
}

type ftsLocalEvalPlan struct {
	search   string
	query    *fulltext.Query
	analyzer fulltext.Analyzer
}

func (b *builtinFtsMysqlMatchAgainstSig) Clone() builtinFunc {
	newSig := &builtinFtsMysqlMatchAgainstSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.modifier = b.modifier
	newSig.localEvalInfo = b.localEvalInfo.Clone()
	return newSig
}

// SetFTSMysqlMatchAgainstModifier sets the SQL modifier on the internal
// MATCH ... AGAINST builtin immediately after planner construction.
func SetFTSMysqlMatchAgainstModifier(sf *ScalarFunction, modifier ast.FulltextSearchModifier) error {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	sig.modifier = modifier
	return nil
}

// SetFTSMysqlMatchAgainstLocalEvalInfo authorises local no-score evaluation.
func SetFTSMysqlMatchAgainstLocalEvalInfo(sf *ScalarFunction, info *FTSLocalEvalInfo) error {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	sig.localEvalInfo = info.Clone()
	return nil
}

// FTSMysqlMatchAgainstLocalEvalInfo returns attached local-evaluation metadata.
func FTSMysqlMatchAgainstLocalEvalInfo(sf *ScalarFunction) (*FTSLocalEvalInfo, bool) {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok || sig.localEvalInfo == nil {
		return nil, false
	}
	return sig.localEvalInfo, true
}

// FTSModifierSupportedByLocalNoScore reports whether local boolean matching can
// preserve the SQL modifier semantics. Natural-language relevance and query
// expansion are deliberately excluded.
func FTSModifierSupportedByLocalNoScore(modifier ast.FulltextSearchModifier) bool {
	return modifier.IsBooleanMode() && !modifier.WithQueryExpansion()
}

// CompileFTSMysqlMatchAgainstLocalQuery compiles a stable search argument at
// plan time, surfacing syntax errors even for empty inputs or short-circuits.
func CompileFTSMysqlMatchAgainstLocalQuery(ctx EvalContext, sf *ScalarFunction, config fulltext.AnalyzerConfig) (*fulltext.Query, error) {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return nil, errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	if !FTSModifierSupportedByLocalNoScore(sig.modifier) {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("local MATCH ... AGAINST outside of IN BOOLEAN MODE")
	}
	search, isNull, err := sig.args[0].EvalString(ctx, chunk.Row{})
	if err != nil || isNull {
		return nil, err
	}
	return fulltext.CompileBooleanQuery(search, config)
}

func (c *ftsMysqlMatchAgainstFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
	return &builtinFtsMysqlMatchAgainstSig{baseBuiltinFunc: bf}, nil
}

func (b *builtinFtsMysqlMatchAgainstSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	if b.localEvalInfo == nil {
		if constArg, ok := b.args[0].(*Constant); ok && constArg.Value.IsNull() {
			return 0, true, nil
		}
		return 0, false, errors.Errorf("cannot use 'MATCH ... AGAINST' outside of fulltext index")
	}
	if !FTSModifierSupportedByLocalNoScore(b.modifier) {
		return 0, false, errors.Errorf("local 'MATCH ... AGAINST' only supports IN BOOLEAN MODE")
	}

	search, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return 0, isNull, err
	}
	if b.localEvalInfo.MatchNothing {
		return 0, false, nil
	}
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

func (b *builtinFtsMysqlMatchAgainstSig) getOrBuildLocalNoScorePlan(search string) (*ftsLocalEvalPlan, error) {
	b.localPlanMu.Lock()
	defer b.localPlanMu.Unlock()
	if b.localPlan != nil && b.localPlan.search == search {
		return b.localPlan, nil
	}
	analyzer, err := fulltext.GetAnalyzer(b.localEvalInfo.AnalyzerConfig)
	if err != nil {
		return nil, err
	}
	query, err := fulltext.CompileBooleanQuery(search, b.localEvalInfo.AnalyzerConfig)
	if err != nil {
		return nil, err
	}
	b.localPlan = &ftsLocalEvalPlan{search: search, query: query, analyzer: analyzer}
	return b.localPlan, nil
}

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
