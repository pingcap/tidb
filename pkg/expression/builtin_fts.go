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
	"slices"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &ftsMatchWordFunctionClass{}
	_ functionClass = &ftsMatchPrefixFunctionClass{}
	_ functionClass = &ftsMatchPhraseFunctionClass{}
	_ functionClass = &ftsMysqlMatchAgainstFunctionClass{}
)

var (
	_ builtinFunc = &builtinFtsMatchWordSig{}
	_ builtinFunc = &builtinFtsMatchPrefixSig{}
	_ builtinFunc = &builtinFtsMatchPhraseSig{}
	_ builtinFunc = &builtinFtsMysqlMatchAgainstSig{}
)

type ftsMatchWordFunctionClass struct {
	baseFunctionClass
}

type builtinFtsMatchWordSig struct {
	baseBuiltinFunc
}

type ftsMatchPhraseFunctionClass struct {
	baseFunctionClass
}

type builtinFtsMatchPhraseSig struct {
	baseBuiltinFunc
}

type ftsMysqlMatchAgainstFunctionClass struct {
	baseFunctionClass
}

// FTSMatchUsage describes where an FTSMysqlMatchAgainst expression is used.
type FTSMatchUsage int

const (
	// FTSMatchUsageScalar means the expression may require scalar relevance
	// semantics and must not be locally evaluated as a no-score filter.
	FTSMatchUsageScalar FTSMatchUsage = iota
	// FTSMatchUsageDirectFilter means the planner saw this expression in a
	// direct boolean filter context.
	FTSMatchUsageDirectFilter
)

// FTSLocalEvalInfo is planner-validated metadata for local no-score
// MATCH ... AGAINST evaluation.
type FTSLocalEvalInfo struct {
	TableID        int64
	IndexID        int64
	ParserType     model.FullTextParserType
	AnalyzerConfig fulltext.AnalyzerConfig

	ColumnIDs       []int64
	ColumnUniqueIDs []int64
	ColumnOffsets   []int

	NoScore bool
}

// Clone returns a deep copy of the local evaluation metadata.
func (info *FTSLocalEvalInfo) Clone() *FTSLocalEvalInfo {
	if info == nil {
		return nil
	}
	cloned := *info
	cloned.AnalyzerConfig.Stopwords = slices.Clone(info.AnalyzerConfig.Stopwords)
	cloned.ColumnIDs = slices.Clone(info.ColumnIDs)
	cloned.ColumnUniqueIDs = slices.Clone(info.ColumnUniqueIDs)
	cloned.ColumnOffsets = slices.Clone(info.ColumnOffsets)
	return &cloned
}

func (info *FTSLocalEvalInfo) normalizedClone() *FTSLocalEvalInfo {
	cloned := info.Clone()
	if cloned == nil {
		return nil
	}
	if cloned.AnalyzerConfig.ParserType == "" {
		cloned.AnalyzerConfig.ParserType = cloned.ParserType
	}
	if cloned.ParserType == "" {
		cloned.ParserType = cloned.AnalyzerConfig.ParserType
	}
	return cloned
}

type ftsLocalEvalPlan struct {
	search   string
	query    *fulltext.Query
	analyzer fulltext.Analyzer
}

type builtinFtsMysqlMatchAgainstSig struct {
	baseBuiltinFunc
	modifier      ast.FulltextSearchModifier
	usage         FTSMatchUsage
	localEvalInfo *FTSLocalEvalInfo

	localPlanMu sync.Mutex
	localPlan   *ftsLocalEvalPlan
}

func (b *builtinFtsMatchWordSig) Clone() builtinFunc {
	newSig := &builtinFtsMatchWordSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinFtsMatchPhraseSig) Clone() builtinFunc {
	newSig := &builtinFtsMatchPhraseSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinFtsMysqlMatchAgainstSig) Clone() builtinFunc {
	newSig := &builtinFtsMysqlMatchAgainstSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.modifier = b.modifier
	newSig.usage = b.usage
	newSig.localEvalInfo = b.localEvalInfo.Clone()
	return newSig
}

func (c *ftsMatchWordFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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

	sig := &builtinFtsMatchWordSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_FTSMatchWord)
	return sig, nil
}

func (b *builtinFtsMatchWordSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	// Matching NULL returns 0.
	if b.args[0].(*Constant).Value.IsNull() {
		return 0, false, nil
	}
	// Reject executing match against in TiDB side
	return 0, false, errors.Errorf("cannot use 'FTS_MATCH_WORD()' outside of fulltext index")
}

func (b *builtinFtsMatchPhraseSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	// Matching NULL returns 0.
	if b.args[0].(*Constant).Value.IsNull() {
		return 0, false, nil
	}
	// Reject executing match against in TiDB side
	return 0, false, errors.Errorf("cannot use 'FTS_MATCH_PHRASE()' outside of fulltext index")
}

func (b *builtinFtsMysqlMatchAgainstSig) SetModifier(modifier ast.FulltextSearchModifier) {
	b.modifier = modifier
}

func (b *builtinFtsMysqlMatchAgainstSig) SetUsage(usage FTSMatchUsage) {
	b.usage = usage
}

func (b *builtinFtsMysqlMatchAgainstSig) SetLocalEvalInfo(info *FTSLocalEvalInfo) {
	b.localPlanMu.Lock()
	defer b.localPlanMu.Unlock()
	b.localEvalInfo = info.normalizedClone()
	b.localPlan = nil
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

// SetFTSMysqlMatchAgainstUsage sets the planner-observed usage of the
// internal MATCH ... AGAINST builtin signature.
func SetFTSMysqlMatchAgainstUsage(sf *ScalarFunction, usage FTSMatchUsage) error {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	sig.SetUsage(usage)
	return nil
}

// FTSMysqlMatchAgainstUsage returns the planner-observed usage of the internal
// MATCH ... AGAINST builtin signature.
func FTSMysqlMatchAgainstUsage(sf *ScalarFunction) (FTSMatchUsage, bool) {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return FTSMatchUsageScalar, false
	}
	return sig.usage, true
}

// SetFTSMysqlMatchAgainstLocalEvalInfo attaches planner-validated local
// no-score evaluation metadata to MATCH ... AGAINST.
func SetFTSMysqlMatchAgainstLocalEvalInfo(sf *ScalarFunction, info *FTSLocalEvalInfo) error {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	sig.SetLocalEvalInfo(info)
	return nil
}

// FTSMysqlMatchAgainstLocalEvalInfo returns the local no-score evaluation
// metadata attached to MATCH ... AGAINST, if any.
func FTSMysqlMatchAgainstLocalEvalInfo(sf *ScalarFunction) (*FTSLocalEvalInfo, bool) {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok || sig.localEvalInfo == nil {
		return nil, false
	}
	return sig.localEvalInfo.Clone(), true
}

// FTSModifierSupportedByLocalNoScore reports whether the modifier can be
// locally evaluated by the no-score MATCH ... AGAINST path.
func FTSModifierSupportedByLocalNoScore(modifier ast.FulltextSearchModifier) bool {
	return modifier.IsBooleanMode() && !modifier.WithQueryExpansion()
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
	search, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return 0, isNull, err
	}
	if b.localEvalInfo == nil {
		return 0, false, errors.Errorf("cannot use 'MATCH ... AGAINST' outside of fulltext index")
	}
	if !b.localEvalInfo.NoScore {
		return 0, false, errors.Errorf("cannot use 'MATCH ... AGAINST' without no-score local evaluation metadata")
	}
	if !FTSModifierSupportedByLocalNoScore(b.modifier) {
		return 0, false, errors.Errorf("local MATCH ... AGAINST only supports IN BOOLEAN MODE")
	}

	plan, err := b.getOrBuildLocalNoScorePlan(search)
	if err != nil {
		return 0, false, err
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

	config := b.localEvalInfo.AnalyzerConfig
	analyzer, err := fulltext.GetAnalyzer(config)
	if err != nil {
		return nil, err
	}
	query, err := fulltext.CompileBooleanQuery(search, config)
	if err != nil {
		return nil, err
	}
	plan := &ftsLocalEvalPlan{
		search:   search,
		query:    query,
		analyzer: analyzer,
	}
	b.localPlan = plan
	return plan, nil
}

func (b *builtinFtsMysqlMatchAgainstSig) evalLocalMatchColumns(ctx EvalContext, row chunk.Row) ([]fulltext.ColumnInput, error) {
	if len(b.localEvalInfo.ColumnOffsets) > 0 {
		columns := make([]fulltext.ColumnInput, 0, len(b.localEvalInfo.ColumnOffsets))
		for _, offset := range b.localEvalInfo.ColumnOffsets {
			if row.IsNull(offset) {
				columns = append(columns, fulltext.ColumnInput{IsNull: true})
				continue
			}
			columns = append(columns, fulltext.ColumnInput{Text: row.GetString(offset)})
		}
		return columns, nil
	}

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

func (b *builtinFtsMysqlMatchAgainstSig) hasLocalEvalInfo() bool {
	return b.localEvalInfo != nil
}

type ftsMatchPrefixFunctionClass struct {
	baseFunctionClass
}

type builtinFtsMatchPrefixSig struct {
	baseBuiltinFunc
}

func (b *builtinFtsMatchPrefixSig) Clone() builtinFunc {
	newSig := &builtinFtsMatchPrefixSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *ftsMatchPrefixFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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

	sig := &builtinFtsMatchPrefixSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_FTSMatchPrefix)
	return sig, nil
}

func (b *builtinFtsMatchPrefixSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	// Matching NULL returns 0.
	if b.args[0].(*Constant).Value.IsNull() {
		return 0, false, nil
	}
	// Reject executing match against in TiDB side.
	return 0, false, errors.Errorf("cannot use 'FTS_MATCH_PREFIX()' outside of fulltext index")
}

func (c *ftsMatchPhraseFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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

	sig := &builtinFtsMatchPhraseSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_FTSMatchPhrase)
	return sig, nil
}
