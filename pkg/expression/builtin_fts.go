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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
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
	cloned.AnalyzerConfig.Stopwords = slices.Clone(info.AnalyzerConfig.Stopwords)
	return &cloned
}

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

func (b *builtinFtsMatchPhraseSig) Clone() builtinFunc {
	newSig := &builtinFtsMatchPhraseSig{}
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

// sameFTSState compares semantic state that is not represented by SQL arguments.
func (b *builtinFtsMysqlMatchAgainstSig) sameFTSState(other *builtinFtsMysqlMatchAgainstSig) bool {
	if b.localEvalInfo == nil || other.localEvalInfo == nil {
		return b.localEvalInfo == other.localEvalInfo
	}
	return b.modifier == other.modifier && b.localEvalInfo.AnalyzerConfig.Equal(other.localEvalInfo.AnalyzerConfig) &&
		b.localEvalInfo.MatchNothing == other.localEvalInfo.MatchNothing &&
		b.localEvalInfo.SelectivityTerm == other.localEvalInfo.SelectivityTerm
}

func (b *builtinFtsMysqlMatchAgainstSig) equal(ctx EvalContext, other builtinFunc) bool {
	o, ok := other.(*builtinFtsMysqlMatchAgainstSig)
	return ok && b.sameFTSState(o) && b.baseBuiltinFunc.equal(ctx, other)
}

// appendFTSStateHash keeps native/local execution and analyzer configurations
// distinct during expression deduplication. The compiled query cache is excluded.
func (b *builtinFtsMysqlMatchAgainstSig) appendFTSStateHash(dst []byte) []byte {
	if b.localEvalInfo == nil {
		return dst
	}
	dst = codec.EncodeInt(dst, int64(b.modifier))
	dst = append(dst, 1)
	info := b.localEvalInfo
	config := info.AnalyzerConfig
	dst = codec.EncodeCompactBytes(dst, []byte(config.ParserType))
	dst = codec.EncodeInt(dst, int64(config.InnodbFtMinTokenSize))
	dst = codec.EncodeInt(dst, int64(config.InnodbFtMaxTokenSize))
	dst = codec.EncodeInt(dst, int64(config.NgramTokenSize))
	if config.InnodbFtEnableStopword {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = codec.EncodeInt(dst, int64(len(config.Stopwords)))
	for _, word := range config.Stopwords {
		dst = codec.EncodeCompactBytes(dst, []byte(word))
	}
	if info.MatchNothing {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	return codec.EncodeCompactBytes(dst, []byte(info.SelectivityTerm))
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
		if _, ok := arg.(*Column); !ok {
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

// SetFTSMysqlMatchAgainstModifier sets the modifier for the internal `MATCH ... AGAINST` builtin signature.
// It is expected to be called by planner right after building the scalar function.
func SetFTSMysqlMatchAgainstModifier(sf *ScalarFunction, modifier ast.FulltextSearchModifier) error {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	sig.SetModifier(modifier)
	sf.CleanHashCode()
	return nil
}

// SetFTSMysqlMatchAgainstLocalEvalInfo authorises local no-score evaluation.
func SetFTSMysqlMatchAgainstLocalEvalInfo(sf *ScalarFunction, info *FTSLocalEvalInfo) error {
	sig, ok := sf.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, sf.Function)
	}
	sig.localEvalInfo = info.Clone()
	sf.CleanHashCode()
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

	argAgainst := args[0]
	argAgainstConstant, ok := argAgainst.(*Constant)
	if !ok {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("match against a non-constant string")
	}
	if argAgainstConstant.Value.Kind() != types.KindString && !argAgainstConstant.Value.IsNull() && argAgainstConstant.ParamMarker == nil {
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
		if constArg, ok := b.args[0].(*Constant); ok && constArg.Value.IsNull() {
			return 0, false, nil
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
		if _, ok := arg.(*Column); !ok {
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
