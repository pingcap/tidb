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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

var (
	_ functionClass = &ftsLikePatternFunctionClass{}
	_ builtinFunc   = &builtinFTSLikePatternSig{}
)

// ftsLikePatternFunctionClass is the internal fts_like_pattern(against,
// modifier, kind, idx) function. It returns the ILIKE pattern ('%word%', with
// LIKE wildcards escaped) that the MATCH...AGAINST LIKE fallback uses for the
// idx-th term of the given kind (see ftsLikeTermKind) in the search string
// `against`. BuildFTSToILikeExpressionForParam wraps it in a deferred constant
// over an `AGAINST(?)` parameter, so the patterns of a cached plan follow the
// parameter's value on each execution.
type ftsLikePatternFunctionClass struct {
	baseFunctionClass
}

func (c *ftsLikePatternFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(mysql.MaxFieldVarCharLength)
	return &builtinFTSLikePatternSig{baseBuiltinFunc: bf}, nil
}

type builtinFTSLikePatternSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.

	// patternCache holds the patterns of the search string seen in the
	// current execution; builtinFuncCache is safe to share across sessions.
	patternCache builtinFuncCache[*ftsLikePatterns]
}

// ftsLikePatterns are the ILIKE patterns of one search string, by term kind.
type ftsLikePatterns struct {
	searchText string
	modifier   ast.FulltextSearchModifier
	byKind     [ftsLikeTermNatural + 1][]string
}

func newFTSLikePatterns(searchText string, modifier ast.FulltextSearchModifier) *ftsLikePatterns {
	p := &ftsLikePatterns{searchText: searchText, modifier: modifier}
	terms := ftsLikeTermsOf(searchText, modifier)
	for kind := ftsLikeTermRequired; kind <= ftsLikeTermNatural; kind++ {
		words := terms.termsOfKind(kind)
		patterns := make([]string, len(words))
		for i, w := range words {
			patterns[i] = ftsLikePatternOf(w)
		}
		p.byKind[kind] = patterns
	}
	return p
}

func (b *builtinFTSLikePatternSig) Clone() builtinFunc {
	newSig := &builtinFTSLikePatternSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinFTSLikePatternSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	searchText, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	var vals [3]int64
	for i := range vals {
		v, isNull, err := b.args[i+1].EvalInt(ctx, row)
		if isNull || err != nil {
			return "", isNull, err
		}
		vals[i] = v
	}
	modifier, kind, idx := ast.FulltextSearchModifier(vals[0]), ftsLikeTermKind(vals[1]), vals[2]
	if kind < ftsLikeTermRequired || kind > ftsLikeTermNatural {
		return "", true, errors.Errorf("%s: unknown term kind %d", ast.FTSLikePattern, kind)
	}

	patterns, err := b.patternCache.getOrInitCache(ctx, func() (*ftsLikePatterns, error) {
		return newFTSLikePatterns(searchText, modifier), nil
	})
	if err != nil {
		return "", true, err
	}
	// The cache is per execution, and within one execution the search string
	// is a single parameter value; recompute defensively if it is not.
	if patterns.searchText != searchText || patterns.modifier != modifier {
		patterns = newFTSLikePatterns(searchText, modifier)
	}
	ofKind := patterns.byKind[kind]
	if idx < 0 || idx >= int64(len(ofKind)) {
		// The plan cache keys plans by FTSLikeFallbackShape, so a cached plan is
		// only reused for search strings with the same number of terms of each
		// kind. Reaching this means that guarantee was broken; fail rather than
		// silently match the wrong term.
		return "", true, errors.Errorf("%s: search string has no term %d of kind %d", ast.FTSLikePattern, idx, kind)
	}
	return ofKind[idx], false, nil
}

func (b *builtinFTSLikePatternSig) vectorized() bool {
	return true
}

func (b *builtinFTSLikePatternSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveString(n)
	for i := range n {
		s, isNull, err := b.evalString(ctx, input.GetRow(i))
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
			continue
		}
		result.AppendString(s)
	}
	return nil
}

// deferredFTSLikePattern returns the ILIKE pattern constant for the idx-th
// term of the given kind of an `AGAINST(?)` parameter: its value is the
// pattern for the parameter's current value (term), and its DeferredExpr
// recomputes it from the parameter on each execution of a cached plan.
func deferredFTSLikePattern(ctx BuildContext, against *Constant, modifier ast.FulltextSearchModifier, kind ftsLikeTermKind, idx int, term string) (Expression, error) {
	intConst := func(v int64) *Constant {
		return &Constant{Value: types.NewIntDatum(v), RetType: types.NewFieldType(mysql.TypeLonglong)}
	}
	fn, err := NewFunction(ctx, ast.FTSLikePattern, types.NewFieldType(mysql.TypeVarchar),
		against, intConst(int64(modifier)), intConst(int64(kind)), intConst(int64(idx)))
	if err != nil {
		return nil, err
	}
	return &Constant{
		Value:        types.NewStringDatum(ftsLikePatternOf(term)),
		RetType:      types.NewFieldType(mysql.TypeVarchar),
		DeferredExpr: fn,
	}, nil
}
