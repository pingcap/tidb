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
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

var (
	_ functionClass = &ftsTokenizeFunctionClass{}
	_ builtinFunc   = &builtinFTSTokenizeSig{}
)

// FTS_TOKENIZE(text, parser, min_token_size, max_token_size, enable_stopword)
// analyzes text and returns its distinct tokens as a JSON array.
//
// The analyzer configuration is taken from the arguments rather than from
// session variables, which is what makes the function deterministic and
// therefore usable in a generated column. A FULLTEXT index on the classic
// kernel is built as a multi-valued index over this expression, so the
// argument literals recorded in the schema are the index's analyzer snapshot:
// changing the session's innodb_ft_* variables cannot make later rows tokenize
// differently from earlier ones.
//
// The tokenizer algorithm itself must therefore stay stable across versions.
// Changing how a given (text, config) pair tokenizes would silently invalidate
// existing index data; such a change needs a new parser type instead.
type ftsTokenizeFunctionClass struct {
	baseFunctionClass
}

type builtinFTSTokenizeSig struct {
	baseBuiltinFunc
}

func (b *builtinFTSTokenizeSig) Clone() builtinFunc {
	newSig := &builtinFTSTokenizeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *ftsTokenizeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	// Reject a non-constant configuration up front: the value would otherwise
	// be free to differ between the write that populates an index entry and
	// the read that consults it.
	for i, arg := range args[1:] {
		if _, ok := arg.(*Constant); !ok {
			return nil, ErrNotSupportedYet.GenWithStackByArgs(
				"non-constant " + ftsTokenizeArgName(i+1) + " argument to FTS_TOKENIZE()")
		}
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson,
		types.ETString, types.ETString, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	// Validate the configuration once at build time so a bad parser name or
	// token-size bound is an error on CREATE INDEX rather than on every row.
	if _, err := ftsTokenizeConfigFromArgs(ctx.GetEvalCtx(), bf.args); err != nil {
		return nil, err
	}
	sig := &builtinFTSTokenizeSig{bf}
	return sig, nil
}

func ftsTokenizeArgName(idx int) string {
	switch idx {
	case 1:
		return "parser"
	case 2:
		return "min_token_size"
	case 3:
		return "max_token_size"
	default:
		return "enable_stopword"
	}
}

// ftsTokenizeConfigFromArgs reads the analyzer configuration out of the
// constant argument list. Callers outside evaluation use it to recover the
// snapshot an index was built with.
func ftsTokenizeConfigFromArgs(ctx EvalContext, args []Expression) (fulltext.AnalyzerConfig, error) {
	parserName, isNull, err := args[1].EvalString(ctx, chunk.Row{})
	if err != nil {
		return fulltext.AnalyzerConfig{}, err
	}
	if isNull {
		return fulltext.AnalyzerConfig{}, ErrNotSupportedYet.GenWithStackByArgs("NULL parser argument to FTS_TOKENIZE()")
	}
	parserType := model.GetFullTextParserTypeBySQLName(parserName)
	if parserType == model.FullTextParserTypeInvalid {
		return fulltext.AnalyzerConfig{}, ErrNotSupportedYet.GenWithStackByArgs(
			"FTS_TOKENIZE() with unknown parser '" + parserName + "'")
	}
	intArg := func(idx int) (int, error) {
		v, isNull, err := args[idx].EvalInt(ctx, chunk.Row{})
		if err != nil {
			return 0, err
		}
		if isNull {
			return 0, ErrNotSupportedYet.GenWithStackByArgs(
				"NULL " + ftsTokenizeArgName(idx) + " argument to FTS_TOKENIZE()")
		}
		return int(v), nil
	}
	minTokenSize, err := intArg(2)
	if err != nil {
		return fulltext.AnalyzerConfig{}, err
	}
	maxTokenSize, err := intArg(3)
	if err != nil {
		return fulltext.AnalyzerConfig{}, err
	}
	enableStopword, err := intArg(4)
	if err != nil {
		return fulltext.AnalyzerConfig{}, err
	}
	config := fulltext.AnalyzerConfig{
		ParserType:             parserType,
		InnodbFtMinTokenSize:   minTokenSize,
		InnodbFtMaxTokenSize:   maxTokenSize,
		InnodbFtEnableStopword: enableStopword != 0,
	}
	if parserType == model.FullTextParserTypeNgramV1 {
		// The ngram parser sizes its grams from min_token_size; the max bound
		// is unused, so keep the two in one field rather than adding a sixth
		// argument that only one parser reads.
		config.NgramTokenSize = minTokenSize
	}
	if _, err := fulltext.GetAnalyzer(config); err != nil {
		return fulltext.AnalyzerConfig{}, err
	}
	return config, nil
}

// FTSTokenizeAnalyzerConfig returns the analyzer configuration frozen into an
// FTS_TOKENIZE call, and the text expression it analyzes. It lets the planner
// recover the snapshot a FULLTEXT index was built with, so a MATCH can be
// matched against that index only when the two agree.
func FTSTokenizeAnalyzerConfig(ctx EvalContext, expr Expression) (Expression, fulltext.AnalyzerConfig, bool) {
	sf, ok := expr.(*ScalarFunction)
	if !ok || sf.FuncName.L != ast.FTSTokenize {
		return nil, fulltext.AnalyzerConfig{}, false
	}
	args := sf.GetArgs()
	if len(args) != 5 {
		return nil, fulltext.AnalyzerConfig{}, false
	}
	config, err := ftsTokenizeConfigFromArgs(ctx, args)
	if err != nil {
		return nil, fulltext.AnalyzerConfig{}, false
	}
	return args[0], config, true
}

func (b *builtinFTSTokenizeSig) evalJSON(ctx EvalContext, row chunk.Row) (types.BinaryJSON, bool, error) {
	text, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil {
		return types.BinaryJSON{}, false, err
	}
	if isNull {
		// A NULL input produces JSON null rather than an empty array. The
		// multi-valued index skips empty arrays entirely but records a null,
		// so this keeps a row with a NULL text column present in the index.
		return types.CreateBinaryJSON(nil), false, nil
	}
	config, err := ftsTokenizeConfigFromArgs(ctx, b.args)
	if err != nil {
		return types.BinaryJSON{}, false, err
	}
	analyzer, err := fulltext.GetAnalyzer(config)
	if err != nil {
		return types.BinaryJSON{}, false, err
	}
	tokens, err := analyzer.Analyze(text)
	if err != nil {
		return types.BinaryJSON{}, false, err
	}
	// Duplicates are dropped here as well as by the index builder: the value
	// is user-visible through the generated column, so it should not depend on
	// whether an index happens to consume it.
	seen := make(map[string]struct{}, len(tokens))
	values := make([]any, 0, len(tokens))
	for _, token := range tokens {
		if _, dup := seen[token.Text]; dup {
			continue
		}
		seen[token.Text] = struct{}{}
		values = append(values, token.Text)
	}
	return types.CreateBinaryJSON(values), false, nil
}

// BuildFTSTokenizeExpr builds the AST for FTS_TOKENIZE(<colExpr>, ...) with the
// supplied analyzer configuration written in as literals. DDL uses it to turn a
// FULLTEXT index definition into an expression index whose schema records the
// exact analyzer that produced the index data.
func BuildFTSTokenizeExpr(colExpr ast.ExprNode, config fulltext.AnalyzerConfig) *ast.FuncCallExpr {
	enableStopword := int64(0)
	if config.InnodbFtEnableStopword {
		enableStopword = 1
	}
	minTokenSize := config.InnodbFtMinTokenSize
	if config.ParserType == model.FullTextParserTypeNgramV1 {
		minTokenSize = config.NgramTokenSize
	}
	return &ast.FuncCallExpr{
		FnName: ast.NewCIStr(ast.FTSTokenize),
		Args: []ast.ExprNode{
			colExpr,
			ast.NewValueExpr(config.ParserType.SQLName(), "", ""),
			ast.NewValueExpr(int64(minTokenSize), "", ""),
			ast.NewValueExpr(int64(config.InnodbFtMaxTokenSize), "", ""),
			ast.NewValueExpr(enableStopword, "", ""),
		},
	}
}

// ftsTokenizeMaxTokenBytes bounds the CHAR(n) element width of the multi-valued
// index built over FTS_TOKENIZE. Token length is filtered in characters while
// the index element width is in characters too, so the configured maximum is
// the right bound; it is clamped to the analyzer's own ceiling.
func ftsTokenizeMaxTokenBytes(config fulltext.AnalyzerConfig) int {
	if config.ParserType == model.FullTextParserTypeNgramV1 {
		return max(config.NgramTokenSize, 1)
	}
	return max(config.InnodbFtMaxTokenSize, 1)
}
