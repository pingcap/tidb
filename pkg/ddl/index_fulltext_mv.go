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

package ddl

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// buildFullTextMVIndexSpec rewrites a FULLTEXT index definition into an
// expression-index specification over the tokenized form of the indexed column:
//
//	FULLTEXT INDEX idx (body)
//	  =>  INDEX idx ((CAST(FTS_TOKENIZE(`body`, 'STANDARD', 3, 84, 1) AS CHAR(84) ARRAY)))
//
// On the classic kernel there is no columnar engine to hold a full-text index,
// so it is materialised as an ordinary multi-valued index in KV. The caller
// must leave IndexInfo.FullTextInfo nil for that reason: setting it would make
// IsColumnarIndex report true and suppress the KV index entirely.
//
// The analyzer configuration is resolved once here and written into the
// expression as literals. That makes the schema itself the analyzer snapshot:
// the generated column is re-evaluated on every write, so reading the config
// from session variables instead would let a later SET reshape the token stream
// and silently disagree with the rows already indexed.
func buildFullTextMVIndexSpec(
	analyzerDefaults fulltext.AnalyzerConfig,
	indexPartSpecifications []*ast.IndexPartSpecification,
	indexOption *ast.IndexOption,
	tblInfo *model.TableInfo,
) ([]*ast.IndexPartSpecification, error) {
	if len(indexPartSpecifications) != 1 {
		return nil, dbterror.ErrUnsupportedIndexType.GenWithStack("FULLTEXT index only supports one column")
	}
	idxPart := indexPartSpecifications[0]
	if idxPart.Column == nil {
		return nil, dbterror.ErrUnsupportedIndexType.GenWithStack("FULLTEXT index only supports one column")
	}
	if idxPart.Length != types.UnspecifiedLength {
		return nil, dbterror.ErrUnsupportedIndexType.GenWithStack("FULLTEXT index does not support prefix length")
	}
	if idxPart.Desc {
		return nil, dbterror.ErrUnsupportedIndexType.GenWithStack("FULLTEXT index does not support DESC order")
	}

	colInfo := findColumnByName(idxPart.Column.Name.L, tblInfo)
	if colInfo == nil {
		return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(idxPart.Column.Name, tblInfo.Name)
	}
	if !types.IsString(colInfo.FieldType.GetType()) {
		return nil, dbterror.ErrUnsupportedIndexType.GenWithStack(
			"FULLTEXT index requires a string column, but %s is %s", colInfo.Name, colInfo.FieldType.String())
	}

	parserType := model.FullTextParserTypeStandardV1
	if indexOption != nil && indexOption.ParserName.L != "" {
		parserType = model.GetFullTextParserTypeBySQLName(indexOption.ParserName.L)
		if parserType == model.FullTextParserTypeInvalid {
			return nil, dbterror.ErrUnsupportedIndexType.GenWithStack(
				"FULLTEXT index with unknown parser '%s'", indexOption.ParserName.O)
		}
	}
	if parserType == model.FullTextParserTypeMultilingualV1 {
		// MULTILINGUAL_V1 has no local analyzer implementation; accepting it
		// here would build an index whose contents no query could reproduce.
		return nil, dbterror.ErrUnsupportedIndexType.GenWithStack(
			"FULLTEXT index with the MULTILINGUAL parser is not supported without a columnar engine")
	}

	config := analyzerDefaults
	config.ParserType = parserType
	if config.ParserType == model.FullTextParserTypeNgramV1 && config.NgramTokenSize <= 0 {
		config.NgramTokenSize = 2
	}
	if _, err := fulltext.GetAnalyzer(config); err != nil {
		return nil, errors.Trace(err)
	}

	exprStr, err := buildFullTextTokenizeExprString(colInfo.Name, config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	expr, err := parseIndexExpr(exprStr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return []*ast.IndexPartSpecification{{Expr: expr}}, nil
}

// buildFullTextTokenizeExprString renders the array-cast tokenize expression as
// SQL. It is produced as text and parsed back rather than assembled as an AST
// so the resulting node matches exactly what the grammar builds for a
// hand-written expression index, including the array flag and charset defaults.
func buildFullTextTokenizeExprString(colName ast.CIStr, config fulltext.AnalyzerConfig) (string, error) {
	tokenizeCall := expression.BuildFTSTokenizeExpr(
		&ast.ColumnNameExpr{Name: &ast.ColumnName{Name: colName}}, config)

	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase |
		format.RestoreNameBackQuotes | format.RestoreSpacesAroundBinaryOperation |
		format.RestoreWithoutSchemaName | format.RestoreWithoutTableName
	if err := tokenizeCall.Restore(format.NewRestoreCtx(restoreFlags, &sb)); err != nil {
		return "", errors.Trace(err)
	}
	// The element width bounds the longest token the analyzer can emit. A
	// narrower cast would silently truncate tokens and make index lookups
	// disagree with the matcher that produced the search terms.
	return fmt.Sprintf("cast(%s as char(%d) array)", sb.String(), ftsTokenizeElemLen(config)), nil
}

// ftsTokenizeElemLen is the CHAR(n) element width of the multi-valued index.
func ftsTokenizeElemLen(config fulltext.AnalyzerConfig) int {
	if config.ParserType == model.FullTextParserTypeNgramV1 {
		return max(config.NgramTokenSize, 1)
	}
	return max(config.InnodbFtMaxTokenSize, 1)
}

// parseIndexExpr parses an expression string into the AST node an index part
// specification expects.
func parseIndexExpr(exprStr string) (ast.ExprNode, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt("select "+exprStr, mysql.UTF8MB4Charset, mysql.UTF8MB4DefaultCollation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok || sel.Fields == nil || len(sel.Fields.Fields) != 1 {
		return nil, errors.Errorf("unexpected parse result for index expression %q", exprStr)
	}
	return sel.Fields.Fields[0].Expr, nil
}

// fullTextMVIndexOption carries the parts of a FULLTEXT index definition that
// still apply once it has been rewritten into an ordinary multi-valued index.
//
// Tp and ParserName are deliberately dropped: the first would send the index
// back down the columnar path, and the second has already been consumed into
// the tokenize expression. Comment describes the index the user asked for
// rather than how it is stored, so it must survive the rewrite - SHOW CREATE
// TABLE reports it, and losing it would stop that output round-tripping.
//
// Visibility is carried for the same reason, though it is unreachable today:
// INVISIBLE is rejected on a FULLTEXT index before the rewrite runs. Keeping it
// means lifting that restriction does not silently drop the flag.
func fullTextMVIndexOption(indexOption *ast.IndexOption) *ast.IndexOption {
	if indexOption == nil {
		return nil
	}
	if indexOption.Comment == "" && indexOption.Visibility == ast.IndexVisibilityDefault {
		return nil
	}
	return &ast.IndexOption{
		Comment:    indexOption.Comment,
		Visibility: indexOption.Visibility,
	}
}
