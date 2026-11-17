// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unsafe"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tipb/go-tipb"
)

const (
	// FtsScoreModeInherit means the current fts expr needs score iff
	// all its parents' score mode is also inherit.
	FtsScoreModeInherit string = "inherit"
	// FtsScoreModeFilter means the current fts expr does not need score.
	FtsScoreModeFilter string = "filter"

	// FTSQueryClauseVersion is the current JSON query-clause format version.
	FTSQueryClauseVersion uint = 1
)

// FTSInfo is an easy to use struct for interpreting a FullTextSearch expression.
type FTSInfo struct {
	// Query and Column retain the convenient single-column view used by
	// FTS_MATCH_WORD callers.
	Query    string
	Column   *Column
	FuncName string
	Columns  []*Column
	Clause   *QueryClause
}

// FTSMatchWordFieldOption contains the parser selected for one field argument
// of FTS_MATCH_WORD. An empty parser name means the parser should be inferred
// from the selected full-text index.
type FTSMatchWordFieldOption struct {
	ParserName string
}

// FTSMatchWordOptions contains function-local FTS_MATCH_WORD metadata.
// Fields[i] describes args[i+1], because args[0] is the query.
type FTSMatchWordOptions struct {
	Fields []FTSMatchWordFieldOption
	// Query overrides the string query argument in the JSON metadata. It is
	// used by planner-generated exact predicates to retain native exact values
	// while keeping the compatibility expression children string typed.
	Query any
}

func (o *FTSMatchWordOptions) clone() *FTSMatchWordOptions {
	if o == nil {
		return nil
	}
	cloned := &FTSMatchWordOptions{
		Fields: make([]FTSMatchWordFieldOption, len(o.Fields)),
		Query:  o.Query,
	}
	copy(cloned.Fields, o.Fields)
	return cloned
}

// MemoryUsage returns the memory usage of FTSMatchWordOptions.
func (o *FTSMatchWordOptions) MemoryUsage() int64 {
	if o == nil {
		return 0
	}
	usage := int64(unsafe.Sizeof(*o)) + int64(cap(o.Fields))*int64(unsafe.Sizeof(FTSMatchWordFieldOption{}))
	for _, field := range o.Fields {
		usage += int64(len(field.ParserName))
	}
	if o.Query != nil {
		encoded, err := json.Marshal(o.Query)
		if err == nil {
			usage += int64(len(encoded))
		}
	}
	return usage
}

// SetFTSMatchWordOptions replaces the FTS match metadata associated with sf.
// The options are copied so callers cannot mutate an expression without also
// invalidating its cached hashes.
func SetFTSMatchWordOptions(sf *ScalarFunction, options *FTSMatchWordOptions) {
	if sf == nil || sf.FuncName.L != ast.FTSMatchWord {
		return
	}
	sig, ok := sf.Function.(*builtinFtsMatchWordSig)
	if !ok {
		return
	}
	sig.options = options.clone()
	sf.hashcode = nil
	sf.canonicalhashcode = nil
}

// GetFTSMatchWordOptions returns a copy of the FTS match metadata associated
// with sf.
func GetFTSMatchWordOptions(sf *ScalarFunction) *FTSMatchWordOptions {
	if sf == nil || sf.FuncName.L != ast.FTSMatchWord {
		return nil
	}
	sig, ok := sf.Function.(*builtinFtsMatchWordSig)
	if !ok {
		return nil
	}
	return sig.options.clone()
}

func (sf *ScalarFunction) ftsMatchWordOptionCount() int {
	if sf == nil || sf.FuncName.L != ast.FTSMatchWord {
		return 0
	}
	if options := GetFTSMatchWordOptions(sf); options != nil {
		return len(options.Fields)
	}
	if len(sf.GetArgs()) < 1 {
		return 0
	}
	return len(sf.GetArgs()) - 1
}

func (sf *ScalarFunction) normalizedFTSMatchWordParserName(index int) string {
	options := GetFTSMatchWordOptions(sf)
	if options == nil || index < 0 || index >= len(options.Fields) {
		return ""
	}
	return normalizeFullTextParserName(options.Fields[index].ParserName)
}

func normalizeFullTextParserName(name string) string {
	parserType := ast.GetFullTextParserTypeByName(name)
	if parserType != ast.FullTextParserTypeInvalid {
		return string(parserType)
	}
	return strings.ToUpper(name)
}

func normalizeFTSScoreMode(mode string) string {
	switch strings.ToLower(mode) {
	case "", FtsScoreModeInherit:
		return FtsScoreModeInherit
	case FtsScoreModeFilter:
		return FtsScoreModeFilter
	default:
		return strings.ToLower(mode)
	}
}

func ftsQueryOverrideJSON(options *FTSMatchWordOptions) []byte {
	if options == nil || options.Query == nil {
		return nil
	}
	encoded, err := json.Marshal(options.Query)
	if err != nil {
		return []byte(fmt.Sprintf("%v", options.Query))
	}
	return encoded
}

func (sf *ScalarFunction) ftsOptionsEqual(other *ScalarFunction) bool {
	if sf.FuncName.L != ast.FTSMatchWord {
		return true
	}
	leftCount := sf.ftsMatchWordOptionCount()
	if leftCount != other.ftsMatchWordOptionCount() {
		return false
	}
	for i := 0; i < leftCount; i++ {
		if sf.normalizedFTSMatchWordParserName(i) != other.normalizedFTSMatchWordParserName(i) {
			return false
		}
	}
	return bytes.Equal(
		ftsQueryOverrideJSON(GetFTSMatchWordOptions(sf)),
		ftsQueryOverrideJSON(GetFTSMatchWordOptions(other)),
	)
}

func (sf *ScalarFunction) appendFTSOptionsHashCode(hash *[]byte) {
	if sf == nil {
		return
	}
	if sf.FuncName.L != ast.FTSMatchWord {
		return
	}
	count := sf.ftsMatchWordOptionCount()
	*hash = codec.EncodeInt(*hash, int64(count))
	for i := 0; i < count; i++ {
		*hash = codec.EncodeCompactBytes(*hash, hack.Slice(sf.normalizedFTSMatchWordParserName(i)))
	}
	*hash = codec.EncodeCompactBytes(*hash, ftsQueryOverrideJSON(GetFTSMatchWordOptions(sf)))
}

func (sf *ScalarFunction) hashFTSOptions64(h base.Hasher) {
	if sf == nil {
		return
	}
	if sf.FuncName.L != ast.FTSMatchWord {
		return
	}
	count := sf.ftsMatchWordOptionCount()
	h.HashInt(count)
	for i := 0; i < count; i++ {
		h.HashString(sf.normalizedFTSMatchWordParserName(i))
	}
	h.HashBytes(ftsQueryOverrideJSON(GetFTSMatchWordOptions(sf)))
}

// ContainsFullTextSearchFn recursively checks whether the expression tree contains a
// possible FullTextSearch function.
func ContainsFullTextSearchFn(expr Expression) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		if x.FuncName.L == ast.FTSMatchWord {
			return true
		}
		if slices.ContainsFunc(x.GetArgs(), ContainsFullTextSearchFn) {
			return true
		}
	}
	return false
}

// InterpretFullTextSearchExpr try to interpret a FullText search expression.
// If interpret successfully, return a FTSInfo struct, otherwise return nil.
func InterpretFullTextSearchExpr(expr Expression) *FTSInfo {
	x, ok := expr.(*ScalarFunction)
	if !ok {
		return nil
	}

	if x.FuncName.L != ast.FTSMatchWord {
		return nil
	}

	if len(x.GetArgs()) < 2 {
		return nil
	}
	argQuery := x.GetArgs()[0]

	query, ok := argQuery.(*Constant)
	if !ok {
		return nil
	}
	queryText := query.Value.GetString()
	queryValue := any(queryText)
	options := GetFTSMatchWordOptions(x)
	if options != nil {
		if options.Query != nil {
			queryValue = options.Query
		}
	}
	fieldCount := len(x.GetArgs()) - 1
	if options != nil && len(options.Fields) != fieldCount {
		return nil
	}
	columns := make([]*Column, 0, fieldCount)
	fields := make([]QueryField, 0, fieldCount)
	for i, arg := range x.GetArgs()[1:] {
		column, ok := arg.(*Column)
		if !ok {
			return nil
		}
		columns = append(columns, column)
		parserName := ""
		if options != nil {
			parserName = options.Fields[i].ParserName
		}
		fields = append(fields, QueryField{
			ColID:      column.ID,
			IndexField: parserName,
		})
	}

	return &FTSInfo{
		Query:    queryText,
		Column:   columns[0],
		FuncName: x.FuncName.L,
		Columns:  columns,
		Clause: &QueryClause{
			Version:   FTSQueryClauseVersion,
			ScoreMode: FtsScoreModeInherit,
			Query:     queryValue,
			Fields:    fields,
		},
	}
}

// QueryField binds a column with specified parser type.
type QueryField struct {
	ColID int64 `json:"col_id"`
	// the parse_type of this column.
	IndexField string `json:"index_field"`
}

// QueryClause is the JSON query tree embedded in a full-text match expression.
type QueryClause struct {
	// query format version, current ver is 1.
	Version uint `json:"version"`
	// inherit or filter
	ScoreMode string `json:"score_mode"`
	// The query content is not necessarily a string; its type should be
	// compatible with the target column type.
	Query any `json:"query"`
	// The query fields.
	Fields []QueryField `json:"fields"`
}

func fullTextParserNameEqual(left, right string) bool {
	if strings.EqualFold(left, right) {
		return true
	}
	leftType := ast.GetFullTextParserTypeByName(left)
	return leftType != ast.FullTextParserTypeInvalid && leftType == ast.GetFullTextParserTypeByName(right)
}

// FTSQueryExpressionToPB converts an FTS query tree to TiPB. Every nested
// FTS_MATCH_WORD node stores its resolved QueryClause as JSON in its Val field.
func FTSQueryExpressionToPB(
	ctx EvalContext,
	client kv.Client,
	expr Expression,
) (tipb.Expr, error) {
	pbExpr := NewPBConverter(client, ctx).ExprToPB(expr)
	if pbExpr == nil {
		return tipb.Expr{}, fmt.Errorf("full-text query expression cannot be pushed down")
	}
	if err := attachFTSQueryClauses(expr, pbExpr); err != nil {
		return tipb.Expr{}, err
	}
	normalizeFTSMatchExprPB(pbExpr)
	if len(pbExpr.Val) == 0 {
		encoded, err := json.Marshal(struct {
			Version uint `json:"version"`
		}{Version: FTSQueryClauseVersion})
		if err != nil {
			return tipb.Expr{}, fmt.Errorf("encode full-text root metadata: %w", err)
		}
		pbExpr.Val = encoded
	}
	return *pbExpr, nil
}

func normalizeFTSMatchExprPB(expr *tipb.Expr) {
	if expr == nil {
		return
	}
	for i, child := range expr.Children {
		normalizeFTSMatchExprPB(child)
		if isFTSTruthWrapper(child) {
			expr.Children[i] = child.Children[0]
		}
	}
}

func isFTSTruthWrapper(expr *tipb.Expr) bool {
	if expr == nil || len(expr.Children) != 1 {
		return false
	}
	switch expr.Sig {
	case tipb.ScalarFuncSig_IntIsTrueWithNull,
		tipb.ScalarFuncSig_RealIsTrueWithNull,
		tipb.ScalarFuncSig_DecimalIsTrueWithNull,
		tipb.ScalarFuncSig_VectorFloat32IsTrueWithNull:
		return true
	default:
		return false
	}
}

func attachFTSQueryClauses(
	expr Expression,
	pbExpr *tipb.Expr,
) error {
	scalar, ok := expr.(*ScalarFunction)
	if !ok {
		return nil
	}
	args := scalar.GetArgs()
	if len(args) != len(pbExpr.Children) {
		return fmt.Errorf(
			"full-text query expression child count mismatch for %s: expression has %d, protobuf has %d",
			scalar.FuncName.L, len(args), len(pbExpr.Children),
		)
	}
	if scalar.FuncName.L == ast.FTSMatchWord {
		info := InterpretFullTextSearchExpr(scalar)
		if info == nil || info.Clause == nil {
			return fmt.Errorf("invalid FTS_MATCH_WORD query expression")
		}
		encoded, err := json.Marshal(info.Clause)
		if err != nil {
			return fmt.Errorf("encode full-text query clause: %w", err)
		}
		pbExpr.Val = encoded
	}
	for i, arg := range args {
		if err := attachFTSQueryClauses(arg, pbExpr.Children[i]); err != nil {
			return err
		}
	}
	return nil
}

// DecodeFTSQueryClause decodes a query clause from an FTS match expression.
func DecodeFTSQueryClause(expr tipb.Expr) (QueryClause, error) {
	var clause QueryClause
	decoder := json.NewDecoder(bytes.NewReader(expr.Val))
	decoder.UseNumber()
	if err := decoder.Decode(&clause); err != nil {
		return QueryClause{}, fmt.Errorf("decode full-text query clause: %w", err)
	}
	return clause, nil
}

// FTSQueryPBMatchesScoringExpr reports whether candidate is the sole scoring
// expression in root. Logical-AND nodes are traversed recursively so nested
// CNF branches can contain the scoring expression. Non-matching sibling
// branches are marked as filters, and the result is true only for exactly one
// match.
func FTSQueryPBMatchesScoringExpr(root *tipb.Expr, candidate tipb.Expr) bool {
	matchCount, ok := matchFTSQueryPBAndMarkFilters(root, &candidate)
	return ok && matchCount == 1
}

func matchFTSQueryPBAndMarkFilters(root, candidate *tipb.Expr) (int, bool) {
	if root == nil {
		return 0, false
	}
	if ftsQueryPBEqual(root, candidate) {
		return 1, true
	}
	if root.Sig != tipb.ScalarFuncSig_LogicalAnd {
		return 0, true
	}
	count := 0
	for _, child := range root.Children {
		childCount, ok := matchFTSQueryPBAndMarkFilters(child, candidate)
		if !ok {
			return 0, false
		}
		if childCount == 0 && !setFTSQueryPBScoreMode(child, FtsScoreModeFilter) {
			return 0, false
		}
		count += childCount
	}
	return count, true
}

func setFTSQueryPBScoreMode(expr *tipb.Expr, scoreMode string) bool {
	if expr == nil {
		return false
	}
	if expr.Sig == tipb.ScalarFuncSig_FTSMatchWord {
		clause, err := DecodeFTSQueryClause(*expr)
		if err != nil {
			return false
		}
		clause.ScoreMode = normalizeFTSScoreMode(scoreMode)
		encoded, err := json.Marshal(&clause)
		if err != nil {
			return false
		}
		expr.Val = encoded
		return true
	}
	encoded, err := json.Marshal(struct {
		Version   uint   `json:"version"`
		ScoreMode string `json:"score_mode"`
	}{
		Version:   FTSQueryClauseVersion,
		ScoreMode: normalizeFTSScoreMode(scoreMode),
	})
	if err != nil {
		return false
	}
	expr.Val = encoded
	return true
}

func ftsQueryPBEqual(left, right *tipb.Expr) bool {
	if left == nil || right == nil || left.Sig != right.Sig {
		return false
	}
	if left.Sig == tipb.ScalarFuncSig_FTSMatchWord {
		leftClause, leftErr := DecodeFTSQueryClause(*left)
		rightClause, rightErr := DecodeFTSQueryClause(*right)
		return leftErr == nil && rightErr == nil && ftsQueryClauseEqual(leftClause, rightClause)
	}
	if len(left.Children) != len(right.Children) {
		return false
	}
	for i := range left.Children {
		if !ftsQueryPBEqual(left.Children[i], right.Children[i]) {
			return false
		}
	}
	return true
}

func ftsQueryClauseEqual(left, right QueryClause) bool {
	if left.Version != right.Version ||
		normalizeFTSScoreMode(left.ScoreMode) != normalizeFTSScoreMode(right.ScoreMode) ||
		len(left.Fields) != len(right.Fields) {
		return false
	}
	leftQuery, leftErr := json.Marshal(left.Query)
	rightQuery, rightErr := json.Marshal(right.Query)
	if leftErr != nil || rightErr != nil || !bytes.Equal(leftQuery, rightQuery) {
		return false
	}
	for i := range left.Fields {
		if left.Fields[i].ColID != right.Fields[i].ColID ||
			!fullTextParserNameEqual(left.Fields[i].IndexField, right.Fields[i].IndexField) {
			return false
		}
	}
	return true
}
