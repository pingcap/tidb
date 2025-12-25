// Copyright 2024 PingCAP, Inc.
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

package model

import (
	"encoding/json"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
)

// DistanceMetric is the distance metric used by the vector index.
// Note that not all distance functions are indexable.
// See FnNameToDistanceMetric for a list of indexable distance functions.
type DistanceMetric string

// Note: tipb.VectorDistanceMetric's enum names must be aligned with these constant values.
const (
	DistanceMetricL2 DistanceMetric = "L2"
	// DistanceMetricCosine is cosine distance.
	DistanceMetricCosine DistanceMetric = "COSINE"
	// DistanceMetricInnerProduct is inner product.
	// Currently this distance metric is not supported. It is placed here only for
	// reminding what's the desired naming convension (UPPER_UNDER_SCORE) if this
	// is going to be implemented.
	DistanceMetricInnerProduct DistanceMetric = "INNER_PRODUCT"
)

// IndexableFnNameToDistanceMetric maps a distance function name to the distance metric.
// Only indexable distance functions should be listed here!
var IndexableFnNameToDistanceMetric = map[string]DistanceMetric{
	ast.VecCosineDistance: DistanceMetricCosine,
	ast.VecL2Distance:     DistanceMetricL2,
}

// IndexableDistanceMetricToFnName maps a distance metric to the distance function name.
var IndexableDistanceMetricToFnName = map[DistanceMetric]string{
	DistanceMetricCosine: ast.VecCosineDistance,
	DistanceMetricL2:     ast.VecL2Distance,
}

// VectorIndexInfo is the information of vector index of a column.
type VectorIndexInfo struct {
	// Dimension is the dimension of the vector.
	Dimension uint64 `json:"dimension"`
	// DistanceMetric is the distance metric used by the index.
	DistanceMetric DistanceMetric `json:"distance_metric"`
}

// InvertedIndexInfo is the information of inverted index.
// Currently, we do not support changing the type of the column that has an inverted index.
// But we expect to support modifying the column type which does not need to change data (e.g., INT -> BIGINT).
// In this case, during reading, we can use ColumnID to get both the old and new column types.
type InvertedIndexInfo struct {
	// ColumnID is used for reading.
	ColumnID int64 `json:"column_id"`

	// IsSigned and TypeSize are used for writing.
	IsSigned bool  `json:"is_signed"`
	TypeSize uint8 `json:"type_size"`
}

// FieldTypeToInvertedIndexInfo converts FieldType to InvertedIndexInfo.
func FieldTypeToInvertedIndexInfo(tp types.FieldType, columnID int64) *InvertedIndexInfo {
	var isSigned bool
	var typeSize uint8

	switch tp.GetType() {
	case mysql.TypeTiny:
		typeSize = 1
		isSigned = !mysql.HasUnsignedFlag(tp.GetFlag())
	case mysql.TypeShort:
		typeSize = 2
		isSigned = !mysql.HasUnsignedFlag(tp.GetFlag())
	case mysql.TypeInt24, mysql.TypeLong:
		typeSize = 4
		isSigned = !mysql.HasUnsignedFlag(tp.GetFlag())
	case mysql.TypeLonglong:
		typeSize = 8
		isSigned = !mysql.HasUnsignedFlag(tp.GetFlag())
	case mysql.TypeYear:
		typeSize = 2
		isSigned = false
	case mysql.TypeEnum:
		typeSize = 2
		isSigned = false
	case mysql.TypeSet:
		typeSize = 8
		isSigned = false
	case mysql.TypeDatetime, mysql.TypeDate, mysql.TypeTimestamp:
		typeSize = 8
		isSigned = false
	case mysql.TypeDuration:
		typeSize = 8
		isSigned = true
	default:
		return nil
	}

	return &InvertedIndexInfo{
		ColumnID: columnID,
		IsSigned: isSigned,
		TypeSize: typeSize,
	}
}

// FullTextParserType is the tokenizer kind.
// Note: Must use UPPER_UNDER_SCORE naming convension.
type FullTextParserType string

const (
	// FullTextParserTypeInvalid is the invalid tokenizer
	FullTextParserTypeInvalid FullTextParserType = "INVALID"
	// FullTextParserTypeStandardV1 is the standard parser, for English texts
	// The value matches with the supported tokenizer in Libclara.
	FullTextParserTypeStandardV1 FullTextParserType = "STANDARD_V1"
	// FullTextParserTypeMultilingualV1 is a parser for multilingual texts
	// The value matches with the supported tokenizer in Libclara.
	FullTextParserTypeMultilingualV1 FullTextParserType = "MULTILINGUAL_V1"
	// FullTextParserTypeNgramV1 is a better recall rate,
	// but may be not better performed parser.
	// The value matches with the supported tokenizer in Libclara.
	FullTextParserTypeNgramV1 FullTextParserType = "NGRAM_V1"
)

// SQLName returns the SQL keyword name of the fulltext parser, which must not include
// any version or internal suffix. This is what we present to users and show in error messages.
func (t FullTextParserType) SQLName() string {
	switch t {
	case FullTextParserTypeStandardV1:
		return "STANDARD"
	case FullTextParserTypeMultilingualV1:
		return "MULTILINGUAL"
	case FullTextParserTypeNgramV1:
		return "NGRAM"
	default:
		return "INVALID"
	}
}

// GetFullTextParserTypeBySQLName returns the FullTextParserType by a SQL name.
func GetFullTextParserTypeBySQLName(name string) FullTextParserType {
	switch strings.ToUpper(name) {
	case "STANDARD":
		return FullTextParserTypeStandardV1
	case "MULTILINGUAL":
		return FullTextParserTypeMultilingualV1
	case "NGRAM":
		return FullTextParserTypeNgramV1
	default:
		return FullTextParserTypeInvalid
	}
}

// FullTextIndexInfo is the information of FULLTEXT index of a column.
type FullTextIndexInfo struct {
	ParserType FullTextParserType `json:"parser_type"`
	// TODO: Add other options
}

// HybridIndexInfo is the information of HYBRID index of a column.
type HybridIndexInfo struct {
	FullText []*HybridFullTextSpec `json:"fulltext,omitempty"`
	Vector   []*HybridVectorSpec   `json:"vector,omitempty"`
	Inverted []*HybridInvertedSpec `json:"inverted,omitempty"`
	Sort     *HybridSortSpec       `json:"sort,omitempty"`
	Sharding *HybridShardingSpec   `json:"sharding_key,omitempty"`
}

// HybridFullTextSpec describes the configuration for a fulltext segment in a hybrid index.
type HybridFullTextSpec struct {
	Columns   []*IndexColumn           `json:"columns"`
	IndexInfo *HybridFulltextIndexInfo `json:"index_info"`
}

// HybridFulltextIndexInfo includes analyzer and tokenizer information for the fulltext component.
type HybridFulltextIndexInfo struct {
	Analyzer     *HybridFulltextAnalyzer  `json:"analyzer,omitempty"`
	Tokenizer    *HybridFulltextTokenizer `json:"tokenizer,omitempty"`
	TokenFilters []string                 `json:"token_filter,omitempty"`
}

// HybridFulltextAnalyzer describes the analyzer configuration for the fulltext component.
type HybridFulltextAnalyzer struct {
	Type   string         `json:"type"`
	Params map[string]any `json:"params,omitempty"`
}

// HybridFulltextTokenizer describes the tokenizer configuration for the fulltext component.
type HybridFulltextTokenizer struct {
	Type    string         `json:"type"`
	Options map[string]any `json:"options,omitempty"`
}

// HybridVectorSpec describes the configuration for a vector segment in a hybrid index.
type HybridVectorSpec struct {
	Columns   []*IndexColumn         `json:"columns"`
	IndexInfo *HybridVectorIndexInfo `json:"index_info"`
}

// HybridVectorIndexInfo describes the configuration of a vector index inside the hybrid index.
type HybridVectorIndexInfo struct {
	DistanceMetric string            `json:"distance_metric,omitempty"`
	Dimension      *uint64           `json:"dimension,omitempty"`
	Options        map[string]string `json:"options,omitempty"`
}

// HybridInvertedSpec describes the configuration for an inverted segment in a hybrid index.
type HybridInvertedSpec struct {
	Columns []*IndexColumn `json:"columns,omitempty"`
	Params  map[string]any `json:"params,omitempty"`
}

// HybridSortSpec describes the order definition of the hybrid index.
type HybridSortSpec struct {
	Columns []*IndexColumn `json:"columns,omitempty"`
	// IsAsc stores, for each column, whether it is sorted in ascending order (true) or descending order (false).
	IsAsc []bool `json:"is_asc,omitempty"`
}

// HybridShardingSpec describes the sharding key definition of the hybrid index.
type HybridShardingSpec struct {
	Columns []*IndexColumn `json:"columns,omitempty"`
}

// Clone clones HybridIndexInfo.
func (info *HybridIndexInfo) Clone() *HybridIndexInfo {
	if info == nil {
		return nil
	}
	cloned := &HybridIndexInfo{}
	if len(info.FullText) > 0 {
		cloned.FullText = make([]*HybridFullTextSpec, len(info.FullText))
		for i, ft := range info.FullText {
			if ft == nil {
				continue
			}
			cloned.FullText[i] = ft.Clone()
		}
	}
	if len(info.Vector) > 0 {
		cloned.Vector = make([]*HybridVectorSpec, len(info.Vector))
		for i, v := range info.Vector {
			if v == nil {
				continue
			}
			cloned.Vector[i] = v.Clone()
		}
	}
	if len(info.Inverted) > 0 {
		cloned.Inverted = make([]*HybridInvertedSpec, len(info.Inverted))
		for i, inv := range info.Inverted {
			if inv == nil {
				continue
			}
			cloned.Inverted[i] = inv.Clone()
		}
	}
	if info.Sort != nil {
		cloned.Sort = info.Sort.Clone()
	}
	if info.Sharding != nil {
		cloned.Sharding = info.Sharding.Clone()
	}
	return cloned
}

// Clone clones HybridFullTextSpec.
func (c *HybridFullTextSpec) Clone() *HybridFullTextSpec {
	if c == nil {
		return nil
	}
	cloned := &HybridFullTextSpec{}
	cloned.Columns = cloneIndexColumnSlice(c.Columns)
	if c.IndexInfo != nil {
		cloned.IndexInfo = c.IndexInfo.Clone()
	}
	return cloned
}

// Clone clones HybridFulltextIndexInfo.
func (info *HybridFulltextIndexInfo) Clone() *HybridFulltextIndexInfo {
	if info == nil {
		return nil
	}
	cloned := &HybridFulltextIndexInfo{}
	if info.Analyzer != nil {
		cloned.Analyzer = info.Analyzer.Clone()
	}
	if info.Tokenizer != nil {
		cloned.Tokenizer = info.Tokenizer.Clone()
	}
	if len(info.TokenFilters) > 0 {
		cloned.TokenFilters = append([]string(nil), info.TokenFilters...)
	}
	return cloned
}

// Clone clones HybridFulltextAnalyzer.
func (cfg *HybridFulltextAnalyzer) Clone() *HybridFulltextAnalyzer {
	if cfg == nil {
		return nil
	}
	cloned := &HybridFulltextAnalyzer{Type: cfg.Type}
	if len(cfg.Params) > 0 {
		cloned.Params = cloneInterfaceMap(cfg.Params)
	}
	return cloned
}

// Clone clones HybridFulltextTokenizer.
func (cfg *HybridFulltextTokenizer) Clone() *HybridFulltextTokenizer {
	if cfg == nil {
		return nil
	}
	cloned := &HybridFulltextTokenizer{Type: cfg.Type}
	if len(cfg.Options) > 0 {
		cloned.Options = cloneInterfaceMap(cfg.Options)
	}
	return cloned
}

// Clone clones HybridVectorSpec.
func (c *HybridVectorSpec) Clone() *HybridVectorSpec {
	if c == nil {
		return nil
	}
	cloned := &HybridVectorSpec{}
	cloned.Columns = cloneIndexColumnSlice(c.Columns)
	if c.IndexInfo != nil {
		cloned.IndexInfo = c.IndexInfo.Clone()
	}
	return cloned
}

// Clone clones HybridVectorIndexInfo.
func (info *HybridVectorIndexInfo) Clone() *HybridVectorIndexInfo {
	if info == nil {
		return nil
	}
	cloned := &HybridVectorIndexInfo{
		DistanceMetric: info.DistanceMetric,
	}
	if info.Dimension != nil {
		dim := *info.Dimension
		cloned.Dimension = &dim
	}
	if len(info.Options) > 0 {
		cloned.Options = make(map[string]string, len(info.Options))
		for k, v := range info.Options {
			cloned.Options[k] = v
		}
	}
	return cloned
}

// Clone clones HybridInvertedSpec.
func (c *HybridInvertedSpec) Clone() *HybridInvertedSpec {
	if c == nil {
		return nil
	}
	cloned := &HybridInvertedSpec{}
	cloned.Columns = cloneIndexColumnSlice(c.Columns)
	if len(c.Params) > 0 {
		cloned.Params = cloneInterfaceMap(c.Params)
	}
	return cloned
}

// Clone clones HybridSortSpec.
func (opt *HybridSortSpec) Clone() *HybridSortSpec {
	if opt == nil {
		return nil
	}
	cloned := &HybridSortSpec{}
	cloned.Columns = cloneIndexColumnSlice(opt.Columns)
	if len(opt.IsAsc) > 0 {
		cloned.IsAsc = append([]bool(nil), opt.IsAsc...)
	}
	return cloned
}

// Clone clones HybridShardingSpec.
func (opt *HybridShardingSpec) Clone() *HybridShardingSpec {
	if opt == nil {
		return nil
	}
	cloned := &HybridShardingSpec{}
	cloned.Columns = cloneIndexColumnSlice(opt.Columns)
	return cloned
}

func cloneIndexColumnSlice(cols []*IndexColumn) []*IndexColumn {
	if len(cols) == 0 {
		return nil
	}
	cloned := make([]*IndexColumn, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		cloned[i] = col.Clone()
	}
	return cloned
}

func cloneInterfaceMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = deepCloneInterface(v)
	}
	return dst
}

func deepCloneInterface(v any) any {
	switch val := v.(type) {
	case map[string]any:
		return cloneInterfaceMap(val)
	case []any:
		if len(val) == 0 {
			return []any{}
		}
		res := make([]any, len(val))
		for i, elem := range val {
			res[i] = deepCloneInterface(elem)
		}
		return res
	case json.RawMessage:
		if val == nil {
			return json.RawMessage(nil)
		}
		return append(json.RawMessage(nil), val...)
	default:
		return val
	}
}

// ColumnarIndexType is the type of columnar index.
type ColumnarIndexType uint8

const (
	// ColumnarIndexTypeNA means this is not a columnar index.
	ColumnarIndexTypeNA ColumnarIndexType = iota
	// ColumnarIndexTypeInverted is the inverted index type.
	ColumnarIndexTypeInverted
	// ColumnarIndexTypeVector is the vector index type.
	ColumnarIndexTypeVector
	// ColumnarIndexTypeFulltext is the fulltext index type.
	ColumnarIndexTypeFulltext
	// ColumnarIndexTypeHybrid is the hybrid index type.
	ColumnarIndexTypeHybrid
)

// SQLName returns the SQL keyword name of the columnar index. Used in log messages or error messages.
func (c ColumnarIndexType) SQLName() string {
	switch c {
	case ColumnarIndexTypeVector:
		return "vector index"
	case ColumnarIndexTypeInverted:
		return "inverted index"
	case ColumnarIndexTypeFulltext:
		return "fulltext index"
	case ColumnarIndexTypeHybrid:
		return "hybrid index"
	default:
		return "columnar index"
	}
}

// IndexInfo provides meta data describing a DB index.
// It corresponds to the statement `CREATE INDEX Name ON Table (Column);`
// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type IndexInfo struct {
	ID                  int64              `json:"id"`
	Name                ast.CIStr          `json:"idx_name"` // Index name.
	Table               ast.CIStr          `json:"tbl_name"` // Table name.
	Columns             []*IndexColumn     `json:"idx_cols"` // Index columns.
	State               SchemaState        `json:"state"`
	BackfillState       BackfillState      `json:"backfill_state"`
	Comment             string             `json:"comment"`                 // Comment
	Tp                  ast.IndexType      `json:"index_type"`              // Index type: Btree, Hash, Rtree, Vector, Inverted, Fulltext
	Unique              bool               `json:"is_unique"`               // Whether the index is unique.
	Primary             bool               `json:"is_primary"`              // Whether the index is primary key.
	Invisible           bool               `json:"is_invisible"`            // Whether the index is invisible.
	Global              bool               `json:"is_global"`               // Whether the index is global.
	MVIndex             bool               `json:"mv_index"`                // Whether the index is multivalued index.
	VectorInfo          *VectorIndexInfo   `json:"vector_index"`            // VectorInfo is the vector index information.
	InvertedInfo        *InvertedIndexInfo `json:"inverted_index"`          // InvertedInfo is the inverted index information.
	FullTextInfo        *FullTextIndexInfo `json:"full_text_index"`         // FullTextInfo is the FULLTEXT index information.
	HybridInfo          *HybridIndexInfo   `json:"hybrid_index"`            // HybridInfo is the HYBRID index information.
	ConditionExprString string             `json:"condition_expr_string"`   // ConditionExprString is the string representation of the partial index condition.
	AffectColumn        []*IndexColumn     `json:"affect_column,omitempty"` // AffectColumn is the columns related to the index.
}

// Hash64 implement HashEquals interface.
func (index *IndexInfo) Hash64(h base.Hasher) {
	h.HashInt64(index.ID)
}

// Equals implements HashEquals interface.
func (index *IndexInfo) Equals(other any) bool {
	// any(nil) can still be converted as (*IndexInfo)(nil)
	index2, ok := other.(*IndexInfo)
	if !ok {
		return false
	}
	if index == nil {
		return index2 == nil
	}
	if index2 == nil {
		return false
	}
	return index.ID == index2.ID
}

// Clone clones IndexInfo.
func (index *IndexInfo) Clone() *IndexInfo {
	if index == nil {
		return nil
	}
	ni := *index
	ni.Columns = make([]*IndexColumn, len(index.Columns))
	for i := range index.Columns {
		ni.Columns[i] = index.Columns[i].Clone()
	}
	if index.HybridInfo != nil {
		ni.HybridInfo = index.HybridInfo.Clone()
	}
	return &ni
}

// HasPrefixIndex returns whether any columns of this index uses prefix length.
func (index *IndexInfo) HasPrefixIndex() bool {
	for _, ic := range index.Columns {
		if ic.Length != types.UnspecifiedLength {
			return true
		}
	}
	return false
}

// HasColumnInIndexColumns checks whether the index contains the column with the specified ID.
func (index *IndexInfo) HasColumnInIndexColumns(tblInfo *TableInfo, colID int64) bool {
	for _, ic := range index.Columns {
		if tblInfo.Columns[ic.Offset].ID == colID {
			return true
		}
	}
	return false
}

// FindColumnByName finds the index column with the specified name.
func (index *IndexInfo) FindColumnByName(nameL string) *IndexColumn {
	_, ret := FindIndexColumnByName(index.Columns, nameL)
	return ret
}

// IsPublic checks if the index state is public
func (index *IndexInfo) IsPublic() bool {
	return index.State == StatePublic
}

// IsColumnarIndex checks whether the index is a columnar index.
// Columnar index only exists in TiFlash, no actual index data need to be written to KV layer.
func (index *IndexInfo) IsColumnarIndex() bool {
	// Exclude the tiflash fulltext index temporarily.
	return index.VectorInfo != nil || index.InvertedInfo != nil // || index.FullTextInfo != nil
}

// IsTiCIIndex checks whether the index is a fulltext index.
// Fulltext index only exists in TiCI, no actual index data need to be written to KV layer.
func (index *IndexInfo) IsTiCIIndex() bool {
	return index.FullTextInfo != nil || index.HybridInfo != nil
}

// GetColumnarIndexType returns the type of columnar index.
func (index *IndexInfo) GetColumnarIndexType() ColumnarIndexType {
	if index.VectorInfo != nil {
		return ColumnarIndexTypeVector
	}
	if index.InvertedInfo != nil {
		return ColumnarIndexTypeInverted
	}
	if index.FullTextInfo != nil {
		return ColumnarIndexTypeFulltext
	}
	if index.HybridInfo != nil {
		return ColumnarIndexTypeHybrid
	}
	return ColumnarIndexTypeNA
}

// HasCondition checks whether the index has a partial index condition.
func (index *IndexInfo) HasCondition() bool {
	return len(index.ConditionExprString) > 0
}

// ConditionExpr parses and returns the condition expression of the partial index.
func (index *IndexInfo) ConditionExpr() (ast.ExprNode, error) {
	stmtStr := "select " + index.ConditionExprString
	stmts, _, err := parser.New().ParseSQL(stmtStr)
	if err != nil {
		return nil, err
	}
	return stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr, nil
}

// FindIndexByColumns find IndexInfo in indices which is cover the specified columns.
func FindIndexByColumns(tbInfo *TableInfo, indices []*IndexInfo, cols ...ast.CIStr) *IndexInfo {
	for _, index := range indices {
		if IsIndexPrefixCovered(tbInfo, index, cols...) {
			return index
		}
	}
	return nil
}

// IsIndexPrefixCovered checks the index's columns beginning with the cols.
func IsIndexPrefixCovered(tbInfo *TableInfo, index *IndexInfo, cols ...ast.CIStr) bool {
	if len(index.Columns) < len(cols) {
		return false
	}
	for i := range cols {
		if cols[i].L != index.Columns[i].Name.L ||
			index.Columns[i].Offset >= len(tbInfo.Columns) {
			return false
		}
		colInfo := tbInfo.Columns[index.Columns[i].Offset]
		if index.Columns[i].Length != types.UnspecifiedLength && index.Columns[i].Length < colInfo.GetFlen() {
			return false
		}
	}
	return true
}

// FindIndexInfoByID finds IndexInfo in indices by id.
func FindIndexInfoByID(indices []*IndexInfo, id int64) *IndexInfo {
	for _, idx := range indices {
		if idx.ID == id {
			return idx
		}
	}
	return nil
}

// IndexColumn provides index column info.
type IndexColumn struct {
	Name   ast.CIStr `json:"name"`   // Index column name
	Offset int       `json:"offset"` // Index column offset in TableInfo.Columns
	// Length of prefix when using column prefix
	// for indexing;
	// UnspecifedLength if not using prefix indexing
	Length int `json:"length"`
	// Whether this index column use changing type
	UseChangingType bool `json:"using_changing_type,omitempty"`
}

// Clone clones IndexColumn.
func (i *IndexColumn) Clone() *IndexColumn {
	ni := *i
	return &ni
}

// FindIndexColumnByName finds IndexColumn by name. When IndexColumn is not found, returns (-1, nil).
func FindIndexColumnByName(indexCols []*IndexColumn, nameL string) (int, *IndexColumn) {
	for i, ic := range indexCols {
		if ic.Name.L == nameL {
			return i, ic
		}
	}
	return -1, nil
}
