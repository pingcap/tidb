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
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/types"
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

	// changingIndexPrefix the prefix is used to initialize new index name created in modify column.
	// The new name will be like "_Idx$_<old_index_name>_n".
	changingIndexPrefix = "_Idx$_"

	// GlobalIndexVersion constants define the key format versions for global indexes.
	// GlobalIndexVersionLegacy is the legacy format (version 0) where partition ID is not in the key.
	// This format has a bug with duplicate handles after EXCHANGE PARTITION on non-clustered tables.
	// See https://github.com/pingcap/tidb/issues/65289
	GlobalIndexVersionLegacy uint8 = 0
	// GlobalIndexVersionV1 is the current format (version 1) where partition ID is encoded in the key
	// for global indexes on non-clustered tables to prevent key collisions
	// after EXCHANGE PARTITION.
	// Applies to non-unique indexes (handle always in key) and unique indexes with nullable
	// columns (handle in key when any indexed value is NULL, since NULL != NULL).
	// For unique global indexes where all columns are NOT NULL, version 0 is used since
	// uniqueness alone prevents collisions.
	// For clustered tables, common handles already include partition-specific data.
	// Notice that for V1 the partition id is still in the value part as well,
	// for decreasing the risk of issues changing the read code path for various index reads.
	GlobalIndexVersionV1 uint8 = 1
	// GlobalIndexVersionV2 is the next, not yet implemented format (version 2) where partition ID
	// is encoded in the key ONLY!
	GlobalIndexVersionV2 uint8 = 2
)

// GenUniqueChangingIndexName generates a unique index name for the changing index.
func GenUniqueChangingIndexName(tblInfo *TableInfo, idxInfo *IndexInfo) string {
	// Check whether the new index name is used.
	indexNameMap := make(map[string]bool, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		indexNameMap[idx.Name.L] = true
	}
	suffix := 0
	newIndexName := fmt.Sprintf("%s%s_%d", changingIndexPrefix, idxInfo.Name.O, suffix)
	for indexNameMap[strings.ToLower(newIndexName)] {
		suffix++
		newIndexName = fmt.Sprintf("%s%s_%d", changingIndexPrefix, idxInfo.Name.O, suffix)
	}
	return newIndexName
}

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

// FullTextParserType is the tokenizer kind.
// Note: Must use UPPER_UNDER_SCORE naming convention.
type FullTextParserType string

const (
	// FullTextParserTypeInvalid is the invalid tokenizer.
	FullTextParserTypeInvalid FullTextParserType = "INVALID"
	// FullTextParserTypeStandardV1 is the standard parser for English texts.
	FullTextParserTypeStandardV1 FullTextParserType = "STANDARD_V1"
	// FullTextParserTypeMultilingualV1 is a parser for multilingual texts.
	FullTextParserTypeMultilingualV1 FullTextParserType = "MULTILINGUAL_V1"
	// FullTextParserTypeNgramV1 is a better recall rate,
	// but may be not better performed parser.
	// The value matches with the supported tokenizer in Libclara.
	FullTextParserTypeNgramV1 FullTextParserType = "NGRAM_V1"
)

// SQLName returns the SQL keyword name of the fulltext parser.
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

// FullTextIndexInfo is the information of a FULLTEXT index.
type FullTextIndexInfo struct {
	ParserType FullTextParserType `json:"parser_type"`
	// ParserConfig records the creation-time tokenization variables. Nil denotes a legacy index
	// whose creation-time analyzer settings are unknown.
	ParserConfig *FullTextParserConfig `json:"parser_config,omitempty"`
}

// FullTextParserConfig preserves the creation-time tokenization variables.
// Custom stopword table names and contents are deliberately not persisted.
type FullTextParserConfig struct {
	InnodbFtMinTokenSize   int  `json:"innodb_ft_min_token_size"`
	InnodbFtMaxTokenSize   int  `json:"innodb_ft_max_token_size"`
	NgramTokenSize         int  `json:"ngram_token_size"`
	InnodbFtEnableStopword bool `json:"innodb_ft_enable_stopword"`
}

// Clone returns an independent copy of the fulltext index metadata.
func (info *FullTextIndexInfo) Clone() *FullTextIndexInfo {
	if info == nil {
		return nil
	}
	cloned := *info
	if info.ParserConfig != nil {
		config := *info.ParserConfig
		cloned.ParserConfig = &config
	}
	return &cloned
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

// IndexInfo provides meta data describing a DB index.
// It corresponds to the statement `CREATE INDEX Name ON Table (Column);`
// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type IndexInfo struct {
	ID                  int64              `json:"id"`
	Name                model.CIStr        `json:"idx_name"` // Index name.
	Table               model.CIStr        `json:"tbl_name"` // Table name.
	Columns             []*IndexColumn     `json:"idx_cols"` // Index columns.
	State               SchemaState        `json:"state"`
	BackfillState       BackfillState      `json:"backfill_state"`
	Comment             string             `json:"comment"`                       // Comment
	Tp                  model.IndexType    `json:"index_type"`                    // Index type: Btree, Hash, Rtree or HNSW
	Unique              bool               `json:"is_unique"`                     // Whether the index is unique.
	Primary             bool               `json:"is_primary"`                    // Whether the index is primary key.
	Invisible           bool               `json:"is_invisible"`                  // Whether the index is invisible.
	Global              bool               `json:"is_global"`                     // Whether the index is global.
	MVIndex             bool               `json:"mv_index"`                      // Whether the index is multivalued index.
	VectorInfo          *VectorIndexInfo   `json:"vector_index"`                  // VectorInfo is the vector index information.
	FullTextInfo        *FullTextIndexInfo `json:"full_text_index"`               // FullTextInfo is the FULLTEXT index information.
	HybridInfo          *HybridIndexInfo   `json:"hybrid_index"`                  // HybridInfo is the HYBRID index information.
	ConditionExprString string             `json:"partial_condition_expr_string"` // ConditionExprString is the string representation of the partial index condition.
	AffectColumn        []*IndexColumn     `json:"affect_column,omitempty"`       // AffectColumn is the columns related to the index.
	// Version of global index key format for non-clustered tables.
	// Set to V1 when the handle can appear in the index key (non-unique indexes,
	// or unique indexes with any nullable column) to prevent collisions after EXCHANGE PARTITION.
	// 0=legacy, or unique with all NOT NULL columns, or clustered.
	// 1=v1 with partition ID in key and value.
	// 2=v2 with partition ID in key only (TODO).
	GlobalIndexVersion uint8 `json:"global_index_version,omitempty"`
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
	ni.FullTextInfo = index.FullTextInfo.Clone()
	if index.HybridInfo != nil {
		ni.HybridInfo = index.HybridInfo.Clone()
	}
	if index.AffectColumn != nil {
		ni.AffectColumn = make([]*IndexColumn, len(index.AffectColumn))
		for i := range index.AffectColumn {
			ni.AffectColumn[i] = index.AffectColumn[i].Clone()
		}
	}
	return &ni
}

// IsChanging checks if the index is a new index added in modify column.
func (index *IndexInfo) IsChanging() bool {
	return strings.HasPrefix(index.Name.O, changingIndexPrefix)
}

// IsRemoving checks if the index is a index to be removed in modify column.
func (index *IndexInfo) IsRemoving() bool {
	return strings.HasPrefix(index.Name.O, removingObjPrefix)
}

// GetRemovingOriginName gets the origin name of the removing index.
func (index *IndexInfo) GetRemovingOriginName() string {
	return strings.TrimPrefix(index.Name.O, removingObjPrefix)
}

// GetChangingOriginName gets the origin index name from the changing index.
func (index *IndexInfo) GetChangingOriginName() string {
	idxName := strings.TrimPrefix(index.Name.O, changingIndexPrefix)
	// Since the unique idxName may contain the suffix number (indexName_num), better trim the suffix.
	var pos int
	if pos = strings.LastIndex(idxName, "_"); pos == -1 {
		return idxName
	}
	return idxName[:pos]
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

// IsTiFlashLocalIndex checks whether the index is a TiFlash local index.
// For a TiFlash local index, no actual index data need to be written to KV layer.
func (index *IndexInfo) IsTiFlashLocalIndex() bool {
	return index.VectorInfo != nil
}

// IsNonKVIndex checks whether the index has no index data in TiKV.
func (index *IndexInfo) IsNonKVIndex() bool {
	return index.IsTiFlashLocalIndex() || index.IsTiCIIndex()
}

// IsTiCIIndex checks whether the index is a fulltext index.
// Fulltext indexes only exist in TiCI, so no actual index data is written to the KV layer.
func (index *IndexInfo) IsTiCIIndex() bool {
	return index.FullTextInfo != nil || index.HybridInfo != nil
}

// HasExtraTiCIShardingKey checks whether the TiCI index uses an explicit
// sharding key, whose storage key has the normal index-key layout.
func (index *IndexInfo) HasExtraTiCIShardingKey() bool {
	return index.HybridInfo != nil && index.HybridInfo.Sharding != nil
}

// HybridShardingColumns returns the sharding columns for a hybrid index.
// It returns nil when the index is not hybrid or sharding is not configured.
func (index *IndexInfo) HybridShardingColumns() []*IndexColumn {
	if index == nil || index.HybridInfo == nil || index.HybridInfo.Sharding == nil {
		return nil
	}
	return index.HybridInfo.Sharding.Columns
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
func FindIndexByColumns(tbInfo *TableInfo, indices []*IndexInfo, cols ...model.CIStr) *IndexInfo {
	for _, index := range indices {
		if IsIndexPrefixCovered(tbInfo, index, cols...) {
			return index
		}
	}
	return nil
}

// IsIndexPrefixCovered checks the index's columns beginning with the cols.
func IsIndexPrefixCovered(tbInfo *TableInfo, index *IndexInfo, cols ...model.CIStr) bool {
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

// FindIndexByColumnsForForeignKey finds an index that can be safely used by a foreign key.
func FindIndexByColumnsForForeignKey(tbInfo *TableInfo, indices []*IndexInfo, cols ...model.CIStr) *IndexInfo {
	for _, index := range indices {
		if IsIndexPrefixCoveredForForeignKey(tbInfo, index, cols...) {
			return index
		}
	}
	return nil
}

// IsIndexPrefixCoveredForForeignKey checks whether the index covers the foreign key columns
// and whether the partial index predicate, if any, is safe for foreign key checks.
func IsIndexPrefixCoveredForForeignKey(tbInfo *TableInfo, index *IndexInfo, cols ...model.CIStr) bool {
	if index.IsNonKVIndex() {
		return false
	}
	if !IsIndexPrefixCovered(tbInfo, index, cols...) {
		return false
	}
	return isIndexConditionCoveredByForeignKeyCols(index, cols...)
}

// isIndexConditionCoveredByForeignKeyCols returns whether the partial index predicate
// is implied by the rows that need foreign key checks.
//
// Foreign keys currently use MATCH SIMPLE semantics: for a composite foreign key,
// a row participates in checks and cascades only when all foreign key columns are
// non-NULL. Therefore, a predicate of "<fk-col> IS NOT NULL" on any one foreign
// key column is safe, because every row that needs a foreign key lookup satisfies
// it. Predicates on non-foreign-key columns, or stricter predicates such as
// comparisons, may filter out rows that still need checks and are not safe here.
func isIndexConditionCoveredByForeignKeyCols(index *IndexInfo, cols ...model.CIStr) bool {
	if !index.HasCondition() {
		return true
	}
	expr, err := index.ConditionExpr()
	if err != nil {
		return false
	}
	isNullExpr, ok := expr.(*ast.IsNullExpr)
	if !ok || !isNullExpr.Not {
		return false
	}
	colExpr, ok := isNullExpr.Expr.(*ast.ColumnNameExpr)
	if !ok {
		return false
	}
	for _, col := range cols {
		if colExpr.Name.Name.L == col.L {
			return true
		}
	}
	return false
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
	Name   model.CIStr `json:"name"`   // Index name
	Offset int         `json:"offset"` // Index offset
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

// GetIdxChangingFieldType gets the field type of index column.
// Since both old/new type may coexist in one column during modify column,
// we need to get the correct type for index column.
func GetIdxChangingFieldType(idxCol *IndexColumn, col *ColumnInfo) *types.FieldType {
	if idxCol.UseChangingType && col.ChangingFieldType != nil {
		return col.ChangingFieldType
	}
	return &col.FieldType
}
