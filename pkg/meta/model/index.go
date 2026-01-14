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
	"fmt"
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

	// changingIndexPrefix the prefix is used to initialize new index name created in modify column.
	// The new name will be like "_Idx$_<old_index_name>_n".
	changingIndexPrefix = "_Idx$_"

	// GlobalIndexVersion constants define the key format versions for global indexes.
	// GlobalIndexVersionLegacy is the legacy format (version 0) where partition ID is not in the key.
	// This format has a bug with duplicate handles after EXCHANGE PARTITION on non-clustered tables.
	// See https://github.com/pingcap/tidb/issues/65289
	GlobalIndexVersionLegacy uint8 = 0
	// GlobalIndexVersionV1 is the current format (version 1) where partition ID is encoded in the key
	// for non-unique global indexes on non-clustered tables to prevent key collisions
	// after EXCHANGE PARTITION.
	// For unique global indexes, the partition ID is not needed in the key since uniqueness is
	// already enforced.
	// For clustered tables, common handles already include partition-specific data.
	// Notice that for V1 the partition id is still in the value part as well,
	// for decreasing the risk of issues changing the read code path for various index reads.
	GlobalIndexVersionV1 uint8 = 1
	// GlobalIndexVersionV2 is the format (version 2) where partition ID is encoded in the key ONLY
	// for non-unique global indexes on non-clustered tables. The partition ID is NOT stored in the
	// value, reducing storage overhead since the partition ID can be extracted from the key when
	// needed. This is the preferred format for new global indexes.
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
)

// SQLName returns the SQL keyword name of the fulltext parser, which must not include
// any version or internal suffix. This is what we present to users and show in error messages.
func (t FullTextParserType) SQLName() string {
	switch t {
	case FullTextParserTypeStandardV1:
		return "STANDARD"
	case FullTextParserTypeMultilingualV1:
		return "MULTILINGUAL"
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
	default:
		return FullTextParserTypeInvalid
	}
}

// FullTextIndexInfo is the information of FULLTEXT index of a column.
type FullTextIndexInfo struct {
	ParserType FullTextParserType `json:"parser_type"`
	// TODO: Add other options
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
	ConditionExprString string             `json:"condition_expr_string"`   // ConditionExprString is the string representation of the partial index condition.
	AffectColumn        []*IndexColumn     `json:"affect_column,omitempty"` // AffectColumn is the columns related to the index.
	// Version of global index key format, only used for non-clustered, non-unique global indexes.
	// 0=legacy/unique/clustered,
	// 1=v1 non-unique non-clustered with partition ID in key and value.
	// 2=v2 non-unique non-clustered with partition ID in key only (TODO).
	GlobalIndexVersion uint8 `json:"global_index_version,omitempty"`
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

// IsColumnarIndex checks whether the index is a columnar index.
// Columnar index only exists in TiFlash, no actual index data need to be written to KV layer.
func (index *IndexInfo) IsColumnarIndex() bool {
	return index.VectorInfo != nil || index.InvertedInfo != nil || index.FullTextInfo != nil
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
