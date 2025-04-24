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

// ColumnarIndexType is the type of columnar index.
type ColumnarIndexType uint8

const (
	// ColumnarIndexTypeNA means this is not a columnar index.
	ColumnarIndexTypeNA ColumnarIndexType = iota
	// ColumnarIndexTypeInverted is the inverted index type.
	ColumnarIndexTypeInverted
	// ColumnarIndexTypeVector is the vector index type.
	ColumnarIndexTypeVector
)

// IndexInfo provides meta data describing a DB index.
// It corresponds to the statement `CREATE INDEX Name ON Table (Column);`
// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type IndexInfo struct {
	ID            int64              `json:"id"`
	Name          ast.CIStr          `json:"idx_name"` // Index name.
	Table         ast.CIStr          `json:"tbl_name"` // Table name.
	Columns       []*IndexColumn     `json:"idx_cols"` // Index columns.
	State         SchemaState        `json:"state"`
	BackfillState BackfillState      `json:"backfill_state"`
	Comment       string             `json:"comment"`        // Comment
	Tp            ast.IndexType      `json:"index_type"`     // Index type: Btree, Hash, Rtree or Vector
	Unique        bool               `json:"is_unique"`      // Whether the index is unique.
	Primary       bool               `json:"is_primary"`     // Whether the index is primary key.
	Invisible     bool               `json:"is_invisible"`   // Whether the index is invisible.
	Global        bool               `json:"is_global"`      // Whether the index is global.
	MVIndex       bool               `json:"mv_index"`       // Whether the index is multivalued index.
	VectorInfo    *VectorIndexInfo   `json:"vector_index"`   // VectorInfo is the vector index information.
	InvertedInfo  *InvertedIndexInfo `json:"inverted_index"` // InvertedInfo is the inverted index information.
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
	return index.VectorInfo != nil || index.InvertedInfo != nil
}

// GetColumnarIndexType returns the type of columnar index.
func (index *IndexInfo) GetColumnarIndexType() ColumnarIndexType {
	if index.VectorInfo != nil {
		return ColumnarIndexTypeVector
	}
	if index.InvertedInfo != nil {
		return ColumnarIndexTypeInverted
	}
	return ColumnarIndexTypeNA
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
