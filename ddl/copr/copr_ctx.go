// Copyright 2023 PingCAP, Inc.
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

package copr

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
)

// CopContext contains the information that is needed when building a coprocessor request.
// It is unchanged after initialization.
type CopContext interface {
	GetBase() *CopContextBase
	IndexColumnOutputOffsets(idxID int64) []int
	IndexInfo(idxID int64) *model.IndexInfo
}

type CopContextBase struct {
	TableInfo      *model.TableInfo
	PrimaryKeyInfo *model.IndexInfo
	SessionContext sessionctx.Context

	RequestSource string

	ColumnInfos []*model.ColumnInfo
	FieldTypes  []*types.FieldType

	ExprColumnInfos             []*expression.Column
	HandleOutputOffsets         []int
	VirtualColumnsOutputOffsets []int
	VirtualColumnsFieldTypes    []*types.FieldType
}

type CopCtxSingleIndex struct {
	*CopContextBase

	idxInfo             *model.IndexInfo
	idxColOutputOffsets []int
}

type CopCtxMultiIndex struct {
	*CopContextBase

	allIndexInfos       []*model.IndexInfo
	idxColOutputOffsets [][]int
}

// NewCopContextBase creates a CopContextBase.
func NewCopContextBase(
	tblInfo *model.TableInfo,
	idxCols []*model.IndexColumn,
	sessCtx sessionctx.Context,
	requestSource string,
) (*CopContextBase, error) {
	var err error
	usedColumnIDs := make(map[int64]struct{}, len(idxCols))
	usedColumnIDs, err = fillUsedColumns(usedColumnIDs, idxCols, tblInfo)
	var handleIDs []int64
	if err != nil {
		return nil, err
	}
	var primaryIdx *model.IndexInfo
	if tblInfo.PKIsHandle {
		pkCol := tblInfo.GetPkColInfo()
		usedColumnIDs[pkCol.ID] = struct{}{}
		handleIDs = []int64{pkCol.ID}
	} else if tblInfo.IsCommonHandle {
		primaryIdx = tables.FindPrimaryIndex(tblInfo)
		handleIDs = make([]int64, 0, len(primaryIdx.Columns))
		for _, pkCol := range primaryIdx.Columns {
			col := tblInfo.Columns[pkCol.Offset]
			handleIDs = append(handleIDs, col.ID)
		}
		usedColumnIDs, err = fillUsedColumns(usedColumnIDs, primaryIdx.Columns, tblInfo)
		if err != nil {
			return nil, err
		}
	}

	// Only collect the columns that are used by the index.
	colInfos := make([]*model.ColumnInfo, 0, len(idxCols))
	fieldTps := make([]*types.FieldType, 0, len(idxCols))
	for i := range tblInfo.Columns {
		col := tblInfo.Columns[i]
		if _, found := usedColumnIDs[col.ID]; found {
			colInfos = append(colInfos, col)
			fieldTps = append(fieldTps, &col.FieldType)
		}
	}

	// Append the extra handle column when _tidb_rowid is used.
	if !tblInfo.HasClusteredIndex() {
		extra := model.NewExtraHandleColInfo()
		colInfos = append(colInfos, extra)
		fieldTps = append(fieldTps, &extra.FieldType)
		handleIDs = []int64{extra.ID}
	}

	expColInfos, _, err := expression.ColumnInfos2ColumnsAndNames(sessCtx,
		model.CIStr{} /* unused */, tblInfo.Name, colInfos, tblInfo)
	if err != nil {
		return nil, err
	}
	hdColOffsets := resolveIndicesForHandle(expColInfos, handleIDs)
	vColOffsets, vColFts := collectVirtualColumnOffsetsAndTypes(expColInfos)

	return &CopContextBase{
		TableInfo:                   tblInfo,
		PrimaryKeyInfo:              primaryIdx,
		SessionContext:              sessCtx,
		RequestSource:               requestSource,
		ColumnInfos:                 colInfos,
		FieldTypes:                  fieldTps,
		ExprColumnInfos:             expColInfos,
		HandleOutputOffsets:         hdColOffsets,
		VirtualColumnsOutputOffsets: vColOffsets,
		VirtualColumnsFieldTypes:    vColFts,
	}, nil
}

func NewCopContext(
	tblInfo *model.TableInfo,
	allIdxInfo []*model.IndexInfo,
	sessCtx sessionctx.Context,
	requestSource string,
) (CopContext, error) {
	if len(allIdxInfo) == 1 {
		return NewCopContextSingleIndex(tblInfo, allIdxInfo[0], sessCtx, requestSource)
	}
	return NewCopContextMultiIndex(tblInfo, allIdxInfo, sessCtx, requestSource)
}

func NewCopContextSingleIndex(
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	sessCtx sessionctx.Context,
	requestSource string,
) (*CopCtxSingleIndex, error) {
	base, err := NewCopContextBase(tblInfo, idxInfo.Columns, sessCtx, requestSource)
	if err != nil {
		return nil, err
	}
	idxOffsets := resolveIndicesForIndex(base.ExprColumnInfos, idxInfo, tblInfo)
	return &CopCtxSingleIndex{
		CopContextBase:      base,
		idxInfo:             idxInfo,
		idxColOutputOffsets: idxOffsets,
	}, nil
}

func (c *CopCtxSingleIndex) GetBase() *CopContextBase {
	return c.CopContextBase
}

func (c *CopCtxSingleIndex) IndexColumnOutputOffsets(_ int64) []int {
	return c.idxColOutputOffsets
}

func (c *CopCtxSingleIndex) IndexInfo(_ int64) *model.IndexInfo {
	return c.idxInfo
}

func NewCopContextMultiIndex(
	tblInfo *model.TableInfo,
	allIdxInfo []*model.IndexInfo,
	sessCtx sessionctx.Context,
	requestSource string,
) (*CopCtxMultiIndex, error) {
	approxColLen := 0
	for _, idxInfo := range allIdxInfo {
		approxColLen += len(idxInfo.Columns)
	}
	distinctOffsets := make(map[int]struct{}, approxColLen)
	allIdxCols := make([]*model.IndexColumn, 0, approxColLen)
	for _, idxInfo := range allIdxInfo {
		for _, idxCol := range idxInfo.Columns {
			if _, found := distinctOffsets[idxCol.Offset]; !found {
				distinctOffsets[idxCol.Offset] = struct{}{}
				allIdxCols = append(allIdxCols, idxCol)
			}
		}
	}

	base, err := NewCopContextBase(tblInfo, allIdxCols, sessCtx, requestSource)
	if err != nil {
		return nil, err
	}

	idxOffsets := make([][]int, 0, len(allIdxInfo))
	for _, idxInfo := range allIdxInfo {
		idxOffsets = append(idxOffsets, resolveIndicesForIndex(base.ExprColumnInfos, idxInfo, tblInfo))
	}
	return &CopCtxMultiIndex{
		CopContextBase:      base,
		allIndexInfos:       allIdxInfo,
		idxColOutputOffsets: idxOffsets,
	}, nil
}

func (c *CopCtxMultiIndex) GetBase() *CopContextBase {
	return c.CopContextBase
}

func (c *CopCtxMultiIndex) IndexColumnOutputOffsets(indexID int64) []int {
	for i, idxInfo := range c.allIndexInfos {
		if idxInfo.ID == indexID {
			return c.idxColOutputOffsets[i]
		}
	}
	return nil
}

func (c *CopCtxMultiIndex) IndexInfo(indexID int64) *model.IndexInfo {
	for _, idxInfo := range c.allIndexInfos {
		if idxInfo.ID == indexID {
			return idxInfo
		}
	}
	return nil
}

func fillUsedColumns(
	usedCols map[int64]struct{},
	idxCols []*model.IndexColumn,
	tblInfo *model.TableInfo,
) (map[int64]struct{}, error) {
	colsToChecks := make([]*model.ColumnInfo, 0, len(idxCols))
	for _, idxCol := range idxCols {
		colsToChecks = append(colsToChecks, tblInfo.Columns[idxCol.Offset])
	}
	for len(colsToChecks) > 0 {
		next := colsToChecks[0]
		colsToChecks = colsToChecks[1:]
		usedCols[next.ID] = struct{}{}
		for depColName := range next.Dependences {
			// Expand the virtual generated columns.
			depCol := model.FindColumnInfo(tblInfo.Columns, depColName)
			if depCol == nil {
				return nil, errors.Trace(errors.Errorf("dependent column %s not found", depColName))
			}
			if _, ok := usedCols[depCol.ID]; !ok {
				colsToChecks = append(colsToChecks, depCol)
			}
		}
	}
	return usedCols, nil
}

func resolveIndicesForIndex(
	outputCols []*expression.Column,
	idxInfo *model.IndexInfo,
	tblInfo *model.TableInfo,
) []int {
	offsets := make([]int, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		hid := tblInfo.Columns[idxCol.Offset].ID
		for j, col := range outputCols {
			if col.ID == hid {
				offsets = append(offsets, j)
				break
			}
		}
	}
	return offsets
}

func resolveIndicesForHandle(cols []*expression.Column, handleIDs []int64) []int {
	offsets := make([]int, 0, len(handleIDs))
	for _, hid := range handleIDs {
		for j, col := range cols {
			if col.ID == hid {
				offsets = append(offsets, j)
				break
			}
		}
	}
	return offsets
}

func collectVirtualColumnOffsetsAndTypes(cols []*expression.Column) ([]int, []*types.FieldType) {
	var offsets []int
	var fts []*types.FieldType
	for i, col := range cols {
		if col.VirtualExpr != nil {
			offsets = append(offsets, i)
			fts = append(fts, col.GetType())
		}
	}
	return offsets, fts
}
