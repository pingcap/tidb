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
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/expression"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	// make sure mock.MockInfoschema is initialized to make sure the test pass
	_ "github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
)

// CopContext contains the information that is needed when building a coprocessor request.
// It is unchanged after initialization.
type CopContext interface {
	GetBase() *CopContextBase
	IndexColumnOutputOffsets(idxID int64) []int
	IndexInfo(idxID int64) *model.IndexInfo
}

// CopContextBase contains common fields for CopContextSingleIndex and CopContextMultiIndex.
type CopContextBase struct {
	TableInfo      *model.TableInfo
	PrimaryKeyInfo *model.IndexInfo
	ExprCtx        exprctx.BuildContext
	DistSQLCtx     *distsqlctx.DistSQLContext
	PushDownFlags  uint64
	RequestSource  string

	ColumnInfos []*model.ColumnInfo
	FieldTypes  []*types.FieldType

	ExprColumnInfos             []*expression.Column
	HandleOutputOffsets         []int
	VirtualColumnsOutputOffsets []int
	VirtualColumnsFieldTypes    []*types.FieldType
}

// CopContextSingleIndex is the coprocessor context for single index.
type CopContextSingleIndex struct {
	*CopContextBase

	idxInfo             *model.IndexInfo
	idxColOutputOffsets []int
}

// CopContextMultiIndex is the coprocessor context for multiple indexes.
type CopContextMultiIndex struct {
	*CopContextBase

	allIndexInfos       []*model.IndexInfo
	idxColOutputOffsets [][]int
}

// NewCopContextBase creates a CopContextBase.
func NewCopContextBase(
	exprCtx exprctx.BuildContext,
	distSQLCtx *distsqlctx.DistSQLContext,
	pushDownFlags uint64,
	tblInfo *model.TableInfo,
	idxCols []*model.IndexColumn,
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

	expColInfos, _, err := expression.ColumnInfos2ColumnsAndNames(exprCtx,
		model.CIStr{} /* unused */, tblInfo.Name, colInfos, tblInfo)
	if err != nil {
		return nil, err
	}
	hdColOffsets := resolveIndicesForHandle(expColInfos, handleIDs)
	vColOffsets, vColFts := collectVirtualColumnOffsetsAndTypes(exprCtx.GetEvalCtx(), expColInfos)

	return &CopContextBase{
		TableInfo:                   tblInfo,
		PrimaryKeyInfo:              primaryIdx,
		ExprCtx:                     exprCtx,
		DistSQLCtx:                  distSQLCtx,
		PushDownFlags:               pushDownFlags,
		RequestSource:               requestSource,
		ColumnInfos:                 colInfos,
		FieldTypes:                  fieldTps,
		ExprColumnInfos:             expColInfos,
		HandleOutputOffsets:         hdColOffsets,
		VirtualColumnsOutputOffsets: vColOffsets,
		VirtualColumnsFieldTypes:    vColFts,
	}, nil
}

// NewCopContext creates a CopContext.
func NewCopContext(
	exprCtx exprctx.BuildContext,
	distSQLCtx *distsqlctx.DistSQLContext,
	pushDownFlags uint64,
	tblInfo *model.TableInfo,
	allIdxInfo []*model.IndexInfo,
	requestSource string,
) (CopContext, error) {
	if len(allIdxInfo) == 1 {
		return NewCopContextSingleIndex(exprCtx, distSQLCtx, pushDownFlags, tblInfo, allIdxInfo[0], requestSource)
	}
	return NewCopContextMultiIndex(exprCtx, distSQLCtx, pushDownFlags, tblInfo, allIdxInfo, requestSource)
}

// NewCopContextSingleIndex creates a CopContextSingleIndex.
func NewCopContextSingleIndex(
	exprCtx exprctx.BuildContext,
	distSQLCtx *distsqlctx.DistSQLContext,
	pushDownFlags uint64,
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	requestSource string,
) (*CopContextSingleIndex, error) {
	base, err := NewCopContextBase(exprCtx, distSQLCtx, pushDownFlags, tblInfo, idxInfo.Columns, requestSource)
	if err != nil {
		return nil, err
	}
	idxOffsets := resolveIndicesForIndex(base.ExprColumnInfos, idxInfo, tblInfo)
	return &CopContextSingleIndex{
		CopContextBase:      base,
		idxInfo:             idxInfo,
		idxColOutputOffsets: idxOffsets,
	}, nil
}

// GetBase implements the CopContext interface.
func (c *CopContextSingleIndex) GetBase() *CopContextBase {
	return c.CopContextBase
}

// IndexColumnOutputOffsets implements the CopContext interface.
func (c *CopContextSingleIndex) IndexColumnOutputOffsets(_ int64) []int {
	return c.idxColOutputOffsets
}

// IndexInfo implements the CopContext interface.
func (c *CopContextSingleIndex) IndexInfo(_ int64) *model.IndexInfo {
	return c.idxInfo
}

// NewCopContextMultiIndex creates a CopContextMultiIndex.
func NewCopContextMultiIndex(
	exprCtx exprctx.BuildContext,
	distSQLCtx *distsqlctx.DistSQLContext,
	pushDownFlags uint64,
	tblInfo *model.TableInfo,
	allIdxInfo []*model.IndexInfo,
	requestSource string,
) (*CopContextMultiIndex, error) {
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

	base, err := NewCopContextBase(exprCtx, distSQLCtx, pushDownFlags, tblInfo, allIdxCols, requestSource)
	if err != nil {
		return nil, err
	}

	idxOffsets := make([][]int, 0, len(allIdxInfo))
	for _, idxInfo := range allIdxInfo {
		idxOffsets = append(idxOffsets, resolveIndicesForIndex(base.ExprColumnInfos, idxInfo, tblInfo))
	}
	return &CopContextMultiIndex{
		CopContextBase:      base,
		allIndexInfos:       allIdxInfo,
		idxColOutputOffsets: idxOffsets,
	}, nil
}

// GetBase implements the CopContext interface.
func (c *CopContextMultiIndex) GetBase() *CopContextBase {
	return c.CopContextBase
}

// IndexColumnOutputOffsets implements the CopContext interface.
func (c *CopContextMultiIndex) IndexColumnOutputOffsets(indexID int64) []int {
	for i, idxInfo := range c.allIndexInfos {
		if idxInfo.ID == indexID {
			return c.idxColOutputOffsets[i]
		}
	}
	return nil
}

// IndexInfo implements the CopContext interface.
func (c *CopContextMultiIndex) IndexInfo(indexID int64) *model.IndexInfo {
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

func collectVirtualColumnOffsetsAndTypes(ctx expression.EvalContext, cols []*expression.Column) ([]int, []*types.FieldType) {
	var offsets []int
	var fts []*types.FieldType
	for i, col := range cols {
		if col.VirtualExpr != nil {
			offsets = append(offsets, i)
			fts = append(fts, col.GetType(ctx))
		}
	}
	return offsets, fts
}
