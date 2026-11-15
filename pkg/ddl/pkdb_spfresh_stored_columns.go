// Copyright 2015 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	parser_types "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func buildSPFreshStoredColumnIDs(colNames []pmodel.CIStr, tblInfo *model.TableInfo, vectorColumnID int64) ([]int64, error) {
	if len(colNames) == 0 {
		return nil, nil
	}
	seen := make(map[int64]struct{}, len(colNames))
	storedColumnIDs := make([]int64, 0, len(colNames))
	var totalStoredPayloadBytes int
	for _, colName := range colNames {
		colInfo := tblInfo.FindPublicColumnByName(colName.L)
		if colInfo == nil {
			return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(colName, tblInfo.Name)
		}
		if colInfo.ID == vectorColumnID {
			return nil, dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(
				fmt.Sprintf("SPFresh STORING cannot include indexed vector column %s", colInfo.Name.O))
		}
		if _, ok := seen[colInfo.ID]; ok {
			return nil, dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(
				fmt.Sprintf("duplicate SPFresh stored column %s", colInfo.Name.O))
		}
		seen[colInfo.ID] = struct{}{}
		if colInfo.IsGenerated() {
			return nil, dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("SPFresh stored column cannot be a generated column")
		}
		if colInfo.FieldType.IsArray() {
			return nil, dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(
				fmt.Sprintf("SPFresh stored column %s cannot be an ARRAY column", colInfo.Name.O))
		}
		valueBytes, err := validateSPFreshStoredColumnDDL(colInfo)
		if err != nil {
			return nil, err
		}
		columnIDBytes, err := codec.EstimateValueSize(
			types.DefaultStmtNoWarningContext,
			types.NewIntDatum(colInfo.ID),
		)
		if err != nil {
			return nil, err
		}
		totalStoredPayloadBytes += columnIDBytes + valueBytes
		if totalStoredPayloadBytes > spfreshMaxStoredPayloadBytes {
			return nil, dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(
				fmt.Sprintf("SPFresh stored columns worst-case encoded sidecar size %d exceeds limit %d",
					totalStoredPayloadBytes, spfreshMaxStoredPayloadBytes))
		}
		storedColumnIDs = append(storedColumnIDs, colInfo.ID)
	}
	return storedColumnIDs, nil
}

// spfreshColumnDependency describes how an SPFresh index uses a column.
type spfreshColumnDependency int

const (
	spfreshColumnIndependent spfreshColumnDependency = iota
	// The indexed vector column.
	spfreshColumnIndexed
	// The handle column or a column stored inside the index payload.
	spfreshColumnStored
)

// spfreshColumnDependencyOf classifies how an SPFresh index depends on col.
func spfreshColumnDependencyOf(tblInfo *model.TableInfo, col *model.ColumnInfo) spfreshColumnDependency {
	if tblInfo == nil || col == nil {
		return spfreshColumnIndependent
	}
	handleColumnID, _, hasHandleColumn := model.SPFreshHandleColumnInfo(tblInfo)
	dependency := spfreshColumnIndependent
	for _, idxInfo := range tblInfo.Indices {
		if idxInfo == nil || !idxInfo.IsSPFreshIndex() {
			continue
		}
		if idxInfo.HasColumnInIndexColumns(tblInfo, col.ID) {
			return spfreshColumnIndexed
		}
		if hasHandleColumn && handleColumnID == col.ID {
			dependency = spfreshColumnStored
			continue
		}
		for _, storedColumnID := range idxInfo.VectorInfo.StoredColumnIDs {
			if storedColumnID == col.ID {
				dependency = spfreshColumnStored
				break
			}
		}
	}
	return dependency
}

func spfreshIndexDependsOnColumn(tblInfo *model.TableInfo, col *model.ColumnInfo) bool {
	return spfreshColumnDependencyOf(tblInfo, col) != spfreshColumnIndependent
}

func checkSPFreshStoredColumnsForTableCharsetChange(tblInfo *model.TableInfo, toCharset, toCollate string) error {
	for _, col := range tblInfo.Columns {
		if !parser_types.HasCharset(&col.FieldType) {
			continue
		}
		if col.GetCharset() == toCharset && col.GetCollate() == toCollate {
			continue
		}
		if spfreshIndexDependsOnColumn(tblInfo, col) {
			return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("column has SPFresh index, drop SPFresh index first")
		}
	}
	return nil
}
