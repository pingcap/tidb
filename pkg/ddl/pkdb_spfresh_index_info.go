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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// buildSPFreshVectorInfoWithCheck validates and builds the metadata for an
// SPFresh vector index. Keeping this path separate from the HNSW builder makes
// the shared index plumbing independent of SPFresh-specific options.
func buildSPFreshVectorInfoWithCheck(indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption,
	tblInfo *model.TableInfo, duplicateCheckScope vectorIndexDuplicateCheckScope) (*model.VectorIndexInfo, string, error) {
	if len(indexPartSpecifications) != 1 {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("a vector distance function must be specified")
	}

	idxPart := indexPartSpecifications[0]
	f, ok := idxPart.Expr.(*ast.FuncCallExpr)
	if !ok {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("a vector distance function must be specified")
	}
	if len(f.Args) == 0 {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("a vector distance function must have exactly one argument")
	}

	defaults := model.DefaultSPFreshVectorRuntimeConfig()
	runtimeConfig := &defaults
	var (
		indexOptions      *model.SPFreshVectorIndexOptions
		searchConfig      *model.SPFreshVectorSearchConfig
		storedColumnNames []pmodel.CIStr
	)
	if indexOption != nil && indexOption.VectorIndexParam != nil {
		var err error
		var parsed model.SPFreshVectorRuntimeConfig
		indexOptions, searchConfig, parsed, err = model.ParseSPFreshVectorIndexParam(*indexOption.VectorIndexParam)
		if err != nil {
			return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(err.Error())
		}
		runtimeConfig = &parsed
	}
	if indexOption != nil && len(indexOption.SPFreshStoredColumns) > 0 {
		storedColumnNames = indexOption.SPFreshStoredColumns
	}
	distanceMetric, ok := model.VectorIndexDistanceMetricByFnName(model.VectorIndexKindSPFresh, f.FnName.L)
	if !ok {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("currently only VEC_L2_DISTANCE, VEC_COSINE_DISTANCE and VEC_NEGATIVE_INNER_PRODUCT is supported in SPFresh vector index")
	}
	colExpr, ok := f.Args[0].(*ast.ColumnNameExpr)
	if !ok {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(fmt.Sprintf("vector column must be specified as the args, but got %v", f.Args[0]))
	}
	colInfo := findColumnByName(colExpr.Name.Name.L, tblInfo)
	if colInfo == nil {
		return nil, "", infoschema.ErrColumnNotExists.GenWithStackByArgs(colExpr.Name.Name, tblInfo.Name)
	}
	if colInfo.FieldType.GetType() != mysql.TypeTiDBVectorFloat32 {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("vector index can only be added on a vector column")
	}
	if len(f.Args) != 1 {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("a vector distance function must have exactly one argument")
	}
	storedColumnIDs, err := buildSPFreshStoredColumnIDs(storedColumnNames, tblInfo, colInfo.ID)
	if err != nil {
		return nil, "", err
	}

	// check duplicated function on the same column
	for _, idx := range tblInfo.Indices {
		if duplicateCheckScope == vectorIndexDuplicateCheckPublicOnly && idx.State != model.StatePublic {
			continue
		}
		if idx.VectorInfo == nil {
			continue
		}
		if idxCol := idx.FindColumnByName(colInfo.Name.L); idxCol == nil {
			continue
		}
		if idx.VectorInfo.DistanceMetric == distanceMetric {
			return nil, "", dbterror.ErrDupKeyName.GenWithStack(
				fmt.Sprintf("vector index '%s' with %s already exist on column %s",
					idx.Name, f.FnName, colInfo.Name))
		}
	}
	if colInfo.FieldType.GetFlen() <= 0 {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("vector index can only be defined on fixed-dimension vector columns")
	}

	exprStr, err := restoreFuncCall(f)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	// It's used for build buildIndexColumns.
	idxPart.Column = &ast.ColumnName{Name: colInfo.Name}
	idxPart.Length = types.UnspecifiedLength

	return &model.VectorIndexInfo{
		Kind:            model.VectorIndexKindSPFresh,
		Dimension:       uint64(colInfo.FieldType.GetFlen()),
		DistanceMetric:  distanceMetric,
		IndexOptions:    indexOptions,
		SearchConfig:    searchConfig,
		RuntimeConfig:   runtimeConfig,
		StoredColumnIDs: storedColumnIDs,
	}, exprStr, nil
}

type vectorIndexDuplicateCheckScope uint8

const (
	vectorIndexDuplicateCheckAll vectorIndexDuplicateCheckScope = iota
	vectorIndexDuplicateCheckPublicOnly
)
