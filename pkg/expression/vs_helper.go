// Copyright 2024 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

var (
	vsDistanceFnNamesLower = map[string]struct{}{
		strings.ToLower(ast.VecL1Distance):           {},
		strings.ToLower(ast.VecL2Distance):           {},
		strings.ToLower(ast.VecCosineDistance):       {},
		strings.ToLower(ast.VecNegativeInnerProduct): {},
	}
)

// VectorSearchExpr defines a minimal Vector Search expression, which is
// a vector distance function, a column to search with, and a reference vector.
type VectorSearchExpr struct {
	DistanceFnName model.CIStr
	Vec            types.VectorFloat32
	Column         *Column
}

// ExtractVectorSearch extracts a VectorSearchExpr from an expression.
// NOTE: not all VectorSearch functions are supported by the index. The caller
// needs to check the distance function name.
func ExtractVectorSearch(expr Expression) (*VectorSearchExpr, error) {
	x, ok := expr.(*ScalarFunction)
	if !ok {
		return nil, nil
	}

	if _, isVecFn := vsDistanceFnNamesLower[x.FuncName.L]; !isVecFn {
		return nil, nil
	}

	args := x.GetArgs()
	if len(args) != 2 {
		return nil, errors.Errorf("internal: expect 2 args for function %s, but got %d", x.FuncName.L, len(args))
	}

	// One arg must be a vector column ref, and one arg must be a vector constant.
	// Note: this must be run after constant folding.

	var vectorConstant *Constant = nil
	var vectorColumn *Column = nil
	nVectorColumns := 0
	nVectorConstants := 0
	for _, arg := range args {
		if v, ok := arg.(*Column); ok {
			if v.RetType.GetType() != mysql.TypeTiDBVectorFloat32 {
				break
			}
			vectorColumn = v
			nVectorColumns++
		} else if v, ok := arg.(*Constant); ok {
			if v.RetType.GetType() != mysql.TypeTiDBVectorFloat32 {
				break
			}
			vectorConstant = v
			nVectorConstants++
		}
	}
	if nVectorColumns != 1 || nVectorConstants != 1 {
		return nil, nil
	}

	// All check passed.
	if vectorConstant.Value.Kind() != types.KindVectorFloat32 {
		return nil, errors.Errorf("internal: expect vectorFloat32 constant, but got %s", vectorConstant.Value.String())
	}

	return &VectorSearchExpr{
		DistanceFnName: x.FuncName,
		Vec:            vectorConstant.Value.GetVectorFloat32(),
		Column:         vectorColumn,
	}, nil
}
