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

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	vsDistanceFnNamesLower = map[string]struct{}{
		strings.ToLower(ast.VecL1Distance):           {},
		strings.ToLower(ast.VecL2Distance):           {},
		strings.ToLower(ast.VecCosineDistance):       {},
		strings.ToLower(ast.VecNegativeInnerProduct): {},
	}
)

// VSInfo is an easy to use struct for interpreting a VectorSearch expression.
// NOTE: not all VectorSearch functions are supported by the index. The caller
// needs to check the distance function name.
type VSInfo struct {
	DistanceFnName model.CIStr
	FnPbCode       tipb.ScalarFuncSig
	Vec            types.VectorFloat32
	Column         *Column
}

// InterpretVectorSearchExpr try to interpret a VectorSearch expression.
// If interpret successfully, return a VSInfo struct, otherwise return nil.
// NOTE: not all VectorSearch functions are supported by the index. The caller
// needs to check the distance function name.
func InterpretVectorSearchExpr(expr Expression) *VSInfo {
	x, ok := expr.(*ScalarFunction)
	if !ok {
		return nil
	}

	if _, isVecFn := vsDistanceFnNamesLower[x.FuncName.L]; !isVecFn {
		return nil
	}

	args := x.GetArgs()
	// One arg must be a vector column ref, and one arg must be a vector constant.
	var vectorConstant *Constant = nil
	var vectorColumn *Column = nil
	nVectorColumns := 0
	nVectorConstants := 0
	for _, arg := range args {
		if v, ok := arg.(*Column); ok {
			if v.RetType.GetType() != mysql.TypeTiDBVectorFloat32 {
				return nil
			}
			vectorColumn = v
			nVectorColumns++
		} else if v, ok := arg.(*Constant); ok {
			if v.RetType.GetType() != mysql.TypeTiDBVectorFloat32 {
				return nil
			}
			vectorConstant = v
			nVectorConstants++
		}
	}
	if nVectorColumns != 1 || nVectorConstants != 1 {
		return nil
	}

	intest.Assert(vectorConstant.Value.Kind() == types.KindVectorFloat32, "internal: expect vectorFloat32 constant, but got %s", vectorConstant.Value.String())

	return &VSInfo{
		DistanceFnName: x.FuncName,
		FnPbCode:       x.Function.PbCode(),
		Vec:            vectorConstant.Value.GetVectorFloat32(),
		Column:         vectorColumn,
	}
}
