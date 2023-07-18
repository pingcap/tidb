// Copyright 2022 PingCAP, Inc.
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

package operator

import (
	"fmt"
	"math/rand"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

var (
	defaultFuncCallNodeCb = func(cb types.TypedExprNodeGen, this types.OpFuncEval, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
		op := this.(*types.BaseOpFunc)
		argList, err := op.GetArgTable().Filter([]*uint64{nil}, &ret)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		if len(argList) == 0 {
			return nil, parser_driver.ValueExpr{}, fmt.Errorf("cannot find valid param for type(%d) returned", ret)
		}
		arg := argList[rand.Intn(len(argList))]
		var v []parser_driver.ValueExpr
		var args []ast.ExprNode
		for i := 0; i < len(arg)-1; i++ {
			expr, value, err := cb(arg[i])
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			v = append(v, value)
			args = append(args, expr)
		}
		value, err := op.Eval(v...)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		node := &ast.FuncCallExpr{
			FnName: model.NewCIStr(op.GetName()),
			Args:   args,
		}
		return node, value, nil
	}
)
