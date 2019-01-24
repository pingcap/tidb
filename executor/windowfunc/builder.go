// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package windowfuncs

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
)

// Build builds window functions according to the window functions description.
func Build(sctx sessionctx.Context, windowFuncDesc *aggregation.WindowFuncDesc, ordinal int) (WindowFunc, error) {
	switch windowFuncDesc.Name {
	case ast.WindowFuncRowNumber:
		return buildRowNumber(ordinal)
	}
	return nil, errors.Errorf("not supported window function %s", windowFuncDesc.Name)
}

func buildRowNumber(ordinal int) (WindowFunc, error) {
	return &rowNumber{baseWindowFunc: baseWindowFunc{ordinal: ordinal}}, nil
}
