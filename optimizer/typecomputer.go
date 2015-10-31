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
// See the License for the specific language governing permissions and
// limitations under the License.

package optimizer

import "github.com/pingcap/tidb/ast"

// typeComputer is an ast Visitor that
// Compute types for ast.ExprNode.
type typeComputer struct {
	err error
}

func (v *typeComputer) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	return in, false
}

func (v *typeComputer) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}
