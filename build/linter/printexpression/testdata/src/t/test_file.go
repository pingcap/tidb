// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package t

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
)

// EmbeddedConstant is a test struct
type EmbeddedConstant struct {
	expression.Constant
}

// String implements ExpressionWithString interface.
func (e *EmbeddedConstant) String() {}

// ExpressionWithString is a test struct
type ExpressionWithString interface {
	expression.Expression
	String()
}

func testFunc() {
	var expr expression.Expression
	var con *expression.Constant
	var conList []*expression.Constant

	var exprWithString ExpressionWithString
	var col *expression.Column
	var embedded *EmbeddedConstant

	fmt.Println(expr)         // want `avoid printing expression directly.*`
	fmt.Println(con)          // want `avoid printing expression directly.*`
	fmt.Printf("%v", conList) // want `avoid printing expression directly.*`
	fmt.Println(exprWithString)
	fmt.Println(col)
	fmt.Println(embedded)
}
