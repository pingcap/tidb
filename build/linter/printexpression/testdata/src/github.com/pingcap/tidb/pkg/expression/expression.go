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

package expression

import (
	"fmt"
)

// Expression is a test struct.
type Expression interface {
	// StringWithCtx method only is not allowed to be printed directly.
	StringWithCtx()
}

// Constant is a test struct.
type Constant struct{}

// StringWithCtx implements the Expression interface.
func (c *Constant) StringWithCtx() {}

// Column is a test struct
type Column struct{}

// String implements ExpressionWithString interface.
func (c *Column) String() {}

// StringWithCtx implements the Expression interface.
func (c *Column) StringWithCtx() {}

func testFunc() {
	var expr Expression
	var con *Constant
	var conList []*Constant

	var col *Column

	fmt.Println(expr)         // want `avoid printing expression directly.*`
	fmt.Println(con)          // want `avoid printing expression directly.*`
	fmt.Printf("%v", conList) // want `avoid printing expression directly.*`
	fmt.Println(col)
}
