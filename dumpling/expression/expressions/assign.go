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

package expressions

import (
	"fmt"

	"github.com/pingcap/tidb/expression"
)

// Assignment is the expression for assignment, like a = 1.
type Assignment struct {
	// ColName is the variable name we want to set.
	ColName string
	// Expr is the expression assigning to ColName.
	Expr expression.Expression
}

// String returns Assignment representation.
func (a *Assignment) String() string {
	return fmt.Sprintf("%s=%s", a.ColName, a.Expr)
}
