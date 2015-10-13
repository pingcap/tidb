// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package field

import (
	"fmt"

	"github.com/pingcap/tidb/expression"
)

// Field is used for parsing column name from SQL.
type Field struct {
	Expr   expression.Expression
	AsName string
}

// String implements fmt.Stringer interface.
func (f *Field) String() string {
	if len(f.AsName) > 0 {
		return fmt.Sprintf("%s AS %s", f.Expr, f.AsName)
	}

	return f.Expr.String()
}

// Opt is used for parsing data type option from SQL.
type Opt struct {
	IsUnsigned bool
	IsZerofill bool
}
