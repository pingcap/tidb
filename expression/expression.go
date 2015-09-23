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

package expression

import (
	"github.com/pingcap/tidb/context"
)

// Expression is an interface for expression.
// See https://dev.mysql.com/doc/refman/5.7/en/expressions.html
type Expression interface {
	// Clone clones another Expression.
	Clone() Expression
	// Eval evaluates expression.
	Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error)
	// IsStatic returns whether this expression can be evaluated statically or not.
	// A Static expression can be evaluated without ctx and args.
	IsStatic() bool
	// String returns the presentation of the expression.
	String() string

	// Accept calls visitor's specific function for its type.
	// It represents a visitor pattern.
	Accept(v Visitor) (Expression, error)
}
