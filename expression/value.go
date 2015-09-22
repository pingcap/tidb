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
	"fmt"

	"github.com/pingcap/tidb/context"
)

var (
	_ Expression = (*Value)(nil)
)

// Value is the expression holding simple value.
type Value struct {
	// Val holds simple value.
	Val interface{}
}

// Clone implements the Expression Clone interface.
func (l Value) Clone() Expression {
	return Value{Val: l.Val}
}

// IsStatic implements the Expression IsStatic interface, always returns true.
func (l Value) IsStatic() bool {
	return true
}

// String implements the Expression String interface.
func (l Value) String() string {
	switch x := l.Val.(type) {
	case nil:
		return "NULL"
	case string:
		return fmt.Sprintf("%q", x)
	default:
		return fmt.Sprintf("%v", l.Val)
	}
}

// Eval implements the Expression Eval interface.
func (l Value) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	return l.Val, nil
}

func (l Value) Accept(v Visitor) (Expression, error) {
	return v.VisitValue(l)
}
