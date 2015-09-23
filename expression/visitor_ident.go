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

// IdentEvalVisitor converts Ident expression to value expression.
type IdentEvalVisitor struct {
	BaseVisitor
	evalMap map[string]interface{}
}

// NewIdentEvalVisitor creates a new IdentEvalVisitor.
func NewIdentEvalVisitor() *IdentEvalVisitor {
	iev := &IdentEvalVisitor{evalMap: map[string]interface{}{}}
	iev.BaseVisitor.V = iev
	return iev
}

// Set sets ident name with value, it should be called before visiting expression.
func (iev *IdentEvalVisitor) Set(name string, value interface{}) {
	iev.evalMap[name] = value
}

// VisitIdent implements Visitor inferface.
func (iev *IdentEvalVisitor) VisitIdent(i *Ident) (Expression, error) {
	val, ok := iev.evalMap[i.L]
	if ok {
		return Value{Val: val}, nil
	}
	return i, nil
}
