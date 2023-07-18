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

package knownbugs

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/connection"
)

// KnownBug is a function to check whether a bug is known.
type KnownBug = func(*Dustbin) bool

// NodeCb is a callback function for ast.Node.
type NodeCb = func(ast.Node) (ast.Node, bool)

// Visitor is a visitor for ast.Node.
type Visitor struct {
	enterFunc NodeCb
	leaveFunc NodeCb
}

// NewVisitor returns a new visitor.
func NewVisitor() Visitor {
	return Visitor{
		enterFunc: emptyNodeCb,
		leaveFunc: emptyNodeCb,
	}
}

var (
	// Bugs is a map of known bugs.
	Bugs        = make(map[string]KnownBug)
	emptyNodeCb = func(in ast.Node) (ast.Node, bool) {
		return in, true
	}
)

// Dustbin is putted into known bug.
type Dustbin struct {
	PivotRows map[string]*connection.QueryItem

	Stmts []ast.Node

	// may add some configs and contexts
}

// NewDustbin returns a new dustbin.
func NewDustbin(stmts []ast.Node, pivots map[string]*connection.QueryItem) Dustbin {
	return Dustbin{Stmts: stmts, PivotRows: pivots}
}

// IsKnownBug checks whether a bug is known.
func (d *Dustbin) IsKnownBug() bool {
	if len(d.Stmts) == 0 {
		panic(errors.New("empty statements in dustbin"))
	}
	for k, b := range Bugs {
		if b(d) {
			log.L().Info(fmt.Sprintf("this bug has been found: %s", k))
			return true
		}
	}
	return false
}

func init() {
	Bugs["issue16788"] = issue16788
}

// Enter implements ast.Visitor interface.
func (v *Visitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	return v.enterFunc(in)
}

// Leave implements ast.Visitor interface.
func (v *Visitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return v.leaveFunc(in)
}

// SetEnter sets the enter function.
func (v *Visitor) SetEnter(e NodeCb) {
	v.enterFunc = e
}

// SetLeave sets the leave function.
func (v *Visitor) SetLeave(e NodeCb) {
	v.leaveFunc = e
}

// ClearEnter clears the enter function.
func (v *Visitor) ClearEnter() {
	v.enterFunc = emptyNodeCb
}

// ClearLeave clears the leave function.
func (v *Visitor) ClearLeave() {
	v.leaveFunc = emptyNodeCb
}
