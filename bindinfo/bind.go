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

package bindinfo

import "github.com/pingcap/parser/ast"

// HintsSet contains all hints of a query.
type HintsSet struct {
	tableHints map[int64][]*ast.TableOptimizerHint
	indexHints map[int64][]*ast.IndexHint
}

type hintCollector struct {
	tableCounter int64
	indexCounter int64
	HintsSet
}

// CollectHint collects hints for a statement.
func CollectHint(in ast.StmtNode) *HintsSet {
	hc := hintCollector{HintsSet: HintsSet{tableHints: make(map[int64][]*ast.TableOptimizerHint), indexHints: make(map[int64][]*ast.IndexHint)}}
	in.Accept(&hc)
	return &hc.HintsSet
}

func (hc *hintCollector) Enter(in ast.Node) (ast.Node, bool) {
	switch v := in.(type) {
	case *ast.SelectStmt:
		hc.tableHints[hc.tableCounter] = v.TableHints
		hc.tableCounter++
	case *ast.TableName:
		hc.indexHints[hc.indexCounter] = v.IndexHints
		hc.indexCounter++
	}
	return in, false
}

func (hc *hintCollector) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

type hintBinder struct {
	tableCounter int64
	indexCounter int64
	*HintsSet
}

func (hc *hintBinder) Enter(in ast.Node) (ast.Node, bool) {
	switch v := in.(type) {
	case *ast.SelectStmt:
		v.TableHints = hc.tableHints[hc.tableCounter]
		hc.tableCounter++
	case *ast.TableName:
		v.IndexHints = hc.indexHints[hc.indexCounter]
		hc.indexCounter++
	}
	return in, false
}

func (hc *hintBinder) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// BindHint will add hints for originStmt according to the hints in `bindMeta`.
func BindHint(stmt ast.StmtNode, hintsSet *HintsSet) ast.StmtNode {
	hb := hintBinder{HintsSet: hintsSet}
	stmt.Accept(&hb)
	return stmt
}
