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

package core

import (
	"fmt"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/set"
)

// MemTablePredicateExtractor is used to extract some predicates from `WHERE` clause
// and push the predicates down to the data retrieving on reading memory table stage.
//
// e.g:
// SELECT * FROM cluster_config WHERE type='tikv' AND address='192.168.1.9:2379'
// We must request all components in the cluster via HTTP API for retrieving
// configurations and filter them by `type/address` columns.
//
// The purpose of defining a `MemTablePredicateExtractor` is to optimize this
// 1. Define a `ClusterConfigTablePredicateExtractor`
// 2. Extract the `type/address` columns on the logic optimizing stage and save them via fields.
// 3. Passing the extractor to the `ClusterConfigReaderExec` executor
// 4. Executor sends requests to the target components instead of all of the components
type MemTablePredicateExtractor interface {
	// Extracts predicates which can be pushed down and returns the remained predicates
	Extract(*expression.Schema, []*types.FieldName, []expression.Expression) (remained []expression.Expression)
}

// extractHelper contains some common utililty functions for all extractor.
// define an individual struct instead of a bunch of un-exported functions
// to avoid polluting the global scope of current package.
type extractHelper struct{}

func (helper extractHelper) extractColInConsExpr(extractCols map[int64]*types.FieldName, expr *expression.ScalarFunction) (string, []types.Datum) {
	args := expr.GetArgs()
	col, isCol := args[0].(*expression.Column)
	if !isCol {
		return "", nil
	}
	name, found := extractCols[col.UniqueID]
	if !found {
		return "", nil
	}
	// All expressions in IN must be a constant
	// SELECT * FROM t1 WHERE c IN ('1', '2')
	var results []types.Datum
	for _, arg := range args[1:] {
		constant, ok := arg.(*expression.Constant)
		if !ok || constant.DeferredExpr != nil || constant.ParamMarker != nil {
			return "", nil
		}
		results = append(results, constant.Value)
	}
	return name.ColName.L, results
}

func (helper extractHelper) extractColEqConsExpr(extractCols map[int64]*types.FieldName, expr *expression.ScalarFunction) (string, []types.Datum) {
	args := expr.GetArgs()
	var col *expression.Column
	var colIdx int
	// c = 'rhs'
	// 'lhs' = c
	for i := 0; i < 2; i++ {
		var isCol bool
		col, isCol = args[i].(*expression.Column)
		if isCol {
			colIdx = i
			break
		}
	}
	if col == nil {
		return "", nil
	}

	name, found := extractCols[col.UniqueID]
	if !found {
		return "", nil
	}
	// The `lhs/rhs` of EQ expression must be a constant
	// SELECT * FROM t1 WHERE c='rhs'
	// SELECT * FROM t1 WHERE 'lhs'=c
	constant, ok := args[1-colIdx].(*expression.Constant)
	if !ok || constant.DeferredExpr != nil || constant.ParamMarker != nil {
		return "", nil
	}
	return name.ColName.L, []types.Datum{constant.Value}
}

func (helper extractHelper) intersection(lhs set.StringSet, datums []types.Datum, toLower bool) set.StringSet {
	tmpNodeTypes := set.NewStringSet()
	for _, datum := range datums {
		var s string
		if toLower {
			s = strings.ToLower(datum.GetString())
		} else {
			s = datum.GetString()
		}
		tmpNodeTypes.Insert(s)
	}
	if len(lhs) > 0 {
		return lhs.Intersection(tmpNodeTypes)
	}
	return tmpNodeTypes
}

// ClusterConfigTableExtractor is used to extract some predicates of `cluster_config`
type ClusterConfigTableExtractor struct {
	extractHelper

	// SkipRequest means the where clause always false, we don't need to request any component
	SkipRequest bool

	// NodeTypes represents all components types we should send request to.
	// e.g:
	// 1. SELECT * FROM cluster_config WHERE type='tikv'
	// 2. SELECT * FROM cluster_config WHERE type in ('tikv', 'tidb')
	NodeTypes set.StringSet

	// Addresses represents all components addresses we should send request to.
	// e.g:
	// 1. SELECT * FROM cluster_config WHERE address='192.168.1.7:2379'
	// 2. SELECT * FROM cluster_config WHERE type in ('192.168.1.7:2379', '192.168.1.9:2379')
	Addresses set.StringSet
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *ClusterConfigTableExtractor) Extract(schema *expression.Schema, names []*types.FieldName, predicates []expression.Expression) []expression.Expression {
	remained := make([]expression.Expression, 0, len(predicates))
	// All columns can be pushed down to the memory table `cluster_config`
	const (
		ColNameType    = "type"
		ColNameAddress = "address"
	)
	extractCols := make(map[int64]*types.FieldName)
	for i, name := range names {
		if ln := name.ColName.L; ln == ColNameType || ln == ColNameAddress {
			extractCols[schema.Columns[i].UniqueID] = name
		}
	}
	// We use the column name literal (local constant) to find the column in `names`
	// instead of using a global constant. So the assumption (named `type/address`)
	// maybe not satisfied if the column name has been changed in the future.
	// The purpose of the following assert is used to make sure our assumption doesn't
	// be broken (or hint the author who refactors this part to change here too).
	if len(extractCols) != 2 {
		panic(fmt.Sprintf("push down columns `type/address` not found in schema, got: %+v", extractCols))
	}

	skipRequest := false
	nodeTypes := set.NewStringSet()
	addresses := set.NewStringSet()

	// We should use INTERSECTION of sets because of the predicates is CNF array
	for _, expr := range predicates {
		var colName string
		var datums []types.Datum
		switch x := expr.(type) {
		case *expression.ScalarFunction:
			switch x.FuncName.L {
			case ast.EQ:
				colName, datums = e.extractColEqConsExpr(extractCols, x)
			case ast.In:
				colName, datums = e.extractColInConsExpr(extractCols, x)
			}
		}
		switch colName {
		case ColNameType:
			nodeTypes = e.intersection(nodeTypes, datums, true)
			skipRequest = len(nodeTypes) == 0
		case ColNameAddress:
			addresses = e.intersection(addresses, datums, false)
			skipRequest = len(addresses) == 0
		default:
			remained = append(remained, expr)
		}
		// There are no data if the low-level executor skip request, so the filter can be droped
		if skipRequest {
			remained = remained[:0]
			break
		}
	}
	e.SkipRequest = skipRequest
	e.NodeTypes = nodeTypes
	e.Addresses = addresses
	return remained
}
