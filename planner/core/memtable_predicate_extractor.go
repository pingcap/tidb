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

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/set"
)

// MemTablePredicateExtractor is used to extract some predicates from `WHERE` clause
// and push the predicates down to the data retrieving on reading memory table stage.
//
// e.g:
// SELECT * FROM tidb_cluster_config WHERE type='tikv' AND address='192.168.1.9:2379'
// We must request all components in the cluster via HTTP API for retrieving
// configurations and filter them by `type/address` columns.
//
// The purpose of defining a `MemTablePredicateExtractor` is to optimize this
// 1. Define a `ClusterConfigTablePredicateExtractor`
// 2. Extract the `type/address` columns on the logic optimizing stage and save them via fields.
// 3. Passing the extractor to the `ClusterConfigReaderExec` executor
// 4. Executor sends requests to the target components instead of all of the components
type MemTablePredicateExtractor interface {
	// extractCols from predicats and returns the remained predicates
	Extract(*expression.Schema, []*types.FieldName, []expression.Expression) (remained []expression.Expression)
}

// extractHelper contains some common utililty functions for all extractor.
// define a individual struct instead of a bunch of unexported functions
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

func (helper extractHelper) extractColEqConsExpr(extractCols map[int64]*types.FieldName, expr *expression.ScalarFunction) (string, types.Datum) {
	args := expr.GetArgs()
	col, isCol := args[0].(*expression.Column)
	if !isCol {
		return "", types.Datum{}
	}
	name, found := extractCols[col.UniqueID]
	if !found {
		return "", types.Datum{}
	}
	// The `rhs` of EQ expression must be a constant
	// SELECT * FROM t1 WHERE c = 'rhs'
	constant, ok := args[1].(*expression.Constant)
	if !ok || constant.DeferredExpr != nil || constant.ParamMarker != nil {
		return "", types.Datum{}
	}
	return name.ColName.L, constant.Value
}

// ClusterConfigTableExtractor is used to extract some predicates of `TIDB_CLUSTER_CONFIG`
type ClusterConfigTableExtractor struct {
	extractHelper

	// SkipRequest means the where cluase always false, we don't need to request any component
	SkipRequest bool

	// NodeTypes represents all components types we should send request to.
	// e.g:
	// 1. SELECT * FROM tidb_cluster_config WHERE type='tikv'
	// 2. SELECT * FROM tidb_cluster_config WHERE type in ('tikv', 'tidb')
	NodeTypes set.StringSet

	// Addresses represents all components addresses we should send request to.
	// e.g:
	// 1. SELECT * FROM tidb_cluster_config WHERE address='192.168.1.7:2379'
	// 2. SELECT * FROM tidb_cluster_config WHERE type in ('192.168.1.7:2379', '192.168.1.9:2379')
	Addresses set.StringSet
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *ClusterConfigTableExtractor) Extract(schema *expression.Schema, names []*types.FieldName, predicates []expression.Expression) []expression.Expression {
	remained := make([]expression.Expression, 0, len(predicates))
	// All columns can be pushed down to the memory table `TIDB_CLUSTER_CONFIG`
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
	// We don't support change the schema of memory table, so something must
	// be wrong if the `type/address` columns cannot be found in the schema.
	// The purpose of the following assert is used to make test failure because
	// it will never happen if our release passing all test cases.
	if len(extractCols) != 2 {
		panic(fmt.Sprintf("push down columns `type/address` not found in schema, got: %+v", extractCols))
	}

	e.NodeTypes = set.NewStringSet()
	e.Addresses = set.NewStringSet()

	// We should use INTERSECTION of sets because of the predicates is CNF array
	for _, expr := range predicates {
		var extracted bool
		switch x := expr.(type) {
		case *expression.ScalarFunction:
			switch x.FuncName.L {
			case ast.EQ:
				colName, datum := e.extractColEqConsExpr(extractCols, x)
				switch colName {
				case ColNameType:
					tp := datum.GetString()
					// SELECT * FROM tidb_cluster_config WHERE type='a' AND type='b'
					if len(e.NodeTypes) > 0 && !e.NodeTypes.Exist(tp) {
						e.NodeTypes = set.NewStringSet()
						e.SkipRequest = true
						return nil
					}
					e.NodeTypes.Insert(tp)
				case ColNameAddress:
					addr := datum.GetString()
					// SELECT * FROM tidb_cluster_config WHERE address='a' AND address='b'
					if len(e.Addresses) > 0 && !e.Addresses.Exist(addr) {
						e.Addresses = set.NewStringSet()
						e.SkipRequest = true
						return nil
					}
					e.Addresses.Insert(addr)
				}
				extracted = colName != ""
			case ast.In:
				colName, datums := e.extractColInConsExpr(extractCols, x)
				switch colName {
				case ColNameType:
					tmpNodeTypes := set.NewStringSet()
					for _, datum := range datums {
						tmpNodeTypes.Insert(datum.GetString())
					}
					if len(e.NodeTypes) > 0 {
						e.NodeTypes = e.NodeTypes.Intersection(tmpNodeTypes)
						if len(e.NodeTypes) == 0 {
							e.SkipRequest = true
							return nil
						}
					} else {
						e.NodeTypes = tmpNodeTypes
					}
				case ColNameAddress:
					tmpAddresses := set.NewStringSet()
					for _, datum := range datums {
						tmpAddresses.Insert(datum.GetString())
					}
					if len(e.Addresses) > 0 {
						e.Addresses = e.Addresses.Intersection(tmpAddresses)
						if len(e.Addresses) == 0 {
							e.SkipRequest = true
							return nil
						}
					} else {
						e.Addresses = tmpAddresses
					}
				}
				extracted = colName != ""
			}
		}
		if !extracted {
			remained = append(remained, expr)
		}
	}
	return remained
}
