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

package core

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/set"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

const (
	fieldKey        = "field"
	tableKey        = "table"
	databaseKey     = "database"
	collationKey    = "collation"
	databaseNameKey = "db_name"
)

var (
	_ ShowPredicateExtractor = &ShowBaseExtractor{}
)

// ShowPredicateExtractor is used to extract some predicates from `PatternLikeOrIlikeExpr` clause
// and push the predicates down to the data retrieving on reading memory table stage when use ShowStmt.
//
// e.g:
// SHOW COLUMNS FROM t LIKE '%abc%'
// We must request all components from the memory table, and filter the result by the PatternLikeOrIlikeExpr predicate.
//
// it is a way to fix https://github.com/pingcap/tidb/issues/29910.
type ShowPredicateExtractor interface {
	// Extract predicates which can be pushed down and returns whether the extractor can extract predicates.
	Extract() bool
	explainInfo() string
	Field() string
	FieldPatternLike() collate.WildcardPattern
}

// ShowBaseExtractor is the definition of base extractor for derived predicates.
type ShowBaseExtractor struct {
	ast.ShowStmt

	field string

	fieldPattern string
}

func newShowBaseExtractor(showStatement ast.ShowStmt) ShowPredicateExtractor {
	return &ShowBaseExtractor{ShowStmt: showStatement}
}

// Extract implements the ShowPredicateExtractor interface.
func (e *ShowBaseExtractor) Extract() bool {
	show := e.ShowStmt
	if show.Pattern != nil && show.Pattern.Pattern != nil {
		pattern := show.Pattern
		switch pattern.Pattern.(type) {
		case *driver.ValueExpr:
			// It is used in `SHOW XXXX in t LIKE `abc``.
			ptn := pattern.Pattern.(*driver.ValueExpr).GetString()
			patValue, patTypes := stringutil.CompilePattern(ptn, pattern.Escape)
			if stringutil.IsExactMatch(patTypes) {
				e.field = strings.ToLower(string(patValue))
				return true
			}
			e.fieldPattern = strings.ToLower(ptn)
			return true
		case *ast.ColumnNameExpr:
			// It is used in `SHOW COLUMNS FROM t LIKE abc`.
			// MySQL do not support this syntax and return the error.
			return false
		}
	} else if show.Column != nil && show.Column.Name.L != "" {
		// it is used in `DESCRIBE t COLUMN`.
		e.field = show.Column.Name.L
		return true
	}
	return false
}

// explainInfo implements the ShowPredicateExtractor interface.
func (e *ShowBaseExtractor) explainInfo() string {
	key := ""
	switch e.ShowStmt.Tp {
	case ast.ShowVariables, ast.ShowColumns:
		key = fieldKey
	case ast.ShowTables, ast.ShowTableStatus:
		key = tableKey
	case ast.ShowDatabases:
		key = databaseKey
	case ast.ShowCollation:
		key = collationKey
	case ast.ShowStatsHealthy:
		key = databaseNameKey
	}

	r := new(bytes.Buffer)
	if len(e.field) > 0 {
		fmt.Fprintf(r, "%s:[%s], ", key, e.field)
	}

	if len(e.fieldPattern) > 0 {
		fmt.Fprintf(r, "%s_pattern:[%s], ", key, e.fieldPattern)
	}

	// remove the last ", " in the message info
	s := r.String()
	if len(s) > 2 {
		return s[:len(s)-2]
	}
	return s
}

// Field will return the variable `field` in ShowBaseExtractor
func (e *ShowBaseExtractor) Field() string {
	return e.field
}

// FieldPatternLike will return compiled collate.WildcardPattern
func (e *ShowBaseExtractor) FieldPatternLike() collate.WildcardPattern {
	if e.fieldPattern == "" {
		return nil
	}
	fieldPatternsLike := collate.GetCollatorByID(collate.CollationName2ID(mysql.UTF8MB4DefaultCollation)).Pattern()
	fieldPatternsLike.Compile(e.fieldPattern, byte('\\'))
	return fieldPatternsLike
}

// ShowStatsMetaExtractor is used to extract some predicates of 'show stats_meta'.
type ShowStatsMetaExtractor struct {
	extractHelper

	// NodeTypes represents all components types we should send request to.
	// e.g:
	// 1. show stats_meta WHERE db_name ='mysql'
	// 2. show stats_meta WHERE db_name in ('mysql', 'test')
	DB set.StringSet

	// Instances represents all components instances we should send request to.
	// e.g:
	// 1. show stats_meta WHERE table_name='test'
	// 2. show stats_meta WHERE table_name in ('t1', 't2')
	Table set.StringSet
}

// Check extract table_name and db_name filter condition for 'show stats_meta where ...' statement.
// Used in Show's logical plan predicate push down.
func (e *ShowStatsMetaExtractor) Check(
	schema *expression.Schema,
	names []*types.FieldName,
	predicates []expression.Expression,
) []expression.Expression {
	remained, _, dbNames := e.extractCol(schema, names, predicates, "db_name", true)
	remained, _, tableNames := e.extractCol(schema, names, remained, "table_name", false)
	e.DB = dbNames
	e.Table = tableNames
	return remained
}

// Extract implements the ShowPredicateExtractor Extract interface
func (*ShowStatsMetaExtractor) Extract() bool {
	return false
}

// explainInfo implements the ShowPredicateExtractor interface.
func (*ShowStatsMetaExtractor) explainInfo() string {
	return ""
}

// Field implements the ShowPredicateExtractor interface.
func (*ShowStatsMetaExtractor) Field() string {
	return ""
}

// FieldPatternLike implements the ShowPredicateExtractor interface.
func (*ShowStatsMetaExtractor) FieldPatternLike() collate.WildcardPattern {
	return nil
}
