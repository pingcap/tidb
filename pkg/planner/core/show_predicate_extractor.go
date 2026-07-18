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

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/collate"
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
	_ base.ShowPredicateExtractor = &ShowBaseExtractor{}
)

// ShowBaseExtractor is the definition of base extractor for derived predicates.
type ShowBaseExtractor struct {
	ast.ShowStmt

	field string

	fieldPattern string
}

func newShowBaseExtractor(showStatement ast.ShowStmt) base.ShowPredicateExtractor {
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

// ExplainInfo implements the base.ShowPredicateExtractor interface.
func (e *ShowBaseExtractor) ExplainInfo() string {
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
