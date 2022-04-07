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

	"github.com/pingcap/tidb/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/stringutil"
)

var (
	_ ShowPredicateExtractor = &ShowColumnsTableExtractor{}
	_ ShowPredicateExtractor = &ShowTablesTableExtractor{}
	_ ShowPredicateExtractor = &ShowVariablesExtractor{}
)

// ShowPredicateExtractor is used to extract some predicates from `PatternLikeExpr` clause
// and push the predicates down to the data retrieving on reading memory table stage when use ShowStmt.
//
// e.g:
// SHOW COLUMNS FROM t LIKE '%abc%'
// We must request all components from the memory table, and filter the result by the PatternLikeExpr predicate.
//
// it is a way to fix https://github.com/pingcap/tidb/issues/29910.
type ShowPredicateExtractor interface {
	// Extract predicates which can be pushed down and returns whether the extractor can extract predicates.
	Extract(show *ast.ShowStmt) bool
	explainInfo() string
}

// ShowBaseExtractor is the definition of base extractor for derived predicates.
type ShowBaseExtractor struct {
	Field string

	FieldPatterns string
}

// Extract implements the ShowPredicateExtractor interface.
func (e *ShowBaseExtractor) Extract(show *ast.ShowStmt) bool {
	if show.Pattern != nil && show.Pattern.Pattern != nil {
		pattern := show.Pattern
		switch pattern.Pattern.(type) {
		case *driver.ValueExpr:
			// It is used in `SHOW XXXX in t LIKE `abc``.
			ptn := pattern.Pattern.(*driver.ValueExpr).GetString()
			patValue, patTypes := stringutil.CompilePattern(ptn, pattern.Escape)
			if stringutil.IsExactMatch(patTypes) {
				e.Field = strings.ToLower(string(patValue))
				return true
			}
			e.FieldPatterns = strings.ToLower(string(patValue))
			return true
		}
	}
	return false
}

// ShowColumnsTableExtractor is used to extract some predicates of tables table.
type ShowColumnsTableExtractor struct {
	ShowBaseExtractor
}

// Extract implements the MemTablePredicateExtractor Extract interface
func (e *ShowColumnsTableExtractor) Extract(show *ast.ShowStmt) bool {
	if show.Pattern != nil && show.Pattern.Pattern != nil {
		pattern := show.Pattern
		switch pattern.Pattern.(type) {
		case *driver.ValueExpr:
			// It is used in `SHOW COLUMNS FROM t LIKE `abc``.
			ptn := pattern.Pattern.(*driver.ValueExpr).GetString()
			patValue, patTypes := stringutil.CompilePattern(ptn, pattern.Escape)
			if stringutil.IsExactMatch(patTypes) {
				e.Field = strings.ToLower(string(patValue))
				return true
			}
			e.FieldPatterns = strings.ToLower(string(patValue))
			return true
		case *ast.ColumnNameExpr:
			// It is used in `SHOW COLUMNS FROM t LIKE abc`.
			// MySQL do not support this syntax and return the error.
			return false
		}
	} else if show.Column != nil && show.Column.Name.L != "" {
		// it is used in `DESCRIBE t COLUMN`.
		e.Field = show.Column.Name.L
		return true
	}
	return false
}

func (e *ShowColumnsTableExtractor) explainInfo() string {
	r := new(bytes.Buffer)
	if len(e.Field) > 0 {
		r.WriteString(fmt.Sprintf("field:[%s], ", e.Field))
	}

	if len(e.FieldPatterns) > 0 {
		r.WriteString(fmt.Sprintf("field_pattern:[%s], ", e.FieldPatterns))
	}

	// remove the last ", " in the message info
	s := r.String()
	if len(s) > 2 {
		return s[:len(s)-2]
	}
	return s
}

// ShowTablesTableExtractor is used to extract some predicates of tables.
type ShowTablesTableExtractor struct {
	ShowBaseExtractor
}

func (e *ShowTablesTableExtractor) explainInfo() string {
	r := new(bytes.Buffer)
	if len(e.Field) > 0 {
		r.WriteString(fmt.Sprintf("table:[%s], ", e.Field))
	}

	if len(e.FieldPatterns) > 0 {
		r.WriteString(fmt.Sprintf("table_pattern:[%s], ", e.FieldPatterns))
	}

	// remove the last ", " in the message info
	s := r.String()
	if len(s) > 2 {
		return s[:len(s)-2]
	}
	return s
}

// ShowVariablesExtractor is used to extract some predicates of variables.
type ShowVariablesExtractor struct {
	ShowBaseExtractor
}

func (e *ShowVariablesExtractor) explainInfo() string {
	r := new(bytes.Buffer)
	if len(e.Field) > 0 {
		r.WriteString(fmt.Sprintf("variable:[%s], ", e.Field))
	}

	if len(e.FieldPatterns) > 0 {
		r.WriteString(fmt.Sprintf("variable_pattern:[%s], ", e.FieldPatterns))
	}

	// remove the last ", " in the message info
	s := r.String()
	if len(s) > 2 {
		return s[:len(s)-2]
	}
	return s
}
