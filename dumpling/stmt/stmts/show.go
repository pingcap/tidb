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

package stmts

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/sessionctx/db"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/format"
)

var _ stmt.Statement = (*ShowStmt)(nil)

// ShowStmt is a statement to provide information about databases, tables, columns and so on.
// See: https://dev.mysql.com/doc/refman/5.7/en/show.html
type ShowStmt struct {
	Target     int // Databases/Tables/Columns/....
	DBName     string
	TableIdent table.Ident // Used for showing columns.
	ColumnName string      // Used for `desc table column`.
	Flag       int         // Some flag parsed from sql, such as FULL.
	Full       bool

	// Used by show variables
	GlobalScope bool
	Pattern     *expression.PatternLike
	Where       expression.Expression

	// Used by show grants
	User string

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *ShowStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *ShowStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *ShowStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *ShowStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *ShowStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	r := &plans.ShowPlan{
		Target:      s.Target,
		DBName:      s.getDBName(ctx),
		TableName:   s.TableIdent.Name.O,
		ColumnName:  s.ColumnName,
		Flag:        s.Flag,
		Full:        s.Full,
		GlobalScope: s.GlobalScope,
		Pattern:     s.Pattern,
		Where:       s.Where,
		User:        s.User,
	}
	if s.Target == stmt.ShowGrants && len(s.User) == 0 {
		r.User = variable.GetSessionVars(ctx).User
	}
	return rsets.Recordset{Ctx: ctx, Plan: r}, nil
}

func (s *ShowStmt) getDBName(ctx context.Context) string {
	if len(s.DBName) > 0 {
		return s.DBName
	}

	// maybe db.table format
	if len(s.TableIdent.Schema.O) > 0 {
		return s.TableIdent.Schema.O
	}

	// try use current db name if possible.
	return db.GetCurrentSchema(ctx)
}

// SetCondition binds like or where expression.
func (s *ShowStmt) SetCondition(expr interface{}) {
	switch x := expr.(type) {
	case *expression.PatternLike:
		s.Pattern = x
	case expression.Expression:
		s.Where = x
	}
}
