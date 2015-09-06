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
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ stmt.Statement = (*DoStmt)(nil)
)

// DoStmt is a statement to execute the expressions but not get any results.
// See: https://dev.mysql.com/doc/refman/5.7/en/do.html
type DoStmt struct {
	Exprs []expression.Expression

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *DoStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *DoStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *DoStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *DoStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *DoStmt) Exec(ctx context.Context) (rset.Recordset, error) {
	m := map[interface{}]interface{}{}
	for _, expr := range s.Exprs {
		_, err := expr.Eval(ctx, m)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}
