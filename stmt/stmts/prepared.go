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
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ stmt.Statement = (*PreparedStmt)(nil)
	_ stmt.Statement = (*DeallocateStmt)(nil)
	_ stmt.Statement = (*ExecuteStmt)(nil)
)

// PreparedStmt is a statement to prepares a SQL statement which contains placeholders,
// and it is executed with ExecuteStmt and released with DeallocateStmt.
// See: https://dev.mysql.com/doc/refman/5.7/en/prepare.html
type PreparedStmt struct {
	InPrepare bool // true for prepare mode, false for use mode
	Name      string
	ID        uint32 // For binary protocol, there is no Name but only ID
	SQLText   string
	SQLVar    *expression.Variable
	SQLStmt   stmt.Statement // The parsed statement from sql text with placeholder
	Params    []*expression.ParamMarker
	Fields    []*field.ResultField

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *PreparedStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *PreparedStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *PreparedStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *PreparedStmt) SetText(text string) {
	s.Text = text
}

func getPreparedStmtIDKey(id uint32) string {
	return fmt.Sprintf("%d", id)
}

// Exec implements the stmt.Statement Exec interface.
func (s *PreparedStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	if !s.InPrepare {
		// Can not execute a prepared statement in use status.
		// TODO: this is an error?
		return nil, nil
	}

	err = s.checkPreparedStmt(ctx)
	if err != nil {
		return nil, err
	}

	vars := variable.GetSessionVars(ctx)
	if len(s.Name) == 0 {
		s.ID = vars.GetNextPreparedStmtID()
		s.Name = getPreparedStmtIDKey(s.ID)
	}
	return nil, nil
}

// GetSQL get prepared SQL text.
func (s *PreparedStmt) GetSQL(ctx context.Context) (string, error) {
	// TODO: finish this.
	if s.SQLVar == nil {
		return s.SQLText, nil
	}

	// Get var value.
	vars := variable.GetSessionVars(ctx)
	name := strings.ToLower(s.SQLVar.Name)
	sql, ok := vars.Users[name]
	if !ok {
		// TODO: cannot find use var, return error?
		return "", nil
	}
	return sql, nil
}

// checkPreparedStmt checks prepared statement validity.
func (s *PreparedStmt) checkPreparedStmt(ctx context.Context) error {
	if s.SQLStmt == nil {
		return errors.New("Empty prepared statement")
	}
	switch x := s.SQLStmt.(type) {
	case *SelectStmt:
		return s.checkSelect(ctx, x)
	default:
		return nil
	}
}

func (s *PreparedStmt) checkSelect(ctx context.Context, ss *SelectStmt) error {
	_, err := ss.Plan(ctx)
	return err
}

// DeallocateStmt is a statement to release PreparedStmt.
// See: https://dev.mysql.com/doc/refman/5.7/en/deallocate-prepare.html
type DeallocateStmt struct {
	Name string
	ID   uint32 // For binary protocol, there is no Name but only ID.

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *DeallocateStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *DeallocateStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *DeallocateStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *DeallocateStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *DeallocateStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	// Get prepared statement.
	vars := variable.GetSessionVars(ctx)
	if len(s.Name) == 0 {
		s.Name = getPreparedStmtIDKey(s.ID)
	}
	_, ok := vars.PreparedStmts[0]
	if !ok {
		return nil, errors.Errorf("Can not find prepared statement with name %s", s.Name)
	}
	delete(vars.PreparedStmts, 0)
	return nil, nil
}

// ExecuteStmt is a statement to execute PreparedStmt.
// See: https://dev.mysql.com/doc/refman/5.7/en/execute.html
type ExecuteStmt struct {
	Name      string
	ID        uint32 // For binary protocol, there is no Name but only ID
	UsingVars []expression.Expression

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *ExecuteStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *ExecuteStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *ExecuteStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *ExecuteStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *ExecuteStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	// Get prepared statement.
	vars := variable.GetSessionVars(ctx)
	if len(s.Name) == 0 {
		s.Name = getPreparedStmtIDKey(s.ID)
	}
	vs, ok := vars.PreparedStmts[0]
	if !ok {
		return nil, errors.Errorf("Can not find prepared statement with name %s", s.Name)
	}
	ps, ok := vs.(*PreparedStmt)
	if !ok {
		return nil, errors.Errorf("Statement %s is not PreparedStmt, but %T", s.Name, vs)
	}

	// Fill param markers.
	if len(s.UsingVars) != len(ps.Params) {
		return nil, errors.Errorf("Parameter number does not match between prepared statement(%d) and execute statement(%d)", len(ps.Params), len(s.UsingVars))
	}
	for i, v := range s.UsingVars {
		ps.Params[i].Expr = v
	}
	ps.InPrepare = false

	// Run statement.
	return ps.SQLStmt.Exec(ctx)
}
