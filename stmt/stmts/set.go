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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ stmt.Statement = (*SetStmt)(nil)
	_ stmt.Statement = (*SetCharsetStmt)(nil)
	_ stmt.Statement = (*SetPwdStmt)(nil)
)

// VariableAssignment is a varible assignment struct.
type VariableAssignment struct {
	Name     string
	Value    expression.Expression
	IsGlobal bool
	IsSystem bool

	Text string
}

// getValue gets VariableAssignment value from context.
// See: https://github.com/mysql/mysql-server/blob/5.7/sql/set_var.cc#L679
func (v *VariableAssignment) getValue(ctx context.Context) (interface{}, error) {
	switch vv := v.Value.(type) {
	case *expressions.Ident:
		return vv.O, nil
	default:
		return vv.Eval(ctx, nil)
	}
}

// String implements the fmt.Stringer interface.
func (v *VariableAssignment) String() string {
	if !v.IsSystem {
		return fmt.Sprintf("@%s=%s", v.Name, v.Value.String())
	}

	if v.IsGlobal {
		return fmt.Sprintf("@@global.%s=%s", v.Name, v.Value.String())
	}
	return fmt.Sprintf("@@session.%s=%s", v.Name, v.Value.String())
}

// SetStmt is a statement to assigns values to different types of variables.
// See: https://dev.mysql.com/doc/refman/5.7/en/set-statement.html
type SetStmt struct {
	Variables []*VariableAssignment

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *SetStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *SetStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *SetStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *SetStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *SetStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	log.Debug("Set sys/user variables")

	sessionVars := variable.GetSessionVars(ctx)

	for _, v := range s.Variables {
		// Variable is case insensitive, we use lower case.
		name := strings.ToLower(v.Name)
		if !v.IsSystem {
			// User variable.
			value, err := v.getValue(ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}

			if value == nil {
				delete(sessionVars.Users, name)
			} else {
				sessionVars.Users[name] = fmt.Sprintf("%v", value)
			}
			return nil, nil
		}

		// TODO: should get global sys var from db.
		sysVar := variable.GetSysVar(name)
		if sysVar == nil {
			return nil, errors.Errorf("Unknown system variable '%s'", name)
		}
		if sysVar.Scope == variable.ScopeNone {
			return nil, errors.Errorf("Variable '%s' is a read only variable", name)
		}

		if v.IsGlobal {
			if sysVar.Scope&variable.ScopeGlobal > 0 {
				value, err := v.getValue(ctx)
				if err != nil {
					return nil, errors.Trace(err)
				}
				if value == nil {
					sysVar.Value = ""
				} else {
					// TODO: set global variables in db, now we only change memory global sys var map.
					// TODO: check sys variable type if possible.
					sysVar.Value = fmt.Sprintf("%v", value)
				}
				return nil, nil
			}
			return nil, errors.Errorf("Variable '%s' is a SESSION variable and can't be used with SET GLOBAL", name)
		}
		if sysVar.Scope&variable.ScopeSession > 0 {
			if value, err := v.getValue(ctx); err != nil {
				return nil, errors.Trace(err)
			} else if value == nil {
				sysVar.Value = ""
			} else {
				// TODO: check sys variable type if possible.
				sessionVars.Systems[name] = fmt.Sprintf("%v", value)
			}
			return nil, nil
		}
		return nil, errors.Errorf("Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL", name)
	}

	return nil, nil
}

// SetCharsetStmt is a statement to assign values to character and collation variables.
// See: https://dev.mysql.com/doc/refman/5.7/en/set-statement.html
type SetCharsetStmt struct {
	Charset string
	Collate string

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *SetCharsetStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *SetCharsetStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *SetCharsetStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *SetCharsetStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *SetCharsetStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	// TODO: finish this
	log.Debug("Set charset to ", s.Charset)
	// ctx.Charset = s.Charset
	return nil, nil
}

// SetPwdStmt is a statement to assign a password to user account.
// See: https://dev.mysql.com/doc/refman/5.7/en/set-password.html
type SetPwdStmt struct {
	User     string
	Password string

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *SetPwdStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *SetPwdStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *SetPwdStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *SetPwdStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *SetPwdStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	// TODO: finish this
	return nil, nil
}
