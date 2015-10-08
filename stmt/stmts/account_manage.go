// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/format"
)

/************************************************************************************
 *  Account Management Statements
 *  https://dev.mysql.com/doc/refman/5.7/en/account-management-sql.html
 ************************************************************************************/
var (
	_ stmt.Statement = (*CreateUserStmt)(nil)
	_ stmt.Statement = (*SetPwdStmt)(nil)
)

// CreateUserStmt creates user account.
// See: https://dev.mysql.com/doc/refman/5.7/en/create-user.html
type CreateUserStmt struct {
	IfNotExists bool
	Specs       []*coldef.UserSpecification

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *CreateUserStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *CreateUserStmt) IsDDL() bool {
	return true
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *CreateUserStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *CreateUserStmt) SetText(text string) {
	s.Text = text
}

func composeUserTableFilter(name string, host string) expression.Expression {
	nameMatch := expression.NewBinaryOperation(opcode.EQ, &expression.Ident{CIStr: model.NewCIStr("User")}, &expression.Value{Val: name})
	hostMatch := expression.NewBinaryOperation(opcode.EQ, &expression.Ident{CIStr: model.NewCIStr("Host")}, &expression.Value{Val: host})
	return expression.NewBinaryOperation(opcode.AndAnd, nameMatch, hostMatch)
}

func composeUserTableRset() *rsets.JoinRset {
	return &rsets.JoinRset{
		Left: &rsets.TableSource{
			Source: table.Ident{
				Name:   model.NewCIStr(mysql.UserTable),
				Schema: model.NewCIStr(mysql.SystemDB),
			},
		},
	}
}

func (s *CreateUserStmt) userExists(ctx context.Context, name string, host string) (bool, error) {
	r := composeUserTableRset()
	p, err := r.Plan(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	where := &rsets.WhereRset{
		Src:  p,
		Expr: composeUserTableFilter(name, host),
	}
	p, err = where.Plan(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer p.Close()
	row, err := p.Next(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	return row != nil, nil
}

// Exec implements the stmt.Statement Exec interface.
func (s *CreateUserStmt) Exec(ctx context.Context) (rset.Recordset, error) {
	st := &InsertIntoStmt{
		TableIdent: table.Ident{
			Name:   model.NewCIStr(mysql.UserTable),
			Schema: model.NewCIStr(mysql.SystemDB),
		},
		ColNames: []string{"Host", "User", "Password"},
	}
	values := make([][]expression.Expression, 0, len(s.Specs))
	for _, spec := range s.Specs {
		strs := strings.Split(spec.User, "@")
		userName := strs[0]
		host := strs[1]
		exists, err1 := s.userExists(ctx, userName, host)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		if exists {
			if !s.IfNotExists {
				return nil, errors.Errorf("Duplicate user")
			}
			continue
		}
		value := make([]expression.Expression, 0, 3)
		value = append(value, expression.Value{Val: host})
		value = append(value, expression.Value{Val: userName})
		if spec.AuthOpt.ByAuthString {
			value = append(value, expression.Value{Val: util.EncodePassword(spec.AuthOpt.AuthString)})
		} else {
			// TODO: Maybe we should hash the string here?
			value = append(value, expression.Value{Val: util.EncodePassword(spec.AuthOpt.HashString)})
		}
		values = append(values, value)
	}
	if len(values) == 0 {
		return nil, nil
	}
	st.Lists = values
	_, err := st.Exec(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
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
	// If len(s.User) == 0, use CURRENT_USER()
	strs := strings.Split(s.User, "@")
	userName := strs[0]
	host := strs[1]
	// Update mysql.user
	asgn := expression.Assignment{
		ColName: "Password",
		Expr:    expression.Value{Val: util.EncodePassword(s.Password)},
	}
	st := &UpdateStmt{
		TableRefs: composeUserTableRset(),
		List:      []expression.Assignment{asgn},
		Where:     composeUserTableFilter(userName, host),
	}
	return st.Exec(ctx)
}
