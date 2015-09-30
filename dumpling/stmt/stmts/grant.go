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
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/util/format"
)

/************************************************************************************
 * Grant Statement
 * See: https://dev.mysql.com/doc/refman/5.7/en/grant.html
 ************************************************************************************/
var (
	_ stmt.Statement = (*GrantStmt)(nil)
)

// GrantStmt grants privilege to user account.
type GrantStmt struct {
	Privs      []*coldef.PrivElem
	ObjectType int
	Level      *coldef.GrantLevel
	Users      []*coldef.UserSpecification
	Text       string
}

// Explain implements the stmt.Statement Explain interface.
func (s *GrantStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *GrantStmt) IsDDL() bool {
	return true
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *GrantStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *GrantStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *GrantStmt) Exec(ctx context.Context) (rset.Recordset, error) {
	// Grant for each user
	for _, user := range s.Users {
		// Check if user exists.
		strs := strings.Split(user.User, "@")
		userName := strs[0]
		host := strs[1]
		exists, err := userExists(ctx, userName, host)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !exists {
			return nil, errors.Errorf("Unknown user: %s", user.User)
		}
		// Grant each priv to the user.
		for _, priv := range s.Privs {
			err := s.grantPriv(ctx, priv, user)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	return nil, nil
}

func (s *GrantStmt) grantPriv(ctx context.Context, priv *coldef.PrivElem, user *coldef.UserSpecification) error {
	switch s.Level.Level {
	case coldef.GrantLevelGlobal:
		return s.grantGlobalPriv(ctx, priv, user)
	case coldef.GrantLevelDB:
		return s.grantDBPriv(ctx, priv, user)
	case coldef.GrantLevelTable:
		return s.grantTablePriv(ctx, priv, user)
	default:
		return errors.Errorf("Unknown grant level: %s", s.Level)
	}
}

func composeGlobalPrivUpdate(priv mysql.PrivilegeType) ([]expression.Assignment, error) {
	if priv == mysql.AllPriv {
		assigns := []expression.Assignment{}
		for _, v := range mysql.Priv2UserCol {
			a := expression.Assignment{
				ColName: v,
				Expr:    expression.Value{Val: "Y"},
			}
			assigns = append(assigns, a)
		}
		return assigns, nil
	}
	col, ok := mysql.Priv2UserCol[priv]
	if !ok {
		return nil, errors.Errorf("Unknown priv: %s", priv)
	}
	asgn := expression.Assignment{
		ColName: col,
		Expr:    expression.Value{Val: "Y"},
	}
	return []expression.Assignment{asgn}, nil
}

// Manipulate mysql.user table.
func (s *GrantStmt) grantGlobalPriv(ctx context.Context, priv *coldef.PrivElem, user *coldef.UserSpecification) error {
	asgns, err := composeGlobalPrivUpdate(priv.Priv)
	if err != nil {
		return errors.Trace(err)
	}
	strs := strings.Split(user.User, "@")
	userName := strs[0]
	host := strs[1]
	st := &UpdateStmt{
		TableRefs: composeUserTableRset(),
		List:      asgns,
		Where:     composeUserTableFilter(userName, host),
	}
	_, err = st.Exec(ctx)
	return errors.Trace(err)
}

// Manipulate mysql.db table.
func (s *GrantStmt) grantDBPriv(ctx context.Context, priv *coldef.PrivElem, user *coldef.UserSpecification) error {
	return nil
}

// Manipulate mysql.tables_priv table.
func (s *GrantStmt) grantTablePriv(ctx context.Context, priv *coldef.PrivElem, user *coldef.UserSpecification) error {
	return nil
}
