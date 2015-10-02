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
	"fmt"
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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/db"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/table"
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
		switch s.Level.Level {
		case coldef.GrantLevelDB:
			err := s.checkAndInitDBPriv(ctx, userName, host)
			if err != nil {
				return nil, errors.Trace(err)
			}
		case coldef.GrantLevelTable:
			err := s.checkAndInitTablePriv(ctx, userName, host)
			if err != nil {
				return nil, errors.Trace(err)
			}
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

func (s *GrantStmt) checkAndInitDBPriv(ctx context.Context, user string, host string) error {
	db, err := s.getTargetSchema(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	ok, err := dbUserExists(ctx, user, host, db.Name.O)
	if err != nil {
		return errors.Trace(err)
	}
	if ok {
		return nil
	}
	// Entry does not exists for user/host/db. Insert a new entry.
	return initDBPrivEntry(ctx, user, host, db.Name.O)
}

func (s *GrantStmt) checkAndInitTablePriv(ctx context.Context, user string, host string) error {
	db, tbl, err := s.getTargetSchemaAndTable(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	ok, err := tableUserExists(ctx, user, host, db.Name.O, tbl.TableName().O)
	if err != nil {
		return errors.Trace(err)
	}
	if ok {
		return nil
	}
	// Entry does not exists for user/host/db. Insert a new entry.
	return initTablePrivEntry(ctx, user, host, db.Name.O, tbl.TableName().O)
}

func initDBPrivEntry(ctx context.Context, user string, host string, db string) error {
	st := &InsertIntoStmt{
		TableIdent: table.Ident{
			Name:   model.NewCIStr(mysql.DBTable),
			Schema: model.NewCIStr(mysql.SystemDB),
		},
		ColNames: []string{"Host", "User", "DB"},
	}
	values := make([][]expression.Expression, 0, 1)
	value := make([]expression.Expression, 0, 3)
	value = append(value, &expression.Value{Val: host})
	value = append(value, &expression.Value{Val: user})
	value = append(value, &expression.Value{Val: db})
	values = append(values, value)
	st.Lists = values
	_, err := st.Exec(ctx)
	return errors.Trace(err)
}

func initTablePrivEntry(ctx context.Context, user string, host string, db string, tbl string) error {
	st := &InsertIntoStmt{
		TableIdent: table.Ident{
			Name:   model.NewCIStr(mysql.TablePrivTable),
			Schema: model.NewCIStr(mysql.SystemDB),
		},
		ColNames: []string{"Host", "User", "DB", "Table_name"},
	}
	values := make([][]expression.Expression, 0, 1)
	value := make([]expression.Expression, 0, 3)
	value = append(value, &expression.Value{Val: host})
	value = append(value, &expression.Value{Val: user})
	value = append(value, &expression.Value{Val: db})
	value = append(value, &expression.Value{Val: tbl})
	values = append(values, value)
	st.Lists = values
	_, err := st.Exec(ctx)
	return errors.Trace(err)
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

func composeDBPrivUpdate(priv mysql.PrivilegeType) ([]expression.Assignment, error) {
	if priv == mysql.AllPriv {
		assigns := []expression.Assignment{}
		for _, p := range mysql.AllDBPrivs {
			v, ok := mysql.Priv2UserCol[p]
			if !ok {
				return nil, errors.Errorf("Unknown db privilege %s", priv)
			}
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

func composeTablePrivUpdate(ctx context.Context, priv mysql.PrivilegeType, name string, host string, db string, tbl string) ([]expression.Assignment, error) {
	var (
		newTablePriv  string
		newColumnPriv string
	)
	if priv == mysql.AllPriv {
		for _, p := range mysql.AllTablePrivs {
			v, ok := mysql.Priv2SetStr[p]
			if !ok {
				return nil, errors.Errorf("Unknown table privilege %s", p)
			}
			if len(newTablePriv) == 0 {
				newTablePriv = v
			} else {
				newTablePriv = fmt.Sprintf("%s,%s", newTablePriv, v)
			}
		}
		for _, p := range mysql.AllColumnPrivs {
			v, ok := mysql.Priv2SetStr[p]
			if !ok {
				return nil, errors.Errorf("Unknown column privilege %s", p)
			}
			if len(newColumnPriv) == 0 {
				newColumnPriv = v
			} else {
				newColumnPriv = fmt.Sprintf("%s,%s", newColumnPriv, v)
			}

		}
	} else {
		currTablePriv, currColumnPriv, err := getTablePriv(ctx, name, host, db, tbl)
		if err != nil {
			return nil, errors.Trace(err)
		}
		p, ok := mysql.Priv2SetStr[priv]
		if !ok {
			return nil, errors.Errorf("Unknown priv: %s", priv)
		}
		if len(currTablePriv) == 0 {
			newTablePriv = p
		} else {
			newTablePriv = fmt.Sprintf("%s,%s", currTablePriv, p)
		}
		for _, cp := range mysql.AllColumnPrivs {
			if priv == cp {
				if len(currColumnPriv) == 0 {
					newColumnPriv = p
				} else {
					newColumnPriv = fmt.Sprintf("%s,%s", currColumnPriv, p)
				}
				break
			}
		}
	}
	t := expression.Assignment{
		ColName: "Table_priv",
		Expr:    expression.Value{Val: newTablePriv},
	}
	c := expression.Assignment{
		ColName: "Column_priv",
		Expr:    expression.Value{Val: newColumnPriv},
	}
	g := expression.Assignment{
		ColName: "Grantor",
		Expr:    expression.Value{Val: variable.GetSessionVars(ctx).User},
	}
	assigns := []expression.Assignment{t, c, g}
	return assigns, nil
}

func composeDBTableFilter(name string, host string, db string) expression.Expression {
	dbMatch := expression.NewBinaryOperation(opcode.EQ, &expression.Ident{CIStr: model.NewCIStr("DB")}, &expression.Value{Val: db})
	return expression.NewBinaryOperation(opcode.AndAnd, composeUserTableFilter(name, host), dbMatch)
}

func composeDBTableRset() *rsets.JoinRset {
	return &rsets.JoinRset{
		Left: &rsets.TableSource{
			Source: table.Ident{
				Name:   model.NewCIStr(mysql.DBTable),
				Schema: model.NewCIStr(mysql.SystemDB),
			},
		},
	}
}

func composeTablePrivFilter(name string, host string, db string, tbl string) expression.Expression {
	tblMatch := expression.NewBinaryOperation(opcode.EQ, &expression.Ident{CIStr: model.NewCIStr("Table_name")}, &expression.Value{Val: tbl})
	return expression.NewBinaryOperation(opcode.AndAnd, composeDBTableFilter(name, host, db), tblMatch)
}

func composeTablePrivRset() *rsets.JoinRset {
	return &rsets.JoinRset{
		Left: &rsets.TableSource{
			Source: table.Ident{
				Name:   model.NewCIStr(mysql.TablePrivTable),
				Schema: model.NewCIStr(mysql.SystemDB),
			},
		},
	}
}

func dbUserExists(ctx context.Context, name string, host string, db string) (bool, error) {
	r := composeDBTableRset()
	p, err := r.Plan(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	where := &rsets.WhereRset{
		Src:  p,
		Expr: composeDBTableFilter(name, host, db),
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

func tableUserExists(ctx context.Context, name string, host string, db string, tbl string) (bool, error) {
	r := composeTablePrivRset()
	p, err := r.Plan(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	where := &rsets.WhereRset{
		Src:  p,
		Expr: composeTablePrivFilter(name, host, db, tbl),
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

func getTablePriv(ctx context.Context, name string, host string, db string, tbl string) (string, string, error) {
	r := composeTablePrivRset()
	p, err := r.Plan(ctx)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	where := &rsets.WhereRset{
		Src:  p,
		Expr: composeTablePrivFilter(name, host, db, tbl),
	}
	p, err = where.Plan(ctx)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	defer p.Close()
	row, err := p.Next(ctx)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	var (
		tPriv string
		cPriv string
	)
	if row.Data[6] != nil {
		tablePriv, ok := row.Data[6].(mysql.Set)
		if !ok {
			return "", "", errors.Errorf("Table Priv should be mysql.Set but get %v with type %T", row.Data[6], row.Data[6])
		}
		tPriv = tablePriv.Name
	}
	if row.Data[7] != nil {
		columnPriv, ok := row.Data[7].(mysql.Set)
		if !ok {
			return "", "", errors.Errorf("Column Priv should be mysql.Set but get %v with type %T", row.Data[7], row.Data[7])
		}
		cPriv = columnPriv.Name
	}
	return tPriv, cPriv, nil
}

func (s *GrantStmt) getTargetSchema(ctx context.Context) (*model.DBInfo, error) {
	dbName := s.Level.DBName
	if len(dbName) == 0 {
		// Grant *, user current schema
		dbName = db.GetCurrentSchema(ctx)
	}
	if len(dbName) == 0 {
		return nil, errors.Errorf("Miss DB name in grant db scope privilege.")
	}
	//check if db exists
	schema := model.NewCIStr(dbName)
	is := sessionctx.GetDomain(ctx).InfoSchema()
	db, ok := is.SchemaByName(schema)
	if !ok {
		return nil, errors.Errorf("Unknown schema name: %s", dbName)
	}
	return db, nil
}

func (s *GrantStmt) getTargetSchemaAndTable(ctx context.Context) (*model.DBInfo, table.Table, error) {
	db, err := s.getTargetSchema(ctx)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	name := model.NewCIStr(s.Level.TableName)
	is := sessionctx.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(db.Name, name)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return db, tbl, nil
}

// Manipulate mysql.db table.
func (s *GrantStmt) grantDBPriv(ctx context.Context, priv *coldef.PrivElem, user *coldef.UserSpecification) error {
	db, err := s.getTargetSchema(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	asgns, err := composeDBPrivUpdate(priv.Priv)
	if err != nil {
		return errors.Trace(err)
	}
	strs := strings.Split(user.User, "@")
	userName := strs[0]
	host := strs[1]
	st := &UpdateStmt{
		TableRefs: composeDBTableRset(),
		List:      asgns,
		Where:     composeDBTableFilter(userName, host, db.Name.O),
	}
	_, err = st.Exec(ctx)
	return errors.Trace(err)
}

// Manipulate mysql.tables_priv table.
func (s *GrantStmt) grantTablePriv(ctx context.Context, priv *coldef.PrivElem, user *coldef.UserSpecification) error {
	db, tbl, err := s.getTargetSchemaAndTable(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	strs := strings.Split(user.User, "@")
	userName := strs[0]
	host := strs[1]
	asgns, err := composeTablePrivUpdate(ctx, priv.Priv, userName, host, db.Name.O, tbl.TableName().O)
	if err != nil {
		return errors.Trace(err)
	}
	st := &UpdateStmt{
		TableRefs: composeTablePrivRset(),
		List:      asgns,
		Where:     composeTablePrivFilter(userName, host, db.Name.O, tbl.TableName().O),
	}
	_, err = st.Exec(ctx)
	return errors.Trace(err)
}
