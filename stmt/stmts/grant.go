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
	"github.com/pingcap/tidb/column"
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

		// If there is no privilege entry in corresponding table, insert a new one.
		// DB scope:		mysql.DB
		// Table scope:		mysql.Tables_priv
		// Column scope:	mysql.Columns_priv
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
			if len(priv.Cols) > 0 {
				// Check column scope privilege entry.
				// TODO: Check validity before insert new entry.
				err1 := s.checkAndInitColumnPriv(ctx, userName, host, priv.Cols)
				if err1 != nil {
					return nil, errors.Trace(err1)
				}
			}
			err2 := s.grantPriv(ctx, priv, user)
			if err2 != nil {
				return nil, errors.Trace(err2)
			}
		}
	}
	return nil, nil
}

// Check if DB scope privilege entry exists in mysql.DB.
// If unexists, insert a new one.
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
	// Entry does not exists for user-host-db. Insert a new entry.
	return initDBPrivEntry(ctx, user, host, db.Name.O)
}

// Check if table scope privilege entry exists in mysql.Tables_priv.
// If unexists, insert a new one.
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
	// Entry does not exists for user-host-db-tbl. Insert a new entry.
	return initTablePrivEntry(ctx, user, host, db.Name.O, tbl.TableName().O)
}

// Check if column scope privilege entry exists in mysql.Columns_priv.
// If unexists, insert a new one.
func (s *GrantStmt) checkAndInitColumnPriv(ctx context.Context, user string, host string, cols []string) error {
	db, tbl, err := s.getTargetSchemaAndTable(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	for _, c := range cols {
		col := column.FindCol(tbl.Cols(), c)
		if col == nil {
			return errors.Errorf("Unknown column: %s", c)
		}
		ok, err := columnPrivEntryExists(ctx, user, host, db.Name.O, tbl.TableName().O, col.Name.O)
		if err != nil {
			return errors.Trace(err)
		}
		if ok {
			continue
		}
		// Entry does not exists for user-host-db-tbl-col. Insert a new entry.
		err = initColumnPrivEntry(ctx, user, host, db.Name.O, tbl.TableName().O, col.Name.O)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Insert a new row into mysql.DB with empty privilege.
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
	value = append(value, &expression.Value{Val: host}) // Host
	value = append(value, &expression.Value{Val: user}) // User
	value = append(value, &expression.Value{Val: db})   // DB
	values = append(values, value)
	st.Lists = values
	_, err := st.Exec(ctx)
	return errors.Trace(err)
}

// Insert a new row into mysql.Tables_priv with empty privilege.
func initTablePrivEntry(ctx context.Context, user string, host string, db string, tbl string) error {
	st := &InsertIntoStmt{
		TableIdent: table.Ident{
			Name:   model.NewCIStr(mysql.TablePrivTable),
			Schema: model.NewCIStr(mysql.SystemDB),
		},
		ColNames: []string{"Host", "User", "DB", "Table_name", "Table_priv", "Column_priv"},
	}
	values := make([][]expression.Expression, 0, 1)
	value := make([]expression.Expression, 0, 3)
	value = append(value, &expression.Value{Val: host}) // Host
	value = append(value, &expression.Value{Val: user}) // User
	value = append(value, &expression.Value{Val: db})   // DB
	value = append(value, &expression.Value{Val: tbl})  // Table_name
	value = append(value, &expression.Value{Val: ""})   // Table_priv
	value = append(value, &expression.Value{Val: ""})   // Column_priv
	values = append(values, value)
	st.Lists = values
	_, err := st.Exec(ctx)
	return errors.Trace(err)
}

// Insert a new row into mysql.Columns_priv with empty privilege.
func initColumnPrivEntry(ctx context.Context, user string, host string, db string, tbl string, col string) error {
	st := &InsertIntoStmt{
		TableIdent: table.Ident{
			Name:   model.NewCIStr(mysql.ColumnPrivTable),
			Schema: model.NewCIStr(mysql.SystemDB),
		},
		ColNames: []string{"Host", "User", "DB", "Table_name", "Column_name", "Column_priv"},
	}
	values := make([][]expression.Expression, 0, 1)
	value := make([]expression.Expression, 0, 3)
	value = append(value, &expression.Value{Val: host}) // Host
	value = append(value, &expression.Value{Val: user}) // User
	value = append(value, &expression.Value{Val: db})   // DB
	value = append(value, &expression.Value{Val: tbl})  // Table_name
	value = append(value, &expression.Value{Val: col})  // Column_name
	value = append(value, &expression.Value{Val: ""})   // Empty Column_priv
	values = append(values, value)
	st.Lists = values
	_, err := st.Exec(ctx)
	return errors.Trace(err)
}

// Grant priv to user in s.Level scope.
func (s *GrantStmt) grantPriv(ctx context.Context, priv *coldef.PrivElem, user *coldef.UserSpecification) error {
	switch s.Level.Level {
	case coldef.GrantLevelGlobal:
		return s.grantGlobalPriv(ctx, priv, user)
	case coldef.GrantLevelDB:
		return s.grantDBPriv(ctx, priv, user)
	case coldef.GrantLevelTable:
		if len(priv.Cols) == 0 {
			return s.grantTablePriv(ctx, priv, user)
		}
		return s.grantColumnPriv(ctx, priv, user)
	default:
		return errors.Errorf("Unknown grant level: %s", s.Level)
	}
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

// Manipulate mysql.tables_priv table.
func (s *GrantStmt) grantColumnPriv(ctx context.Context, priv *coldef.PrivElem, user *coldef.UserSpecification) error {
	db, tbl, err := s.getTargetSchemaAndTable(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	strs := strings.Split(user.User, "@")
	userName := strs[0]
	host := strs[1]
	for _, c := range priv.Cols {
		col := column.FindCol(tbl.Cols(), c)
		if col == nil {
			return errors.Errorf("Unknown column: %s", c)
		}
		asgns, err := composeColumnPrivUpdate(ctx, priv.Priv, userName, host, db.Name.O, tbl.TableName().O, col.Name.O)
		if err != nil {
			return errors.Trace(err)
		}
		st := &UpdateStmt{
			TableRefs: composeColumnPrivRset(),
			List:      asgns,
			Where:     composeColumnPrivFilter(userName, host, db.Name.O, tbl.TableName().O, col.Name.O),
		}
		_, err = st.Exec(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Compose update stmt assignment list for global scope privilege update.
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

// Compose update stmt assignment list for db scope privilege update.
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

// Compose update stmt assignment list for table scope privilege update.
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

// Compose update stmt assignment list for column scope privilege update.
func composeColumnPrivUpdate(ctx context.Context, priv mysql.PrivilegeType, name string, host string, db string, tbl string, col string) ([]expression.Assignment, error) {
	newColumnPriv := ""
	if priv == mysql.AllPriv {
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
		currColumnPriv, err := getColumnPriv(ctx, name, host, db, tbl, col)
		if err != nil {
			return nil, errors.Trace(err)
		}
		p, ok := mysql.Priv2SetStr[priv]
		if !ok {
			return nil, errors.Errorf("Unknown priv: %s", priv)
		}
		if len(currColumnPriv) == 0 {
			newColumnPriv = p
		} else {
			newColumnPriv = fmt.Sprintf("%s,%s", currColumnPriv, p)
		}
	}
	t := expression.Assignment{
		ColName: "Column_priv",
		Expr:    expression.Value{Val: newColumnPriv},
	}
	assigns := []expression.Assignment{t}
	return assigns, nil
}

// Compose where clause for select/update mysql.DB.
func composeDBTableFilter(name string, host string, db string) expression.Expression {
	dbMatch := expression.NewBinaryOperation(opcode.EQ, &expression.Ident{CIStr: model.NewCIStr("DB")}, &expression.Value{Val: db})
	return expression.NewBinaryOperation(opcode.AndAnd, composeUserTableFilter(name, host), dbMatch)
}

// Compose from clause for select/update mysql.DB.
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

// Compose where clause for select/update mysql.Tables_priv.
func composeTablePrivFilter(name string, host string, db string, tbl string) expression.Expression {
	tblMatch := expression.NewBinaryOperation(opcode.EQ, &expression.Ident{CIStr: model.NewCIStr("Table_name")}, &expression.Value{Val: tbl})
	return expression.NewBinaryOperation(opcode.AndAnd, composeDBTableFilter(name, host, db), tblMatch)
}

// Compose from clause for select/update mysql.Tables_priv.
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

// Compose where clause for select/update mysql.Columns_priv.
func composeColumnPrivFilter(name string, host string, db string, tbl string, col string) expression.Expression {
	colMatch := expression.NewBinaryOperation(opcode.EQ, &expression.Ident{CIStr: model.NewCIStr("Column_name")}, &expression.Value{Val: col})
	return expression.NewBinaryOperation(opcode.AndAnd, composeTablePrivFilter(name, host, db, tbl), colMatch)
}

// Compose from clause for select/update mysql.Columns_priv.
func composeColumnPrivRset() *rsets.JoinRset {
	return &rsets.JoinRset{
		Left: &rsets.TableSource{
			Source: table.Ident{
				Name:   model.NewCIStr(mysql.ColumnPrivTable),
				Schema: model.NewCIStr(mysql.SystemDB),
			},
		},
	}
}

// Check if there is an entry with key user-host-db in mysql.DB.
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

// Check if there is an entry with key user-host-db-tbl in mysql.Tables_priv.
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

// Check if there is an entry with key user-host-db-tbl-col in mysql.Columns_priv.
func columnPrivEntryExists(ctx context.Context, name string, host string, db string, tbl string, col string) (bool, error) {
	r := composeColumnPrivRset()
	p, err := r.Plan(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	where := &rsets.WhereRset{
		Src:  p,
		Expr: composeColumnPrivFilter(name, host, db, tbl, col),
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

// Get current table scope privilege set from mysql.Tables_priv.
// Return Table_priv and Column_priv.
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

// Get current column scope privilege set from mysql.Columns_priv.
// Return Column_priv.
func getColumnPriv(ctx context.Context, name string, host string, db string, tbl string, col string) (string, error) {
	r := composeColumnPrivRset()
	p, err := r.Plan(ctx)
	if err != nil {
		return "", errors.Trace(err)
	}
	where := &rsets.WhereRset{
		Src:  p,
		Expr: composeColumnPrivFilter(name, host, db, tbl, col),
	}
	p, err = where.Plan(ctx)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer p.Close()
	row, err := p.Next(ctx)
	if err != nil {
		return "", errors.Trace(err)
	}
	cPriv := ""
	if row.Data[6] != nil {
		columnPriv, ok := row.Data[6].(mysql.Set)
		if !ok {
			return "", errors.Errorf("Column Priv should be mysql.Set but get %v with type %T", row.Data[6], row.Data[6])
		}
		cPriv = columnPriv.Name
	}
	return cPriv, nil
}

// Find the schema by dbName.
func (s *GrantStmt) getTargetSchema(ctx context.Context) (*model.DBInfo, error) {
	dbName := s.Level.DBName
	if len(dbName) == 0 {
		// Grant *, use current schema
		dbName = db.GetCurrentSchema(ctx)
	}
	if len(dbName) == 0 {
		return nil, errors.New("Miss DB name for grant privilege.")
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

// Find the schema and table by dbName and tableName.
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
