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
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ stmt.Statement = (*CreateDatabaseStmt)(nil)
	_ stmt.Statement = (*CreateTableStmt)(nil)
	_ stmt.Statement = (*CreateIndexStmt)(nil)
)

// CreateDatabaseStmt is a statement to create a database.
// See: https://dev.mysql.com/doc/refman/5.7/en/create-database.html
type CreateDatabaseStmt struct {
	IfNotExists bool
	Name        string
	Opt         *coldef.CharsetOpt

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *CreateDatabaseStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *CreateDatabaseStmt) IsDDL() bool {
	return true
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *CreateDatabaseStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *CreateDatabaseStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *CreateDatabaseStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	log.Debug("create database")
	err = sessionctx.GetDomain(ctx).DDL().CreateSchema(ctx, model.NewCIStr(s.Name), s.Opt)
	if err != nil {
		if terror.ErrorEqual(err, infoschema.DatabaseExists) && s.IfNotExists {
			err = nil
		}
	}
	return nil, errors.Trace(err)
}

// CreateTableStmt is a statement to create a table.
// See: https://dev.mysql.com/doc/refman/5.7/en/create-table.html
type CreateTableStmt struct {
	IfNotExists bool
	Ident       table.Ident
	Cols        []*coldef.ColumnDef
	Constraints []*coldef.TableConstraint
	Opt         *coldef.TableOption

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *CreateTableStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *CreateTableStmt) IsDDL() bool {
	return true
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *CreateTableStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *CreateTableStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *CreateTableStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	err = sessionctx.GetDomain(ctx).DDL().CreateTable(ctx, s.Ident.Full(ctx), s.Cols, s.Constraints)
	if terror.ErrorEqual(err, infoschema.TableExists) {
		if s.IfNotExists {
			return nil, nil
		}
		return nil, infoschema.TableExists.Gen("CREATE TABLE: table exists %s", s.Ident)
	}
	return nil, errors.Trace(err)
}

// CreateIndexStmt is a statement to create an index.
// See: https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type CreateIndexStmt struct {
	IndexName     string
	TableIdent    table.Ident
	Unique        bool
	IndexColNames []*coldef.IndexColName

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *CreateIndexStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *CreateIndexStmt) IsDDL() bool {
	return true
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *CreateIndexStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *CreateIndexStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *CreateIndexStmt) Exec(ctx context.Context) (rset.Recordset, error) {
	err := sessionctx.GetDomain(ctx).DDL().CreateIndex(ctx, s.TableIdent.Full(ctx), s.Unique, model.NewCIStr(s.IndexName), s.IndexColNames)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return nil, nil
}
