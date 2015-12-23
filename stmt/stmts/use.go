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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/db"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/util/format"
)

var _ stmt.Statement = (*UseStmt)(nil)

// UseStmt is a statement to use the DBName database as the current database.
// See: https://dev.mysql.com/doc/refman/5.7/en/use.html
type UseStmt struct {
	DBName string

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *UseStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *UseStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *UseStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *UseStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *UseStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	dbname := model.NewCIStr(s.DBName)
	dbinfo, exists := sessionctx.GetDomain(ctx).InfoSchema().SchemaByName(dbname)
	if !exists {
		return nil, infoschema.DatabaseNotExists.Gen("database %s not exists", dbname)
	}
	db.BindCurrentSchema(ctx, dbname.O)
	s.updateSysVars(ctx, dbinfo)
	return nil, nil
}

// Update system variables
func (s *UseStmt) updateSysVars(ctx context.Context, dbinfo *model.DBInfo) {
	// character_set_database is the character set used by the default database.
	// The server sets this variable whenever the default database changes.
	// See: http://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_character_set_database
	sessionVars := variable.GetSessionVars(ctx)
	sessionVars.Systems[variable.CharsetDatabase] = dbinfo.Charset
	sessionVars.Systems[variable.CollationDatabase] = dbinfo.Collate
}
