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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/errors2"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ stmt.Statement = (*DropDatabaseStmt)(nil)
	_ stmt.Statement = (*DropTableStmt)(nil)
	_ stmt.Statement = (*DropIndexStmt)(nil)
)

// DropDatabaseStmt is a statement to drop a database and all tables in the database.
// See: https://dev.mysql.com/doc/refman/5.7/en/drop-database.html
type DropDatabaseStmt struct {
	IfExists bool
	Name     string

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *DropDatabaseStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *DropDatabaseStmt) IsDDL() bool {
	return true
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *DropDatabaseStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *DropDatabaseStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *DropDatabaseStmt) Exec(ctx context.Context) (rset.Recordset, error) {
	err := sessionctx.GetDomain(ctx).DDL().DropSchema(ctx, model.NewCIStr(s.Name))
	if errors2.ErrorEqual(err, ddl.ErrNotExists) && s.IfExists {
		err = nil
	}
	return nil, errors.Trace(err)
}

// DropTableStmt is a statement to drop one or more tables.
// See: https://dev.mysql.com/doc/refman/5.7/en/drop-table.html
type DropTableStmt struct {
	IfExists    bool
	TableIdents []table.Ident

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *DropTableStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *DropTableStmt) IsDDL() bool {
	return true
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *DropTableStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *DropTableStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *DropTableStmt) Exec(ctx context.Context) (rset.Recordset, error) {
	var notExistTables []string
	for _, ti := range s.TableIdents {
		err := sessionctx.GetDomain(ctx).DDL().DropTable(ctx, ti.Full(ctx))
		if err != nil && strings.HasSuffix(err.Error(), "not exist") {
			notExistTables = append(notExistTables, ti.String())
		} else if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if len(notExistTables) > 0 && !s.IfExists {
		return nil, errors.Errorf("DROP TABLE: table %s does not exist", strings.Join(notExistTables, ","))
	}
	return nil, nil
}

// DropIndexStmt is a statement to drop the index.
// See: https://dev.mysql.com/doc/refman/5.7/en/drop-index.html
type DropIndexStmt struct {
	IfExists  bool
	IndexName string

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *DropIndexStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *DropIndexStmt) IsDDL() bool {
	return true
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *DropIndexStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *DropIndexStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *DropIndexStmt) Exec(ctx context.Context) (rset.Recordset, error) {
	log.Info("drop index", s.IndexName)
	return nil, nil
}
