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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ stmt.Statement = (*BeginStmt)(nil)
	_ stmt.Statement = (*CommitStmt)(nil)
	_ stmt.Statement = (*RollbackStmt)(nil)
)

// BeginStmt is a statement to start a new transaction.
// See: https://dev.mysql.com/doc/refman/5.7/en/commit.html
type BeginStmt struct {
	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *BeginStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *BeginStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *BeginStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *BeginStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *BeginStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	_, err = ctx.GetTxn(true)
	// With START TRANSACTION, autocommit remains disabled until you end
	// the transaction with COMMIT or ROLLBACK. The autocommit mode then
	// reverts to its previous state.
	variable.GetSessionVars(ctx).SetStatusFlag(mysql.ServerStatusInTrans, true)
	return
}

// CommitStmt is a statement to commit the current transaction.
// See: https://dev.mysql.com/doc/refman/5.7/en/commit.html
type CommitStmt struct {
	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *CommitStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *CommitStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *CommitStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *CommitStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *CommitStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	err = ctx.FinishTxn(false)
	variable.GetSessionVars(ctx).SetStatusFlag(mysql.ServerStatusInTrans, false)
	return
}

// RollbackStmt is a statement to roll back the current transaction.
// See: https://dev.mysql.com/doc/refman/5.7/en/commit.html
type RollbackStmt struct {
	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *RollbackStmt) Explain(ctx context.Context, w format.Formatter) {
	w.Format("%s\n", s.Text)
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *RollbackStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *RollbackStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *RollbackStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *RollbackStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	err = ctx.FinishTxn(true)
	variable.GetSessionVars(ctx).SetStatusFlag(mysql.ServerStatusInTrans, false)
	return
}
