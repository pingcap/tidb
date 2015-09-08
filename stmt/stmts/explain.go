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
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/rset/rsets"
	"github.com/pingcap/tidb/stmt"
	"github.com/pingcap/tidb/util/format"
)

var _ stmt.Statement = (*ExplainStmt)(nil)

// ExplainStmt is a statement to provide information about how is SQL statement executed
// or get columns information in a table.
// See: https://dev.mysql.com/doc/refman/5.7/en/explain.html
type ExplainStmt struct {
	S stmt.Statement

	Text string
}

// Explain implements the stmt.Statement Explain interface.
func (s *ExplainStmt) Explain(ctx context.Context, w format.Formatter) {
	if x, ok := s.S.(*ShowStmt); ok {
		x.SetText(s.Text)
		x.Explain(ctx, w)
		return
	}

	for {
		x, ok := s.S.(*ExplainStmt)
		if !ok {
			s.S.Explain(ctx, w)
			return
		}
		s = x
	}
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *ExplainStmt) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *ExplainStmt) OriginText() string {
	return s.Text
}

// SetText implements the stmt.Statement SetText interface.
func (s *ExplainStmt) SetText(text string) {
	s.Text = text
}

// Exec implements the stmt.Statement Exec interface.
func (s *ExplainStmt) Exec(ctx context.Context) (_ rset.Recordset, err error) {
	if v, ok := s.S.(*ShowStmt); ok {
		return v.Exec(ctx)
	}

	return rsets.Recordset{Ctx: ctx, Plan: &plans.ExplainDefaultPlan{S: s.S}}, nil
}
