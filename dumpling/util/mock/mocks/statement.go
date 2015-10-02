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

// Package mocks is just for test only.
package mocks

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/util/format"
)

// Statement represents mocked stmt.Statement.
type Statement struct {
	text string
	Rset *Recordset
	P    *Plan
}

// Exec implements the stmt.Statement Exec interface.
func (s *Statement) Exec(ctx context.Context) (rset.Recordset, error) {
	return s.Rset, nil
}

// Explain implements the stmt.Statement Explain interface.
func (s *Statement) Explain(ctx context.Context, w format.Formatter) {
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *Statement) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *Statement) OriginText() string {
	return s.text
}

// SetText implements the stmt.Statement SetText interface.
func (s *Statement) SetText(text string) {
	s.text = text
}

// Plan implements plan.Plan interface.
func (s *Statement) Plan(ctx context.Context) (plan.Plan, error) {
	return s.P, nil
}

// SetFieldOffset sets field offset.
func (s *Statement) SetFieldOffset(offset int) {
	s.Rset.SetFieldOffset(offset)
}

// String implements fmt.Stringer interface.
func (s *Statement) String() string {
	return "mock statement"
}
