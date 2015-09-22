package mocks

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/util/format"
)

// Statement represents a mocked Statement.
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
