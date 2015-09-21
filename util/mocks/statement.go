package mocks

import (
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/rset"
	"github.com/pingcap/tidb/util/format"
)

// MockStatement represents a mocked Statement.
type MockStatement struct {
	text string
	Rset *Recordset
	P    *MockPlan
}

// Exec implements the stmt.Statement Exec interface.
func (s *MockStatement) Exec(ctx context.Context) (rset.Recordset, error) {
	return s.Rset, nil
}

// Explain implements the stmt.Statement Explain interface.
func (s *MockStatement) Explain(ctx context.Context, w format.Formatter) {
}

// IsDDL implements the stmt.Statement IsDDL interface.
func (s *MockStatement) IsDDL() bool {
	return false
}

// OriginText implements the stmt.Statement OriginText interface.
func (s *MockStatement) OriginText() string {
	return s.text
}

// SetText implements the stmt.Statement SetText interface.
func (s *MockStatement) SetText(text string) {
	s.text = text
}

// Plan implements plan.Plan interface.
func (s *MockStatement) Plan(ctx context.Context) (plan.Plan, error) {
	return s.P, nil
}

// SetFieldOffset sets field offset.
func (s *MockStatement) SetFieldOffset(offset int) {
	s.Rset.SetFieldOffset(offset)
}

// String implements fmt.Stringer interface.
func (s *MockStatement) String() string {
	return "mock statement"
}
