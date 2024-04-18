package base

import "github.com/pingcap/tipb/go-tipb"

// AccessObject represents what is accessed by an operator.
// It corresponds to the "access object" column in an EXPLAIN statement result.
type AccessObject interface {
	String() string
	NormalizedString() string
	// SetIntoPB transform itself into a protobuf message and set into the binary plan.
	SetIntoPB(*tipb.ExplainOperator)
}
