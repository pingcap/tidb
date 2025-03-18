package property

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/stretchr/testify/require"
)

func TestNeedEnforceExchangerWithHashByEquivalence(t *testing.T) {
	for _, testcase := range []struct {
		fd                  *funcdep.FDSet
		MPPPartitionColumns []*MPPPartitionColumn
		HashCol             []*MPPPartitionColumn
		expected            bool
	}{
		// One MPPPartitionColumn is equivalent
		{
			fd: buildTPCHQ3FD(),
			// MPPPartitionColumns: [18，13，16]
			MPPPartitionColumns: []*MPPPartitionColumn{
				{Col: &expression.Column{UniqueID: 1}},
				{Col: &expression.Column{UniqueID: 2}},
				{Col: &expression.Column{UniqueID: 3}},
			},
			// HashCol: [9]
			HashCol: []*MPPPartitionColumn{
				{Col: &expression.Column{UniqueID: 1}},
				{Col: &expression.Column{UniqueID: 2}},
				{Col: &expression.Column{UniqueID: 4}},
				{Col: &expression.Column{UniqueID: 5}},
			},
			expected: true,
		},
		{
			fd: buildTPCHQ3FD2(),
			// MPPPartitionColumns: [18，13，16]
			MPPPartitionColumns: []*MPPPartitionColumn{
				{Col: &expression.Column{UniqueID: 1}},
				{Col: &expression.Column{UniqueID: 2}},
			},
			// HashCol: [9]
			HashCol: []*MPPPartitionColumn{
				{Col: &expression.Column{UniqueID: 1}},
				{Col: &expression.Column{UniqueID: 2}},
				{Col: &expression.Column{UniqueID: 5}},
			},
			expected: false,
		},
	} {
		prop := &PhysicalProperty{
			MPPPartitionCols: testcase.MPPPartitionColumns,
		}
		require.Equal(t, testcase.expected, prop.NeedEnforceExchangerWithHashByEquivalence(testcase.HashCol, testcase.fd))
	}
}

func buildTPCHQ3FD() *funcdep.FDSet {
	// we build a  case like:
	// parent prop require: partition cols:   1, 2, 3
	// the child can supply: partition cols:  1, 4, 5
	// child supply is not subset of parent required, while child did can supply the parent required cols.
	// with fd has an equivalence between column 3 and 4
	fd := &funcdep.FDSet{}
	fd.AddEquivalence(intset.NewFastIntSet(2), intset.NewFastIntSet(4))
	fd.AddEquivalence(intset.NewFastIntSet(3), intset.NewFastIntSet(4))
	return fd
}
func buildTPCHQ3FD2() *funcdep.FDSet {
	// we build a  case like:
	// parent prop require: partition cols:   1, 2
	// the child can supply: partition cols:  1, 4, 5
	// child supply is not subset of parent required, while child did can supply the parent required cols.
	// with fd has an equivalence between column 3 and 4
	fd := &funcdep.FDSet{}
	fd.AddEquivalence(intset.NewFastIntSet(2), intset.NewFastIntSet(4))
	fd.AddEquivalence(intset.NewFastIntSet(2), intset.NewFastIntSet(5))
	return fd
}
