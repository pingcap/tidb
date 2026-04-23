// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package property_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/stretchr/testify/require"
)

func RunNeedEnforceExchangerWithHashByEquivalence(t *testing.T) {
	for _, testcase := range []struct {
		fd                  *funcdep.FDSet
		MPPPartitionColumns []*property.ExportedMPPPartitionColumn
		HashCol             []*property.ExportedMPPPartitionColumn
		expected            bool
	}{
		// One MPPPartitionColumn is equivalent
		{
			// FD: (1)-->(2-6,8), ()-->(7), (9)-->(10-17), (1,10)==(1,10), (18,21)-->(19,20,22-33), (9,18)==(9,18)
			fd: buildTPCHQ3FD(),
			// MPPPartitionColumns: [18，13，16]
			MPPPartitionColumns: []*property.ExportedMPPPartitionColumn{
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 18}, -1), // -> (9,18)
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 13}, -1), // -> (13)
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 16}, -1), // -> (16)
			},
			// HashCol: [9]
			HashCol: []*property.ExportedMPPPartitionColumn{
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 9}, -1),
			},
			expected: false,
		},
		// Two MPPPartitionColumns are equivalent
		{
			// FD: (1)-->(2-6,8), ()-->(7), (9)-->(10-17), (1,10)==(1,10), (18,21)-->(19,20,22-33), (9,18)==(9,18)
			fd: buildTPCHQ3FD(),
			// MPPPartitionColumns: [18，13，16]
			MPPPartitionColumns: []*property.ExportedMPPPartitionColumn{
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 18}, -1), // -> (9,18)
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 13}, -1), // -> (13)
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 16}, -1), // -> (16)
			},
			// HashCol: [9, 8]
			HashCol: []*property.ExportedMPPPartitionColumn{
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 9}, -1),
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 13}, -1),
			},
			expected: false,
		},
		// One MPPPartitionColumn is not equivalent， the other is equivalent
		{
			// FD: (1)-->(2-6,8), ()-->(7), (9)-->(10-17), (1,10)==(1,10), (18,21)-->(19,20,22-33), (9,18)==(9,18)
			fd: buildTPCHQ3FD(),
			// MPPPartitionColumns: [18，13，16]
			MPPPartitionColumns: []*property.ExportedMPPPartitionColumn{
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 18}, -1), // -> (9,18)
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 13}, -1), // -> (13)
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 16}, -1), // -> (16)
			},
			// HashCol: [9, 17]
			HashCol: []*property.ExportedMPPPartitionColumn{
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 9}, -1),
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 17}, -1),
			},
			expected: true,
		},
		// Two MPPPartitionColumn is not equivalent
		{
			// FD: (1)-->(2-6,8), ()-->(7), (9)-->(10-17), (1,10)==(1,10), (18,21)-->(19,20,22-33), (9,18)==(9,18)
			fd: buildTPCHQ3FD(),
			// MPPPartitionColumns: [18，13，16]
			MPPPartitionColumns: []*property.ExportedMPPPartitionColumn{
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 18}, -1), // -> (9,18)
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 13}, -1), // -> (13)
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 16}, -1), // -> (16)
			},
			// HashCol: [9, 17]
			HashCol: []*property.ExportedMPPPartitionColumn{
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 1}, -1),
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 17}, -1),
			},
			expected: true,
		},
		// One MPPPartitionColumn is equivalent
		{
			fd: buildFD(),
			// MPPPartitionColumns: [1,2,3]
			MPPPartitionColumns: []*property.ExportedMPPPartitionColumn{
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 1}, -1),
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 2}, -1),
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 3}, -1),
			},
			// HashCol: [9]
			HashCol: []*property.ExportedMPPPartitionColumn{
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 1}, -1),
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 2}, -1),
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 4}, -1),
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 5}, -1),
			},
			expected: true,
		},
		{
			// (2,4,5)==(2,4,5)
			fd: buildFD2(),
			// MPPPartitionColumns: [1,2]
			MPPPartitionColumns: []*property.ExportedMPPPartitionColumn{
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 1}, -1),
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 2}, -1),
			},
			// HashCol: [1,2,5]
			HashCol: []*property.ExportedMPPPartitionColumn{
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 1}, -1),
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 2}, -1),
				property.ExportedNewMPPPartitionColumn(&expression.Column{UniqueID: 5}, -1),
			},
			expected: false,
		},
	} {
		prop := &property.PhysicalProperty{
			MPPPartitionCols: property.ExportedSliceFromExportedMPPPartitionColumn(testcase.MPPPartitionColumns),
		}
		t.Log(testcase.fd.String())
		require.Equal(t, testcase.expected, prop.NeedMPPExchangeByEquivalence(property.ExportedSliceFromExportedMPPPartitionColumn(testcase.HashCol), testcase.fd))
	}
}

func buildTPCHQ3FD() *funcdep.FDSet {
	// FD: (1)-->(2-6,8), ()-->(7), (9)-->(10-17), (1,10)==(1,10), (18,21)-->(19,20,22-33), (9,18)==(9,18)
	fd := &funcdep.FDSet{}
	fd.AddEquivalence(intset.NewFastIntSet(1, 10), intset.NewFastIntSet(1, 10))
	fd.AddStrictFunctionalDependency(intset.NewFastIntSet(1), intset.NewFastIntSet(2, 3, 4, 5, 6, 8))
	fd.AddStrictFunctionalDependency(intset.NewFastIntSet(), intset.NewFastIntSet(7))
	fd.AddStrictFunctionalDependency(intset.NewFastIntSet(9), intset.NewFastIntSet(10, 11, 12, 13, 14, 15, 16, 17))
	fd.AddStrictFunctionalDependency(intset.NewFastIntSet(10, 21), intset.NewFastIntSet(19, 20, 22, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33))

	fd.AddEquivalenceUnion(intset.NewFastIntSet(9, 18))
	fd.AddEquivalenceUnion(intset.NewFastIntSet(1, 10))
	return fd
}

func buildFD() *funcdep.FDSet {
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
func buildFD2() *funcdep.FDSet {
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
