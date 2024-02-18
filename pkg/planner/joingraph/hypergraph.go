// Copyright 2024 PingCAP, Inc.
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

package joingraph

import (
	"github.com/bits-and-blooms/bitset"
)

// In the paper, both the concept SemiJoin and LeftSemiJoin are leftSemi.
// Full join and group join are not used yet.
const (
	cartesian = iota
	inner
	leftSemi
	antiSemi
	leftOuter
	fullOuter
	// groupJoin
)

// Notice that in the paper the outer join is not simplified,
// but we will simplify the outer join to inner join before entering join reorder.
var assocom = [][]bool{
	{true, true, true, true, true, false},      // true},
	{true, true, true, true, true, false},      // true},
	{false, false, false, false, false, false}, // false},
	{false, false, false, false, false, false}, // false},
	{false, false, false, false, false, false}, // false},
	{false, false, false, false, false, false}, // false},
	{false, false, false, false, false, false}, // false},
}

var leftAssocom = [][]bool{
	{true, true, true, true, true, false},      //  true},
	{true, true, true, true, true, false},      // true},
	{true, true, true, true, true, false},      // true},
	{true, true, true, true, true, false},      // true},
	{true, true, true, true, true, false},      // true},
	{false, false, false, false, false, false}, // false},
	// {true, true, true, true, true, false, true},
}

var rightAssocom = [][]bool{
	{true, true, false, false, false, false},   // false},
	{true, true, false, false, false, false},   // false},
	{false, false, false, false, false, false}, // false},
	{false, false, false, false, false, false}, // false},
	{false, false, false, false, false, false}, // false},
	{false, false, false, false, false, false}, // false},
	{false, false, false, false, false, false}, // false},
}

type hyperEdge struct {
	dominatedNodeSet bitset.BitSet
	tesNodeSet       bitset.BitSet
	nodeSetInEdge    bitset.BitSet
	edgeType         uint
}

func calcSES(e *hyperEdge) {
	switch e.edgeType {
	case inner, fullOuter, leftOuter, antiSemi, leftSemi:
		e.tesNodeSet = *e.nodeSetInEdge.Clone()
	case cartesian:
		e.tesNodeSet.ClearAll()
	}
}
