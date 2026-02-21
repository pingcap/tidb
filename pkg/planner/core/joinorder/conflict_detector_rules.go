// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package joinorder

// ruleTableEntry encodes whether a given algebraic property holds for a pair of
// join types (see Table 2 and Table 3 in the paper):
//
//	0 — property does NOT hold; a conflict rule must be generated.
//	1 — property holds unconditionally.
//	2 — property holds only when the null-rejection condition is satisfied.
//
// Currently, value 2 is unused because:
//  1. TiDB does not support FULL OUTER JOIN, which is the main source of
//     conditional entries in the paper's tables.
//  2. extractJoinGroup() only admits non-inner joins that have at least one
//     equi-condition, which implicitly guarantees null-rejection on both sides.
//     This allows assoc(LEFT, LEFT) and assoc(RIGHT, RIGHT) to be treated as
//     unconditional (value 1). If non-inner joins without equi-conditions are
//     admitted in the future, null-rejection checks must be added here.
//
// The value 2 is retained as a placeholder for future extension.
type ruleTableEntry int

// assocRuleTable[e1][e2] indicates whether the associativity transformation
//
//	(R1 ⋈_e1 R2) ⋈_e2 R3  ⟺  R1 ⋈_e1 (R2 ⋈_e2 R3)
//
// is valid for the given pair of join types.
// Rows = join type of e1 (left/child edge), Columns = join type of e2 (right/parent edge).
var assocRuleTable = [][]ruleTableEntry{
	// INNER
	{
		1, // INNER
		1, // LEFT OUTER
		0, // RIGHT OUTER
		1, // LEFT SEMI and LEFT OUTER SEMI
		1, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT OUTER
	{
		0, // INNER
		1, // LEFT OUTER, check NOTE above.
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// RIGHT OUTER
	{
		1, // INNER
		1, // LEFT OUTER
		1, // RIGHT OUTER, check NOTE above.
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT SEMI and LEFT OUTER SEMI
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},

	// LEFT ANTI and ANTI LEFT OUTER SEMI
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
}

// leftAsscomRuleTable[e1][e2] indicates whether the left-asscom transformation
//
//	(R1 ⋈_e1 R2) ⋈_e2 R3  ⟺  (R1 ⋈_e2 R3) ⋈_e1 R2
//
// is valid. Here e1 is the child edge (in leftEdges) and e2 is the parent edge.
var leftAsscomRuleTable = [][]ruleTableEntry{
	// INNER
	{
		1, // INNER
		1, // LEFT OUTER
		0, // RIGHT OUTER
		1, // LEFT SEMI and LEFT OUTER SEMI
		1, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT OUTER
	{
		1, // INNER
		1, // LEFT OUTER
		0, // RIGHT OUTER
		1, // LEFT SEMI and LEFT OUTER SEMI
		1, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// RIGHT OUTER
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT SEMI and LEFT OUTER SEMI
	{
		1, // INNER
		1, // LEFT OUTER
		1, // RIGHT OUTER
		1, // LEFT SEMI and LEFT OUTER SEMI
		1, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT ANTI and ANTI LEFT OUTER SEMI
	{
		1, // INNER
		1, // LEFT OUTER
		1, // RIGHT OUTER
		1, // LEFT SEMI and LEFT OUTER SEMI
		1, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
}

// rightAsscomRuleTable[e1][e2] indicates whether the right-asscom transformation
//
//	R1 ⋈_e1 (R2 ⋈_e2 R3)  ⟺  R2 ⋈_e2 (R1 ⋈_e1 R3)
//
// is valid. Here e1 is the parent edge and e2 is the child edge (in rightEdges).
var rightAsscomRuleTable = [][]ruleTableEntry{
	// INNER
	{
		1, // INNER
		1, // LEFT OUTER
		1, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT OUTER
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// RIGHT OUTER
	{
		0, // INNER
		1, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT SEMI and LEFT OUTER SEMI
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
	// LEFT ANTI and ANTI LEFT OUTER SEMI
	{
		0, // INNER
		0, // LEFT OUTER
		0, // RIGHT OUTER
		0, // LEFT SEMI and LEFT OUTER SEMI
		0, // LEFT ANTI and ANTI LEFT OUTER SEMI
	},
}
