// Copyright 2026 PingCAP, Inc.
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

package matchagainst

// UnaryOp describes MySQL boolean-mode prefix operators, aligned with InnoDB's
// single-operator selection priority.
type UnaryOp uint8

// Possible UnaryOp values.
const (
	OpNone UnaryOp = iota
	OpExist
	OpIgnore
	OpNegate
	OpIncrRating
	OpDecrRating
)

// Node is one boolean query element, with an optional unary operator applied.
//
// NOTE: InnoDB keeps at most one unary operator node per element. If multiple
// modifiers are present, Op is chosen by MySQL/InnoDB priority, see pickUnaryOp.
type Node struct {
	Op   UnaryOp
	Item Item
}

// Item is a concrete query element.
type Item interface {
	item()
}

// Word is a single word term. Trunc means a trailing '*' was present (e.g. foo*).
// Ignored marks words that are intentionally emitted but should be ignored by
// later stages (e.g. @N distance terms).
type Word struct {
	Text    string
	Trunc   bool
	Ignored bool
}

func (*Word) item() {}

// Phrase is a quoted phrase. This implementation stores it as a normalized
// space-joined string.
type Phrase struct {
	Text string
}

func (*Phrase) item() {}

// Group is a parenthesized sub-expression, represented as three buckets
// according to boolean require semantics.
type Group struct {
	Must    []Node
	Should  []Node
	MustNot []Node
}

func (*Group) item() {}

func (g *Group) addNode(n Node) {
	switch n.Op {
	case OpExist:
		g.Must = append(g.Must, n)
	case OpIgnore:
		g.MustNot = append(g.MustNot, n)
	default:
		g.Should = append(g.Should, n)
	}
}
