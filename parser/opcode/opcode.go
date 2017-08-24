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

package opcode

import "fmt"

// Op is opcode type.
type Op int

// List operators.
const (
	LogicAnd Op = iota + 1
	LeftShift
	RightShift
	LogicOr
	GE
	LE
	EQ
	NE
	LT
	GT
	Plus
	Minus
	And
	Or
	Mod
	Xor
	Div
	Mul
	Not
	BitNeg
	IntDiv
	LogicXor
	NullEQ
	In
	Like
	Case
	Regexp
	IsNull
	IsTruth
	IsFalsity
)

// Ops maps opcode to string.
var Ops = map[Op]string{
	LogicAnd:   "and",
	LogicOr:    "or",
	LogicXor:   "xor",
	LeftShift:  "leftshift",
	RightShift: "rightshift",
	GE:         "ge",
	LE:         "le",
	EQ:         "eq",
	NE:         "ne",
	LT:         "lt",
	GT:         "gt",
	Plus:       "plus",
	Minus:      "minus",
	And:        "bitand",
	Or:         "bitor",
	Mod:        "mod",
	Xor:        "bitxor",
	Div:        "div",
	Mul:        "mul",
	Not:        "not",
	BitNeg:     "bitneg",
	IntDiv:     "intdiv",
	NullEQ:     "nulleq",
	In:         "in",
	Like:       "like",
	Case:       "case",
	Regexp:     "regexp",
	IsNull:     "isnull",
	IsTruth:    "istrue",
	IsFalsity:  "isfalse",
}

// String implements Stringer interface.
func (o Op) String() string {
	str, ok := Ops[o]
	if !ok {
		panic(fmt.Sprintf("%d", o))
	}

	return str
}
