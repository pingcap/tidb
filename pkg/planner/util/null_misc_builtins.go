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

package util

import "github.com/pingcap/tidb/pkg/parser/ast"

// nullRejectTestMode describes how IS TRUE / IS FALSE style builtins handle a
// NULL child during null-reject proof.
type nullRejectTestMode uint8

const (
	nullRejectTestReturnsFalse nullRejectTestMode = iota // f(NULL) = FALSE; nonTrue but not mustNull
	nullRejectTestKeepsNull                              // f(NULL) = NULL; both nonTrue and mustNull
)

// nullRejectNullPreservingFunctions lists builtins that return NULL when any
// argument is NULL. See the file-level comment in null_misc.go for how this
// drives the proof.
var nullRejectNullPreservingFunctions = map[string]struct{}{
	ast.Cast:            {},
	ast.GE:              {},
	ast.LE:              {},
	ast.EQ:              {},
	ast.NE:              {},
	ast.LT:              {},
	ast.GT:              {},
	ast.Plus:            {},
	ast.Minus:           {},
	ast.Mod:             {},
	ast.Div:             {},
	ast.Mul:             {},
	ast.IntDiv:          {},
	ast.BitNeg:          {},
	ast.And:             {},
	ast.LeftShift:       {},
	ast.RightShift:      {},
	ast.Or:              {},
	ast.Xor:             {},
	ast.UnaryMinus:      {},
	ast.LogicXor:        {},
	ast.Like:            {},
	ast.Ilike:           {},
	ast.Regexp:          {},
	ast.RegexpLike:      {},
	ast.RegexpSubstr:    {},
	ast.RegexpInStr:     {},
	ast.RegexpReplace:   {},
	ast.Strcmp:          {},
	ast.Abs:             {},
	ast.Ceil:            {},
	ast.Ceiling:         {},
	ast.CRC32:           {},
	ast.Degrees:         {},
	ast.Exp:             {},
	ast.Floor:           {},
	ast.Ln:              {},
	ast.Log:             {},
	ast.Log2:            {},
	ast.Log10:           {},
	ast.Pow:             {},
	ast.Power:           {},
	ast.Radians:         {},
	ast.Round:           {},
	ast.Sign:            {},
	ast.Sqrt:            {},
	ast.ASCII:           {},
	ast.Bin:             {},
	ast.BitLength:       {},
	ast.CharLength:      {},
	ast.CharacterLength: {},
	ast.Concat:          {},
	ast.FindInSet:       {},
	ast.FromBase64:      {},
	ast.Hex:             {},
	ast.InsertFunc:      {},
	ast.Lcase:           {},
	ast.Left:            {},
	ast.Length:          {},
	ast.Locate:          {},
	ast.Lower:           {},
	ast.LTrim:           {},
	ast.Mid:             {},
	ast.Oct:             {},
	ast.Ord:             {},
	ast.Position:        {},
	ast.Repeat:          {},
	ast.Replace:         {},
	ast.Reverse:         {},
	ast.Right:           {},
	ast.RTrim:           {},
	ast.Space:           {},
	ast.Substring:       {},
	ast.SubstringIndex:  {},
	ast.Trim:            {},
	ast.Ucase:           {},
	ast.Unhex:           {},
	ast.Upper:           {},
	ast.AddDate:         {},
	ast.DateAdd:         {},
	ast.SubDate:         {},
	ast.DateSub:         {},
	ast.AddTime:         {},
	ast.ConvertTz:       {},
	ast.Date:            {},
	ast.DateFormat:      {},
	ast.DateDiff:        {},
	ast.Day:             {},
	ast.DayOfMonth:      {},
	ast.DayOfWeek:       {},
	ast.DayOfYear:       {},
	ast.Extract:         {},
	ast.FromDays:        {},
	ast.FromUnixTime:    {},
	ast.Hour:            {},
	ast.MakeDate:        {},
	ast.MakeTime:        {},
	ast.MicroSecond:     {},
	ast.Minute:          {},
	ast.Month:           {},
	ast.PeriodAdd:       {},
	ast.PeriodDiff:      {},
	ast.Quarter:         {},
	ast.SecToTime:       {},
	ast.Second:          {},
	ast.StrToDate:       {},
	ast.SubTime:         {},
	ast.Time:            {},
	ast.TimeDiff:        {},
	ast.TimestampDiff:   {},
	ast.ToDays:          {},
	ast.ToSeconds:       {},
	ast.UnixTimestamp:   {},
	ast.Year:            {},
}

// nullRejectRejectNullTests lists IS-TRUE / IS-FALSE style builtins that
// convert a NULL input into a definite boolean. The mode records whether the
// output is FALSE (ReturnsFalse) or stays NULL (KeepsNull).
var nullRejectRejectNullTests = map[string]nullRejectTestMode{
	ast.IsTruthWithoutNull: nullRejectTestReturnsFalse,
	ast.IsTruthWithNull:    nullRejectTestKeepsNull,
	ast.IsFalsity:          nullRejectTestReturnsFalse,
}
