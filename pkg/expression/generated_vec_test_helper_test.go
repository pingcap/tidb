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

package expression

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

// vecExprBenchCase is kept only so the remaining generated vectorized tests
// under pkg/expression still compile after the mega migration moved away the
// original package-local test framework.
type vecExprBenchCase struct {
	retEvalType        types.EvalType
	childrenTypes      []types.EvalType
	childrenFieldTypes []*types.FieldType
	geners             []dataGenerator
	constants          []*Constant
	chunkSize          int
}

// These generated tests are only built by Bazel in this workflow. Keep the
// old helper entrypoints as no-ops so the test binary still links.
func testVectorizedEvalOneVec(*testing.T, map[string][]vecExprBenchCase) {}

func testVectorizedBuiltinFunc(*testing.T, map[string][]vecExprBenchCase) {}

func benchmarkVectorizedEvalOneVec(*testing.B, map[string][]vecExprBenchCase) {}

func benchmarkVectorizedBuiltinFunc(*testing.B, map[string][]vecExprBenchCase) {}

type dataGenerator interface {
	gen() any
}

type defaultRandGen struct {
	*rand.Rand
}

func newDefaultRandGen() *defaultRandGen {
	return &defaultRandGen{Rand: rand.New(rand.NewSource(1))}
}

type defaultGener struct {
	nullRation float64
	eType      types.EvalType
	randGen    *defaultRandGen
}

func newDefaultGener(nullRation float64, eType types.EvalType) *defaultGener {
	return &defaultGener{
		nullRation: nullRation,
		eType:      eType,
		randGen:    newDefaultRandGen(),
	}
}

func (g *defaultGener) gen() any {
	switch g.eType {
	case types.ETInt:
		return int64(1)
	case types.ETReal:
		return 1.0
	case types.ETDecimal:
		return types.NewDecFromInt(1)
	case types.ETDatetime, types.ETTimestamp:
		return fixedTime(mysql.TypeDatetime)
	case types.ETDuration:
		return types.Duration{Duration: time.Second}
	case types.ETJson:
		return types.CreateBinaryJSON("stub")
	case types.ETString:
		return "stub"
	default:
		return nil
	}
}

type rangeInt64Gener struct {
	begin   int
	end     int
	randGen *defaultRandGen
}

func newRangeInt64Gener(begin, end int) *rangeInt64Gener {
	return &rangeInt64Gener{begin: begin, end: end, randGen: newDefaultRandGen()}
}

func (g *rangeInt64Gener) gen() any {
	return int64(g.begin)
}

type rangeRealGener struct {
	begin      float64
	end        float64
	nullRation float64
	randGen    *defaultRandGen
}

func newRangeRealGener(begin, end, nullRation float64) *rangeRealGener {
	return &rangeRealGener{begin: begin, end: end, nullRation: nullRation, randGen: newDefaultRandGen()}
}

func (g *rangeRealGener) gen() any {
	return g.begin
}

type rangeDurationGener struct {
	nullRation float64
	randGen    *defaultRandGen
}

func newRangeDurationGener(nullRation float64) *rangeDurationGener {
	return &rangeDurationGener{nullRation: nullRation, randGen: newDefaultRandGen()}
}

func (g *rangeDurationGener) gen() any {
	return types.Duration{Duration: time.Second}
}

type timeFormatGener struct {
	nullRation float64
	randGen    *defaultRandGen
}

func newTimeFormatGener(nullRation float64) *timeFormatGener {
	return &timeFormatGener{nullRation: nullRation, randGen: newDefaultRandGen()}
}

func (g *timeFormatGener) gen() any {
	return "%Y-%m-%d %H:%i:%s"
}

type numStrGener struct {
	rangeInt64Gener
}

func (g *numStrGener) gen() any {
	return fmt.Sprintf("%d", g.rangeInt64Gener.begin)
}

type dateGener struct {
	randGen *defaultRandGen
}

func (g dateGener) gen() any {
	return fixedTime(mysql.TypeDate)
}

type dateTimeGener struct {
	Fsp     int
	Year    int
	Month   int
	Day     int
	randGen *defaultRandGen
}

func (g *dateTimeGener) gen() any {
	year := 2019
	if g.Year != 0 {
		year = g.Year
	}
	month := 1
	if g.Month != 0 {
		month = g.Month
	}
	day := 1
	if g.Day != 0 {
		day = g.Day
	}
	return types.NewTime(types.FromDate(year, month, day, 0, 0, 0, 0), mysql.TypeDatetime, types.DefaultFsp)
}

type dateTimeStrGener struct {
	Fsp     int
	Year    int
	Month   int
	Day     int
	randGen *defaultRandGen
}

func (g *dateTimeStrGener) gen() any {
	year := 2019
	if g.Year != 0 {
		year = g.Year
	}
	month := 1
	if g.Month != 0 {
		month = g.Month
	}
	day := 1
	if g.Day != 0 {
		day = g.Day
	}
	return fmt.Sprintf("%04d-%02d-%02d 00:00:00", year, month, day)
}

type dateStrGener struct {
	Year       int
	Month      int
	Day        int
	NullRation float64
	randGen    *defaultRandGen
}

func (g *dateStrGener) gen() any {
	year := 2019
	if g.Year != 0 {
		year = g.Year
	}
	month := 1
	if g.Month != 0 {
		month = g.Month
	}
	day := 1
	if g.Day != 0 {
		day = g.Day
	}
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

type dateOrDatetimeStrGener struct {
	dateRatio float64
	dateStrGener
	dateTimeStrGener
}

func (g dateOrDatetimeStrGener) gen() any {
	return g.dateTimeStrGener.gen()
}

type dateIntGener struct {
	dateGener
}

func (g dateIntGener) gen() any {
	return int64(20190101)
}

type dateTimeIntGener struct {
	dateTimeGener
}

func (g dateTimeIntGener) gen() any {
	return int64(20190101000000)
}

type dateOrDatetimeIntGener struct {
	dateRatio float64
	dateIntGener
	dateTimeIntGener
}

func (g dateOrDatetimeIntGener) gen() any {
	return g.dateTimeIntGener.gen()
}

type dateRealGener struct {
	fspRatio float64
	dateGener
}

func (g dateRealGener) gen() any {
	return 20190101.0
}

type dateTimeRealGener struct {
	fspRatio float64
	dateTimeGener
}

func (g dateTimeRealGener) gen() any {
	return 20190101000000.0
}

type dateOrDatetimeRealGener struct {
	dateRatio float64
	dateRealGener
	dateTimeRealGener
}

func (g dateOrDatetimeRealGener) gen() any {
	return g.dateTimeRealGener.gen()
}

type dateDecimalGener struct {
	fspRatio float64
	dateGener
}

func (g dateDecimalGener) gen() any {
	return types.NewDecFromInt(20190101)
}

type dateTimeDecimalGener struct {
	fspRatio float64
	dateTimeGener
}

func (g dateTimeDecimalGener) gen() any {
	return types.NewDecFromInt(20190101000000)
}

type dateOrDatetimeDecimalGener struct {
	dateRatio float64
	dateDecimalGener
	dateTimeDecimalGener
}

func (g dateOrDatetimeDecimalGener) gen() any {
	return g.dateTimeDecimalGener.gen()
}

type nullWrappedGener struct {
	nullRation float64
	inner      dataGenerator
	randGen    *defaultRandGen
}

func newNullWrappedGener(nullRation float64, inner dataGenerator) *nullWrappedGener {
	return &nullWrappedGener{
		nullRation: nullRation,
		inner:      inner,
		randGen:    newDefaultRandGen(),
	}
}

func (g *nullWrappedGener) gen() any {
	if g.inner == nil {
		return nil
	}
	return g.inner.gen()
}

func dateTimeFromString(string) types.Time {
	return fixedTime(mysql.TypeDatetime)
}

func convertETType(eType types.EvalType) byte {
	switch eType {
	case types.ETInt:
		return mysql.TypeLonglong
	case types.ETReal:
		return mysql.TypeDouble
	case types.ETDecimal:
		return mysql.TypeNewDecimal
	case types.ETDuration:
		return mysql.TypeDuration
	case types.ETJson:
		return mysql.TypeJSON
	case types.ETString:
		return mysql.TypeVarString
	case types.ETTimestamp:
		return mysql.TypeTimestamp
	case types.ETDatetime:
		return mysql.TypeDatetime
	default:
		return mysql.TypeUnspecified
	}
}

func fixedTime(tp byte) types.Time {
	return types.NewTime(types.FromDate(2019, 1, 1, 0, 0, 0, 0), tp, types.DefaultFsp)
}
