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

package statistics_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

// The merge walks two streams: partition TopN entries keyed by their
// encoded bytes, and partition histogram buckets keyed by their bound
// Datums. Both properties below hold only if the two streams agree on
// ordering and on how a value is rebuilt, which is type-dependent.
//
// typeCase supplies one field type plus two values in the order a
// partition histogram would store them (ascending under Datum.Compare,
// which is what the sampling builder sorts by).
type typeCase struct {
	name string
	ft   *types.FieldType
	lo   types.Datum // sorts first in the histogram
	hi   types.Datum
}

func ftOf(tp byte, mods ...func(*types.FieldType)) *types.FieldType {
	ft := types.NewFieldType(tp)
	for _, m := range mods {
		m(ft)
	}
	return ft
}

func unsigned(ft *types.FieldType) { ft.AddFlag(mysql.UnsignedFlag) }

func withCollate(cs, co string) func(*types.FieldType) {
	return func(ft *types.FieldType) {
		ft.SetCharset(cs)
		ft.SetCollate(co)
	}
}

func withElems(elems ...string) func(*types.FieldType) {
	return func(ft *types.FieldType) { ft.SetElems(elems) }
}

func withFlen(n int) func(*types.FieldType) {
	return func(ft *types.FieldType) { ft.SetFlen(n) }
}

func withDecimal(n int) func(*types.FieldType) {
	return func(ft *types.FieldType) { ft.SetDecimal(n) }
}

func enumD(name string, val uint64) types.Datum {
	var d types.Datum
	d.SetMysqlEnum(types.Enum{Name: name, Value: val}, charset.CollationUTF8MB4)
	return d
}

func setD(name string, val uint64) types.Datum {
	var d types.Datum
	d.SetMysqlSet(types.Set{Name: name, Value: val}, charset.CollationUTF8MB4)
	return d
}

// byteSize matches the storage width of the BIT column, which is what
// a real histogram bound carries; a trimmed literal would compare on a
// different number of bytes.
func bitD(v uint64, byteSize int) types.Datum {
	var d types.Datum
	d.SetMysqlBit(types.NewBinaryLiteralFromUint(v, byteSize))
	return d
}

func timeD(y, m, day int, tp byte) types.Datum {
	return types.NewTimeDatum(types.NewTime(types.FromDate(y, m, day, 0, 0, 0, 0), tp, 0))
}

func decD(v int64) types.Datum {
	var d types.Datum
	d.SetMysqlDecimal(types.NewDecFromInt(v))
	d.SetLength(decimalFlen)
	d.SetFrac(0)
	return d
}

// A DECIMAL histogram bound is re-encoded during the merge using the
// field type's precision, so the fixture needs a specified one.
const decimalFlen = 10

// sortKeyD builds the bound the stats builder stores for a string
// column under a non-binary collation: the collation sort key, not the
// original string.
func sortKeyD(s, collation string) types.Datum {
	d := types.NewBytesDatum(collate.GetCollator(collation).Key(s))
	d.SetCollation(charset.CollationBin)
	return d
}

func strD(s, collation string) types.Datum {
	d := types.NewStringDatum(s)
	d.SetCollation(collation)
	return d
}

// typeCases covers the column types a histogram can carry, including
// the signed/unsigned split and the string collation split, since both
// change how a value encodes relative to how it compares.
func typeCases() []typeCase {
	intKinds := []struct {
		name string
		tp   byte
	}{
		{"bool_tinyint", mysql.TypeTiny},
		{"smallint", mysql.TypeShort},
		{"mediumint", mysql.TypeInt24},
		{"int", mysql.TypeLong},
		{"bigint", mysql.TypeLonglong},
	}
	cases := make([]typeCase, 0, 32)
	for _, k := range intKinds {
		cases = append(cases,
			typeCase{k.name + "_signed", ftOf(k.tp), types.NewIntDatum(-3), types.NewIntDatum(7)},
			typeCase{k.name + "_unsigned", ftOf(k.tp, unsigned), types.NewUintDatum(3), types.NewUintDatum(7)},
		)
	}
	return append(cases,
		typeCase{"year", ftOf(mysql.TypeYear), types.NewIntDatum(2001), types.NewIntDatum(2020)},
		typeCase{"float", ftOf(mysql.TypeFloat), types.NewFloat32Datum(-1.5), types.NewFloat32Datum(2.5)},
		typeCase{"double", ftOf(mysql.TypeDouble), types.NewFloat64Datum(-1.5), types.NewFloat64Datum(2.5)},
		typeCase{"decimal", ftOf(mysql.TypeNewDecimal, withFlen(decimalFlen), withDecimal(0)), decD(-3), decD(7)},
		typeCase{"varchar_bin", ftOf(mysql.TypeVarchar, withCollate(charset.CharsetUTF8MB4, charset.CollationBin)),
			strD("aaa", charset.CollationBin), strD("zzz", charset.CollationBin)},
		// Stats store the collation sort key as the bound for a
		// non-binary collation, so the fixture must too.
		typeCase{"varchar_general_ci", ftOf(mysql.TypeVarchar, withCollate(charset.CharsetUTF8MB4, "utf8mb4_general_ci")),
			sortKeyD("AAA", "utf8mb4_general_ci"), sortKeyD("zzz", "utf8mb4_general_ci")},
		typeCase{"blob", ftOf(mysql.TypeBlob, withCollate(charset.CharsetBin, charset.CollationBin)),
			types.NewBytesDatum([]byte("aaa")), types.NewBytesDatum([]byte("zzz"))},
		// ENUM/SET encode by numeric value but compare by name, so the
		// declaration order below is deliberately the reverse of the
		// alphabetical order the histogram stores them in.
		typeCase{"enum", ftOf(mysql.TypeEnum, withElems("z", "a")), enumD("a", 2), enumD("z", 1)},
		typeCase{"set", ftOf(mysql.TypeSet, withElems("z", "a")), setD("a", 2), setD("z", 1)},
		typeCase{"bit", ftOf(mysql.TypeBit, withFlen(16)), bitD(1, 2), bitD(2, 2)},
		// BIT compares on the literal with leading zeros trimmed, so
		// 256 ("\x01\x00") sorts before 2 ("\x02") even though the
		// encoded key orders them numerically. This is the pair where
		// the two relations disagree.
		typeCase{"bit_multibyte", ftOf(mysql.TypeBit, withFlen(16)), bitD(256, 2), bitD(2, 2)},
		typeCase{"date", ftOf(mysql.TypeDate), timeD(2001, 1, 1, mysql.TypeDate), timeD(2020, 6, 6, mysql.TypeDate)},
		typeCase{"datetime", ftOf(mysql.TypeDatetime), timeD(2001, 1, 1, mysql.TypeDatetime), timeD(2020, 6, 6, mysql.TypeDatetime)},
		typeCase{"timestamp", ftOf(mysql.TypeTimestamp), timeD(2001, 1, 1, mysql.TypeTimestamp), timeD(2020, 6, 6, mysql.TypeTimestamp)},
		typeCase{"duration", ftOf(mysql.TypeDuration),
			types.NewDurationDatum(types.Duration{Duration: time.Hour}),
			types.NewDurationDatum(types.Duration{Duration: 5 * time.Hour})},
	)
}

// assertHistogramOrder guards the fixture itself: lo must really sort
// before hi, otherwise the case would not describe a histogram the
// sampling builder could produce.
func assertHistogramOrder(t *testing.T, sc *stmtctx.StatementContext, tc typeCase) {
	c, err := tc.lo.Compare(sc.TypeCtx(), &tc.hi, collate.GetBinaryCollator())
	require.NoError(t, err)
	require.Lessf(t, c, 0, "%s: fixture lo must sort before hi", tc.name)
}

// TestMergeAggregatesEqualValuesAcrossTypes: a value contributed by one
// partition's TopN and another partition's bucket upper must end up as
// one global TopN entry holding the sum, for every column type.
func TestMergeAggregatesEqualValuesAcrossTypes(t *testing.T) {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}
	for _, tc := range typeCases() {
		for _, onHi := range []bool{false, true} {
			runAggregationCase(t, sc, &killer, tc, onHi)
		}
	}
}

// runAggregationCase puts the shared value in one partition's TopN and
// the other partition's bucket upper. onHi selects which of the two
// fixture values is shared, because a mismatch between encoded order
// and datum order only shows up for one of them.
func runAggregationCase(t *testing.T, sc *stmtctx.StatementContext, killer *sqlkiller.SQLKiller, tc typeCase, onHi bool) {
	name := tc.name + "/topn_on_lo"
	shared := tc.lo
	if onHi {
		name = tc.name + "/topn_on_hi"
		shared = tc.hi
	}
	t.Run(name, func(t *testing.T) {
		assertHistogramOrder(t, sc, tc)
		loEnc, err := codec.EncodeKey(sc.TimeZone(), nil, shared)
		require.NoError(t, err)

		// p0 holds lo only in its TopN.
		p0TopN := statistics.NewTopN(1)
		p0TopN.AppendTopN(loEnc, 5)
		p0Hist := statistics.NewHistogram(1, 1, 0, 0, tc.ft, chunk.InitialCapacity, 0)

		// p1 holds both values as single-value buckets.
		p1Hist := statistics.NewHistogram(1, 2, 0, 0, tc.ft, chunk.InitialCapacity, 0)
		if onHi {
			p1Hist.AppendBucket(&tc.lo, &tc.lo, 7, 7)
			p1Hist.AppendBucket(&tc.hi, &tc.hi, 10, 3)
		} else {
			p1Hist.AppendBucket(&tc.lo, &tc.lo, 3, 3)
			p1Hist.AppendBucket(&tc.hi, &tc.hi, 10, 7)
		}

		gTopN, _, err := statistics.MergePartTopNAndHistToGlobal(
			sc, killer,
			[]*statistics.TopN{p0TopN, statistics.NewTopN(0)},
			[]*statistics.Histogram{p0Hist, p1Hist},
			3, 2, false,
		)
		require.NoError(t, err)
		require.NotNil(t, gTopN)

		byKey := map[string]uint64{}
		for _, m := range gTopN.TopN {
			byKey[string(m.Encoded)] += m.Count
		}
		require.Lenf(t, gTopN.TopN, len(byKey),
			"the same value must not occupy two TopN slots; got %v", gTopN.TopN)
		require.Equalf(t, uint64(8), byKey[string(loEnc)],
			"the shared value must aggregate its TopN 5 and its bucket repeat 3")
	})
}

// TestMergeRebuildsNonPromotedTopNAcrossTypes: a TopN value that loses
// the global slot is rebuilt into a virtual histogram, which requires a
// datum compatible with the histogram's own field type.
func TestMergeRebuildsNonPromotedTopNAcrossTypes(t *testing.T) {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}
	for _, tc := range typeCases() {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("merge panicked rebuilding a non-promoted TopN value: %v", r)
				}
			}()
			loEnc, err := codec.EncodeKey(sc.TimeZone(), nil, tc.lo)
			require.NoError(t, err)

			p0TopN := statistics.NewTopN(1)
			p0TopN.AppendTopN(loEnc, 5)
			p0Hist := statistics.NewHistogram(1, 1, 0, 0, tc.ft, chunk.InitialCapacity, 0)

			// hi's 10 rows outrank lo's 5, so lo is not promoted.
			p1Hist := statistics.NewHistogram(1, 1, 0, 0, tc.ft, chunk.InitialCapacity, 0)
			p1Hist.AppendBucket(&tc.hi, &tc.hi, 10, 10)

			gTopN, gHist, err := statistics.MergePartTopNAndHistToGlobal(
				sc, &killer,
				[]*statistics.TopN{p0TopN, statistics.NewTopN(0)},
				[]*statistics.Histogram{p0Hist, p1Hist},
				1, 2, false,
			)
			require.NoError(t, err)

			var total int64
			if gHist.Len() > 0 {
				total = gHist.Buckets[gHist.Len()-1].Count
			}
			if gTopN != nil {
				total += int64(gTopN.TotalCount())
			}
			require.Equalf(t, int64(15), total,
				"all 15 rows must survive the merge, in the histogram or the TopN")
		})
	}
}

// indexBound is how an index histogram stores a bound: the encoded key
// itself, as a Bytes datum, which is also the form its TopN is keyed by.
func indexBound(t *testing.T, sc *stmtctx.StatementContext, d types.Datum) types.Datum {
	enc, err := codec.EncodeKey(sc.TimeZone(), nil, d)
	require.NoError(t, err)
	var out types.Datum
	out.SetBytes(enc)
	return out
}

// TestMergeIndexPathAcrossTypes runs both properties through the index
// path, where bounds and TopN keys are the same encoded bytes. If the
// column failures come from the two streams disagreeing on ordering and
// kind, the index path should be immune.
func TestMergeIndexPathAcrossTypes(t *testing.T) {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}
	blobFt := types.NewFieldType(mysql.TypeBlob)
	for _, tc := range typeCases() {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("index merge panicked: %v", r)
				}
			}()
			// Index bounds sort by encoded bytes, which may order the
			// pair differently than the column form does.
			lo, hi := indexBound(t, sc, tc.lo), indexBound(t, sc, tc.hi)
			if bytes.Compare(lo.GetBytes(), hi.GetBytes()) > 0 {
				lo, hi = hi, lo
			}

			p0TopN := statistics.NewTopN(1)
			p0TopN.AppendTopN(lo.GetBytes(), 5)
			p0Hist := statistics.NewHistogram(1, 1, 0, 0, blobFt, chunk.InitialCapacity, 0)

			p1Hist := statistics.NewHistogram(1, 2, 0, 0, blobFt, chunk.InitialCapacity, 0)
			p1Hist.AppendBucket(&lo, &lo, 3, 3)
			p1Hist.AppendBucket(&hi, &hi, 10, 7)

			gTopN, _, err := statistics.MergePartTopNAndHistToGlobal(
				sc, &killer,
				[]*statistics.TopN{p0TopN, statistics.NewTopN(0)},
				[]*statistics.Histogram{p0Hist, p1Hist},
				3, 2, true,
			)
			require.NoError(t, err)
			require.NotNil(t, gTopN)

			byKey := map[string]uint64{}
			for _, m := range gTopN.TopN {
				byKey[string(m.Encoded)] += m.Count
			}
			require.Lenf(t, gTopN.TopN, len(byKey), "index: value must not occupy two TopN slots")
			require.Equalf(t, uint64(8), byKey[string(lo.GetBytes())],
				"index: shared value must aggregate 5 + 3")
		})
	}
}

// TestMergeAggregatesBothValuesAcrossTypes stresses the ordering of the
// TopN stream itself: both values appear in one partition's TopN and as
// the other partition's bucket uppers, so the two streams must agree on
// order, not merely on how a single value compares.
func TestMergeAggregatesBothValuesAcrossTypes(t *testing.T) {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
	killer := sqlkiller.SQLKiller{}
	for _, tc := range typeCases() {
		t.Run(tc.name, func(t *testing.T) {
			assertHistogramOrder(t, sc, tc)
			loEnc, err := codec.EncodeKey(sc.TimeZone(), nil, tc.lo)
			require.NoError(t, err)
			hiEnc, err := codec.EncodeKey(sc.TimeZone(), nil, tc.hi)
			require.NoError(t, err)

			p0TopN := statistics.NewTopN(2)
			p0TopN.AppendTopN(loEnc, 5)
			p0TopN.AppendTopN(hiEnc, 4)
			p0TopN.Sort()
			p0Hist := statistics.NewHistogram(1, 2, 0, 0, tc.ft, chunk.InitialCapacity, 0)

			p1Hist := statistics.NewHistogram(1, 2, 0, 0, tc.ft, chunk.InitialCapacity, 0)
			p1Hist.AppendBucket(&tc.lo, &tc.lo, 3, 3)
			p1Hist.AppendBucket(&tc.hi, &tc.hi, 10, 7)

			gTopN, _, err := statistics.MergePartTopNAndHistToGlobal(
				sc, &killer,
				[]*statistics.TopN{p0TopN, statistics.NewTopN(0)},
				[]*statistics.Histogram{p0Hist, p1Hist},
				4, 2, false,
			)
			require.NoError(t, err)
			require.NotNil(t, gTopN)

			byKey := map[string]uint64{}
			for _, m := range gTopN.TopN {
				byKey[string(m.Encoded)] += m.Count
			}
			require.Lenf(t, gTopN.TopN, len(byKey),
				"no value may occupy two TopN slots; got %v", gTopN.TopN)
			require.Equalf(t, uint64(8), byKey[string(loEnc)], "lo must aggregate 5 + 3")
			require.Equalf(t, uint64(11), byKey[string(hiEnc)], "hi must aggregate 4 + 7")
		})
	}
}
