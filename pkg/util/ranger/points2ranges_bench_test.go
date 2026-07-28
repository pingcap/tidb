package ranger

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
)

var (
	benchmarkPoints2RangesSink         Ranges
	benchmarkPoints2RangesFallbackSink bool
	benchmarkPoints2RangesErrSink      error
)

func BenchmarkPoints2RangesRandomRanges(b *testing.B) {
	sctx := &rangerctx.RangerContext{
		TypeCtx: types.DefaultStmtNoWarningContext,
		ErrCtx:  errctx.StrictNoWarningContext,
	}
	intType := types.NewFieldType(mysql.TypeLong)
	intType.AddFlag(mysql.NotNullFlag)

	for _, rangeCount := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("ranges=%d", rangeCount), func(b *testing.B) {
			points := makeIntegerRangePoints(rangeCount)
			ranges, fallback, err := points2Ranges(sctx, points, intType, 0)
			if err != nil {
				b.Fatal(err)
			}
			if fallback || len(ranges) != rangeCount {
				b.Fatalf("unexpected baseline result: fallback=%v ranges=%d", fallback, len(ranges))
			}

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				ranges, fallback, err = points2Ranges(sctx, points, intType, 0)
			}
			b.StopTimer()

			benchmarkPoints2RangesSink = ranges
			benchmarkPoints2RangesFallbackSink = fallback
			benchmarkPoints2RangesErrSink = err
			if err != nil {
				b.Fatal(err)
			}
			if fallback || len(ranges) != rangeCount {
				b.Fatalf("unexpected result: fallback=%v ranges=%d", fallback, len(ranges))
			}
		})
	}
}

func makeIntegerRangePoints(rangeCount int) []*point {
	points := make([]*point, 0, rangeCount*2)
	for i := 0; i < rangeCount; i++ {
		low := int64(i*100 + 1)
		points = append(points,
			&point{value: types.NewIntDatum(low), start: true},
			&point{value: types.NewIntDatum(low + 5)},
		)
	}
	return points
}

func TestSingleColumnRangeStorageIndependence(t *testing.T) {
	sctx := &rangerctx.RangerContext{
		TypeCtx: types.DefaultStmtNoWarningContext,
		ErrCtx:  errctx.StrictNoWarningContext,
	}
	intType := types.NewFieldType(mysql.TypeLong)
	intType.AddFlag(mysql.NotNullFlag)

	ranges, fallback, err := points2Ranges(sctx, makeIntegerRangePoints(2), intType, 0)
	if err != nil {
		t.Fatal(err)
	}
	if fallback || len(ranges) != 2 {
		t.Fatalf("unexpected result: fallback=%v ranges=%d", fallback, len(ranges))
	}
	for i, ran := range ranges {
		if len(ran.LowVal) != 1 || cap(ran.LowVal) != 1 ||
			len(ran.HighVal) != 1 || cap(ran.HighVal) != 1 ||
			len(ran.Collators) != 1 || cap(ran.Collators) != 1 {
			t.Fatalf("range %d has unexpected single-column storage", i)
		}
	}

	originalHigh := ranges[0].HighVal[0].GetInt64()
	originalSiblingLow := ranges[1].LowVal[0].GetInt64()
	originalSiblingCollator := ranges[1].Collators[0]
	ranges[0].LowVal[0].SetInt64(999)
	ranges[0].Collators[0] = nil

	if ranges[0].HighVal[0].GetInt64() != originalHigh {
		t.Fatal("low and high values alias")
	}
	if ranges[1].LowVal[0].GetInt64() != originalSiblingLow {
		t.Fatal("sibling range values alias")
	}
	if ranges[1].Collators[0] != originalSiblingCollator {
		t.Fatal("sibling range collators alias")
	}
}

func TestConvertPointsBatchStorageIndependence(t *testing.T) {
	sctx := &rangerctx.RangerContext{
		TypeCtx: types.DefaultStmtNoWarningContext,
		ErrCtx:  errctx.StrictNoWarningContext,
	}
	intType := types.NewFieldType(mysql.TypeLong)
	intType.AddFlag(mysql.NotNullFlag)

	input := makeIntegerRangePoints(2)
	originalPointers := append([]*point(nil), input...)
	converted, err := convertPoints(sctx, input, intType, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(converted) != 4 {
		t.Fatalf("unexpected converted length: %d", len(converted))
	}
	for i := range converted {
		if converted[i] == originalPointers[i] {
			t.Fatalf("point %d was not independently converted", i)
		}
		for j := i + 1; j < len(converted); j++ {
			if converted[i] == converted[j] {
				t.Fatalf("converted points %d and %d alias", i, j)
			}
		}
	}
	converted[0].value.SetInt64(999)
	if converted[1].value.GetInt64() == 999 ||
		originalPointers[0].value.GetInt64() == 999 {
		t.Fatal("converted point mutation changed another point")
	}

	sentinels := []*point{
		{value: types.MinNotNullDatum(), start: true},
		{value: types.MaxValueDatum()},
	}
	convertedSentinels, err := convertPoints(sctx, sentinels, intType, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if convertedSentinels[0] != sentinels[0] || convertedSentinels[1] != sentinels[1] {
		t.Fatal("sentinel pointer identity changed")
	}
}
