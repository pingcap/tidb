package ranger

import (
	"testing"

	"github.com/pingcap/tidb/pkg/types"
)

var benchmarkGetFullRangeSink []*point

func BenchmarkGetFullRangeRandomRanges(b *testing.B) {
	points := getFullRange()
	if len(points) != 2 || !points[0].start ||
		!points[0].value.IsNull() ||
		points[1].value.Kind() != types.KindMaxValue {
		b.Fatal("unexpected full range")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		points = getFullRange()
	}
	b.StopTimer()

	benchmarkGetFullRangeSink = points
}

func TestGetFullRangeStorageIndependence(t *testing.T) {
	first := getFullRange()
	second := getFullRange()
	if len(first) != 2 || cap(first) != 2 || first[0] == first[1] {
		t.Fatal("unexpected full-range storage")
	}
	first[0].value.SetInt64(99)
	first[0].excl = true
	if first[1].value.Kind() != types.KindMaxValue || first[1].excl {
		t.Fatal("full-range points alias")
	}
	if !second[0].value.IsNull() || second[0].excl {
		t.Fatal("full-range calls alias")
	}
}
