// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package streamhelper_test

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/require"
)

func TestInsert(t *testing.T) {
	cases := []func(func(ts uint64, a, b string)){
		func(insert func(ts uint64, a, b string)) {
			insert(1, "", "01")
			insert(1, "01", "02")
			insert(2, "02", "022")
			insert(4, "022", "")
		},
		func(insert func(ts uint64, a, b string)) {
			insert(1, "", "01")
			insert(2, "", "01")
			insert(2, "011", "02")
			insert(1, "", "")
			insert(65, "03", "04")
		},
	}

	for _, c := range cases {
		cps := streamhelper.NewCheckpoints()
		expected := map[uint64]*streamhelper.RangesSharesTS{}
		checkpoint := uint64(math.MaxUint64)
		insert := func(ts uint64, a, b string) {
			cps.InsertRange(ts, kv.KeyRange{
				StartKey: []byte(a),
				EndKey:   []byte(b),
			})
			i, ok := expected[ts]
			if !ok {
				expected[ts] = &streamhelper.RangesSharesTS{TS: ts, Ranges: []kv.KeyRange{{StartKey: []byte(a), EndKey: []byte(b)}}}
			} else {
				i.Ranges = append(i.Ranges, kv.KeyRange{StartKey: []byte(a), EndKey: []byte(b)})
			}
			if ts < checkpoint {
				checkpoint = ts
			}
		}
		c(insert)
		require.Equal(t, checkpoint, cps.CheckpointTS())
		rngs := cps.PopRangesWithGapGT(0)
		for _, rng := range rngs {
			other := expected[rng.TS]
			require.Equal(t, other, rng)
		}
	}
}

func TestMergeRanges(t *testing.T) {
	r := func(a, b string) kv.KeyRange {
		return kv.KeyRange{StartKey: []byte(a), EndKey: []byte(b)}
	}
	type Case struct {
		expected  []kv.KeyRange
		parameter []kv.KeyRange
	}
	cases := []Case{
		{
			parameter: []kv.KeyRange{r("01", "01111"), r("0111", "0112")},
			expected:  []kv.KeyRange{r("01", "0112")},
		},
		{
			parameter: []kv.KeyRange{r("01", "03"), r("02", "04")},
			expected:  []kv.KeyRange{r("01", "04")},
		},
		{
			parameter: []kv.KeyRange{r("04", "08"), r("09", "10")},
			expected:  []kv.KeyRange{r("04", "08"), r("09", "10")},
		},
		{
			parameter: []kv.KeyRange{r("01", "03"), r("02", "04"), r("05", "07"), r("08", "09")},
			expected:  []kv.KeyRange{r("01", "04"), r("05", "07"), r("08", "09")},
		},
		{
			parameter: []kv.KeyRange{r("01", "02"), r("012", "")},
			expected:  []kv.KeyRange{r("01", "")},
		},
		{
			parameter: []kv.KeyRange{r("", "01"), r("02", "03"), r("021", "")},
			expected:  []kv.KeyRange{r("", "01"), r("02", "")},
		},
		{
			parameter: []kv.KeyRange{r("", "01"), r("001", "")},
			expected:  []kv.KeyRange{r("", "")},
		},
	}

	for i, c := range cases {
		result := streamhelper.CollpaseRanges(len(c.parameter), func(i int) kv.KeyRange {
			return c.parameter[i]
		})
		require.Equal(t, c.expected, result, "case = %d", i)
	}

}
