// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package split_test

import (
	"testing"

	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/stretchr/testify/require"
)

func v(s, e string, val split.Value) split.Valued {
	return split.Valued{
		Key: split.Span{
			StartKey: []byte(s),
			EndKey:   []byte(e),
		},
		Value: val,
	}
}

func mb(b uint64) split.Value {
	return split.Value{
		Size:   b * 1024 * 1024,
		Number: int64(b),
	}
}

func TestSumSorted(t *testing.T) {
	cases := []struct {
		values []split.Valued
		result []uint64
	}{
		{
			values: []split.Valued{
				v("a", "f", mb(100)),
				v("a", "c", mb(200)),
				v("d", "g", mb(100)),
			},
			result: []uint64{0, 250, 25, 75, 50, 0},
		},
		{
			values: []split.Valued{
				v("a", "f", mb(100)),
				v("a", "c", mb(200)),
				v("d", "f", mb(100)),
			},
			result: []uint64{0, 250, 25, 125, 0},
		},
		{
			values: []split.Valued{
				v("a", "f", mb(100)),
				v("a", "c", mb(200)),
				v("c", "f", mb(100)),
			},
			result: []uint64{0, 250, 150, 0},
		},
		{
			values: []split.Valued{
				v("a", "f", mb(100)),
				v("a", "c", mb(200)),
				v("c", "f", mb(100)),
				v("da", "db", mb(100)),
			},
			result: []uint64{0, 250, 50, 150, 50, 0},
		},
		{
			values: []split.Valued{
				v("a", "f", mb(100)),
				v("a", "c", mb(200)),
				v("c", "f", mb(100)),
				v("da", "db", mb(100)),
				v("cb", "db", mb(100)),
			},
			result: []uint64{0, 250, 25, 75, 200, 50, 0},
		},
		{
			values: []split.Valued{
				v("a", "f", mb(100)),
				v("a", "c", mb(200)),
				v("c", "f", mb(100)),
				v("da", "db", mb(100)),
				v("cb", "f", mb(150)),
			},
			result: []uint64{0, 250, 25, 75, 200, 100, 0},
		},
		{
			values: []split.Valued{
				v("a", "f", mb(100)),
				v("a", "c", mb(200)),
				v("c", "f", mb(100)),
				v("da", "db", mb(100)),
				v("cb", "df", mb(150)),
			},
			result: []uint64{0, 250, 25, 75, 200, 75, 25, 0},
		},
		{
			values: []split.Valued{
				v("a", "f", mb(100)),
				v("a", "c", mb(200)),
				v("c", "f", mb(100)),
				v("da", "db", mb(100)),
				v("cb", "df", mb(150)),
			},
			result: []uint64{0, 250, 25, 75, 200, 75, 25, 0},
		},
		{
			values: []split.Valued{
				v("a", "f", mb(100)),
				v("a", "c", mb(200)),
				v("c", "f", mb(100)),
				v("da", "db", mb(100)),
				v("c", "df", mb(150)),
			},
			result: []uint64{0, 250, 100, 200, 75, 25, 0},
		},
		{
			values: []split.Valued{
				v("a", "f", mb(100)),
				v("a", "c", mb(200)),
				v("c", "f", mb(100)),
				v("da", "db", mb(100)),
				v("c", "f", mb(150)),
			},
			result: []uint64{0, 250, 100, 200, 100, 0},
		},
	}

	for _, ca := range cases {
		full := split.NewSplitHelper()
		for _, v := range ca.values {
			full.Merge(v)
		}

		i := 0
		full.Traverse(func(v split.Valued) bool {
			require.Equal(t, mb(ca.result[i]), v.Value)
			i++
			return true
		})
	}
}
