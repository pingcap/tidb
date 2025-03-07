// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package split_test

import (
	"fmt"
	"testing"

	split "github.com/pingcap/tidb/br/pkg/restore/split"
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

func exportString(startKey, endKey, size string, number int) string {
	return fmt.Sprintf("([%s, %s), %s MB, %d)", startKey, endKey, size, number)
}

func TestSumSorted(t *testing.T) {
	cases := []struct {
		values []split.Valued
		result []uint64
		strs   []string
	}{
		{
			values: []split.Valued{
				v("a", "f", mb(100)),
				v("a", "c", mb(200)),
				v("d", "g", mb(100)),
			},
			result: []uint64{0, 250, 25, 75, 50, 0},
			strs: []string{
				exportString("61", "66", "100.00", 100),
				exportString("61", "63", "200.00", 200),
				exportString("64", "67", "100.00", 100),
			},
		},
		{
			values: []split.Valued{
				v("a", "f", mb(100)),
				v("a", "c", mb(200)),
				v("d", "f", mb(100)),
			},
			result: []uint64{0, 250, 25, 125, 0},
			strs: []string{
				exportString("61", "66", "100.00", 100),
				exportString("61", "63", "200.00", 200),
				exportString("64", "66", "100.00", 100),
			},
		},
		{
			values: []split.Valued{
				v("a", "f", mb(100)),
				v("a", "c", mb(200)),
				v("c", "f", mb(100)),
			},
			result: []uint64{0, 250, 150, 0},
			strs: []string{
				exportString("61", "66", "100.00", 100),
				exportString("61", "63", "200.00", 200),
				exportString("63", "66", "100.00", 100),
			},
		},
		{
			values: []split.Valued{
				v("a", "f", mb(100)),
				v("a", "c", mb(200)),
				v("c", "f", mb(100)),
				v("da", "db", mb(100)),
			},
			result: []uint64{0, 250, 50, 150, 50, 0},
			strs: []string{
				exportString("61", "66", "100.00", 100),
				exportString("61", "63", "200.00", 200),
				exportString("63", "66", "100.00", 100),
				exportString("6461", "6462", "100.00", 100),
			},
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
			strs: []string{
				exportString("61", "66", "100.00", 100),
				exportString("61", "63", "200.00", 200),
				exportString("63", "66", "100.00", 100),
				exportString("6461", "6462", "100.00", 100),
				exportString("6362", "6462", "100.00", 100),
			},
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
			strs: []string{
				exportString("61", "66", "100.00", 100),
				exportString("61", "63", "200.00", 200),
				exportString("63", "66", "100.00", 100),
				exportString("6461", "6462", "100.00", 100),
				exportString("6362", "66", "150.00", 150),
			},
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
			strs: []string{
				exportString("61", "66", "100.00", 100),
				exportString("61", "63", "200.00", 200),
				exportString("63", "66", "100.00", 100),
				exportString("6461", "6462", "100.00", 100),
				exportString("6362", "6466", "150.00", 150),
			},
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
			strs: []string{
				exportString("61", "66", "100.00", 100),
				exportString("61", "63", "200.00", 200),
				exportString("63", "66", "100.00", 100),
				exportString("6461", "6462", "100.00", 100),
				exportString("6362", "6466", "150.00", 150),
			},
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
			strs: []string{
				exportString("61", "66", "100.00", 100),
				exportString("61", "63", "200.00", 200),
				exportString("63", "66", "100.00", 100),
				exportString("6461", "6462", "100.00", 100),
				exportString("63", "6466", "150.00", 150),
			},
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
			strs: []string{
				exportString("61", "66", "100.00", 100),
				exportString("61", "63", "200.00", 200),
				exportString("63", "66", "100.00", 100),
				exportString("6461", "6462", "100.00", 100),
				exportString("63", "66", "150.00", 150),
			},
		},
	}

	for _, ca := range cases {
		full := split.NewSplitHelper()
		for i, v := range ca.values {
			require.Equal(t, ca.strs[i], v.String())
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
