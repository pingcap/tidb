// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package spans_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/stretchr/testify/require"
)

func TestSortedBasic(t *testing.T) {
	type Case struct {
		InputSequence  []spans.Valued
		RetainLessThan spans.Value
		Result         []spans.Valued
	}

	run := func(t *testing.T, c Case) {
		full := spans.Sorted(spans.NewFullWith(spans.Full(), 0))
		fmt.Println(t.Name())
		for _, i := range c.InputSequence {
			full.Merge(i)
			spans.Debug(full)
		}

		var result []spans.Valued
		full.TraverseValuesLessThan(c.RetainLessThan, func(v spans.Valued) bool {
			result = append(result, v)
			return true
		})

		require.True(t, spans.ValuedSetEquals(result, c.Result), "%s\nvs\n%s", result, c.Result)
	}

	cases := []Case{
		{
			InputSequence: []spans.Valued{
				kv(s("0001", "0002"), 1),
				kv(s("0002", "0003"), 2),
			},
			Result: []spans.Valued{
				kv(s("", "0001"), 0),
				kv(s("0001", "0002"), 1),
				kv(s("0002", "0003"), 2),
				kv(s("0003", ""), 0),
			},
			RetainLessThan: 10,
		},
		{
			InputSequence: []spans.Valued{
				kv(s("0001", "0002"), 1),
				kv(s("0002", "0003"), 2),
				kv(s("0001", "0003"), 4),
			},
			RetainLessThan: 1,
			Result: []spans.Valued{
				kv(s("", "0001"), 0),
				kv(s("0003", ""), 0),
			},
		},
		{
			InputSequence: []spans.Valued{
				kv(s("0001", "0004"), 3),
				kv(s("0004", "0008"), 5),
				kv(s("0001", "0007"), 4),
				kv(s("", "0002"), 2),
			},
			RetainLessThan: 5,
			Result: []spans.Valued{
				kv(s("", "0001"), 2),
				kv(s("0001", "0004"), 4),
				kv(s("0008", ""), 0),
			},
		},
		{
			InputSequence: []spans.Valued{
				kv(s("0001", "0004"), 3),
				kv(s("0004", "0008"), 5),
				kv(s("0001", "0007"), 4),
				kv(s("", "0002"), 2),
				kv(s("0001", "0004"), 5),
				kv(s("0008", ""), 10),
				kv(s("", "0001"), 20),
			},
			RetainLessThan: 11,
			Result: []spans.Valued{
				kv(s("0001", "0008"), 5),
				kv(s("0008", ""), 10),
			},
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("#%d", i+1), func(t *testing.T) { run(t, c) })
	}
}
