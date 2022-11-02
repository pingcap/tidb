package spans_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/stretchr/testify/require"
)

func s(a, b string) spans.Span {
	return spans.Span{
		StartKey: []byte(a),
		EndKey:   []byte(b),
	}
}

func kv(s spans.Span, v spans.Value) spans.Valued {
	return spans.Valued{
		Key:   s,
		Value: v,
	}
}

func TestBasic(t *testing.T) {
	type Case struct {
		InputSequence []spans.Valued
		Result        []spans.Valued
	}

	useGreater := func(a spans.Value, b spans.Value) spans.Value {
		if a > b {
			return a
		}
		return b
	}

	run := func(t *testing.T, c Case) {
		full := spans.NewFullWith(0, useGreater)
		fmt.Println(t.Name())
		for _, i := range c.InputSequence {
			full.Merge(i)
			var result []spans.Valued
			full.Traverse(func(v spans.Valued) bool {
				result = append(result, v)
				return true
			})
			fmt.Printf("%s -> %s\n", i, result)
		}

		var result []spans.Valued
		full.Traverse(func(v spans.Valued) bool {
			result = append(result, v)
			return true
		})

		require.Equal(t, result, c.Result, "%s\nvs\n%s", result, c.Result)
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
		},
		{
			InputSequence: []spans.Valued{
				kv(s("0001", "0002"), 1),
				kv(s("0002", "0003"), 2),
				kv(s("0001", "0003"), 4),
			},
			Result: []spans.Valued{
				kv(s("", "0001"), 0),
				kv(s("0001", "0003"), 4),
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
			Result: []spans.Valued{
				kv(s("", "0001"), 2),
				kv(s("0001", "0004"), 4),
				kv(s("0004", "0008"), 5),
				kv(s("0008", ""), 0),
			},
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("#%d", i+1), func(t *testing.T) { run(t, c) })
	}
}
