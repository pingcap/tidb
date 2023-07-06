// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

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

	run := func(t *testing.T, c Case) {
		full := spans.NewFullWith(spans.Full(), 0)
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
		{
			InputSequence: []spans.Valued{
				kv(s("0001", "0004"), 3),
				kv(s("0004", "0008"), 5),
				kv(s("0001", "0009"), 4),
			},
			Result: []spans.Valued{
				kv(s("", "0001"), 0),
				kv(s("0001", "0004"), 4),
				kv(s("0004", "0008"), 5),
				kv(s("0008", "0009"), 4),
				kv(s("0009", ""), 0),
			},
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("#%d", i+1), func(t *testing.T) { run(t, c) })
	}
}

func TestSubRange(t *testing.T) {
	type Case struct {
		Range         []spans.Span
		InputSequence []spans.Valued
		Result        []spans.Valued
	}

	run := func(t *testing.T, c Case) {
		full := spans.NewFullWith(c.Range, 0)
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

		require.True(t, spans.ValuedSetEquals(result, c.Result), "%s\nvs\n%s", result, c.Result)
	}

	cases := []Case{
		{
			Range: []spans.Span{s("0001", "0004"), s("0008", "")},
			InputSequence: []spans.Valued{
				kv(s("0001", "0007"), 42),
				kv(s("0000", "0009"), 41),
				kv(s("0002", "0005"), 43),
			},
			Result: []spans.Valued{
				kv(s("0001", "0002"), 42),
				kv(s("0002", "0004"), 43),
				kv(s("0008", "0009"), 41),
				kv(s("0009", ""), 0),
			},
		},
		{
			Range: []spans.Span{
				s("0001", "0004"),
				s("0008", "")},
			InputSequence: []spans.Valued{kv(s("", ""), 42)},
			Result: []spans.Valued{
				kv(s("0001", "0004"), 42),
				kv(s("0008", ""), 42),
			},
		},
		{
			Range: []spans.Span{
				s("0001", "0004"),
				s("0005", "0008"),
			},
			InputSequence: []spans.Valued{
				kv(s("0001", "0002"), 42),
				kv(s("0002", "0008"), 43),
				kv(s("0004", "0007"), 45),
				kv(s("0000", "00015"), 48),
			},
			Result: []spans.Valued{
				kv(s("0001", "00015"), 48),
				kv(s("00015", "0002"), 42),
				kv(s("0002", "0004"), 43),
				kv(s("0005", "0007"), 45),
				kv(s("0007", "0008"), 43),
			},
		},
		{
			Range: []spans.Span{
				s("0001", "0004"),
				s("0005", "0008"),
			},
			InputSequence: []spans.Valued{
				kv(s("0004", "0008"), 32),
				kv(s("00041", "0007"), 33),
				kv(s("0004", "00041"), 99999),
				kv(s("0005", "0006"), 34),
			},
			Result: []spans.Valued{
				kv(s("0001", "0004"), 0),
				kv(s("0005", "0006"), 34),
				kv(s("0006", "0007"), 33),
				kv(s("0007", "0008"), 32),
			},
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("#%d", i+1), func(t *testing.T) { run(t, c) })
	}
}
