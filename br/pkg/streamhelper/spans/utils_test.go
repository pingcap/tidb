// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package spans_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/stretchr/testify/require"
)

func TestValuedEquals(t *testing.T) {
	s := func(start, end string, val spans.Value) spans.Valued {
		return spans.Valued{
			Key: spans.Span{
				StartKey: []byte(start),
				EndKey:   []byte(end),
			},
			Value: val,
		}
	}
	type Case struct {
		inputA   []spans.Valued
		inputB   []spans.Valued
		required bool
	}
	cases := []Case{
		{
			inputA:   []spans.Valued{s("0001", "0002", 3)},
			inputB:   []spans.Valued{s("0001", "0003", 3)},
			required: false,
		},
		{
			inputA:   []spans.Valued{s("0001", "0002", 3)},
			inputB:   []spans.Valued{s("0001", "0002", 3)},
			required: true,
		},
		{
			inputA:   []spans.Valued{s("0001", "0003", 3)},
			inputB:   []spans.Valued{s("0001", "0002", 3), s("0002", "0003", 3)},
			required: true,
		},
		{
			inputA:   []spans.Valued{s("0001", "0003", 4)},
			inputB:   []spans.Valued{s("0001", "0002", 3), s("0002", "0003", 3)},
			required: false,
		},
		{
			inputA:   []spans.Valued{s("0001", "0003", 3)},
			inputB:   []spans.Valued{s("0001", "0002", 4), s("0002", "0003", 3)},
			required: false,
		},
		{
			inputA:   []spans.Valued{s("0001", "0003", 3)},
			inputB:   []spans.Valued{s("0001", "0002", 3), s("0002", "0004", 3)},
			required: false,
		},
		{
			inputA:   []spans.Valued{s("", "0003", 3)},
			inputB:   []spans.Valued{s("0001", "0002", 3), s("0002", "0003", 3)},
			required: false,
		},
		{
			inputA:   []spans.Valued{s("0001", "", 1)},
			inputB:   []spans.Valued{s("0001", "0003", 1), s("0004", "", 1)},
			required: false,
		},
		{
			inputA:   []spans.Valued{s("0001", "0004", 1), s("0001", "0002", 1)},
			inputB:   []spans.Valued{s("0001", "0002", 1), s("0001", "0004", 1)},
			required: true,
		},
	}
	run := func(t *testing.T, c Case) {
		require.Equal(t, c.required, spans.ValuedSetEquals(c.inputA, c.inputB))
		require.Equal(t, c.required, spans.ValuedSetEquals(c.inputB, c.inputA))
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("#%d", i+1), func(t *testing.T) { run(t, c) })
	}
}
