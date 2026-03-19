// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package rtree_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestLogRanges(t *testing.T) {
	cases := []struct {
		count  int
		expect string
	}{
		{0, `{"ranges": []}`},
		{1, `{"ranges": ["[30, 31)"]}`},
		{2, `{"ranges": ["[30, 31)", "[31, 32)"]}`},
		{3, `{"ranges": ["[30, 31)", "[31, 32)", "[32, 33)"]}`},
		{4, `{"ranges": ["[30, 31)", "(skip 2)", "[33, 34)"]}`},
		{5, `{"ranges": ["[30, 31)", "(skip 3)", "[34, 35)"]}`},
		{6, `{"ranges": ["[30, 31)", "(skip 4)", "[35, 36)"]}`},
		{1024, `{"ranges": ["[30, 31)", "(skip 1022)", "[31303233, 31303234)"]}`},
	}

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	for _, cs := range cases {
		ranges := make([]rtree.KeyRange, cs.count)
		for j := range cs.count {
			ranges[j] = newRange(fmt.Appendf(nil, "%d", j), fmt.Appendf(nil, "%d", j+1)).KeyRange
		}
		out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{rtree.ZapRanges(ranges)})
		require.NoError(t, err)
		require.Equal(t, cs.expect, strings.TrimRight(out.String(), "\n"))
	}
}
