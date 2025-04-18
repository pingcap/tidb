// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package rtree

import (
	"fmt"

	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// String formats a range to a string.
func (rg KeyRange) String() string {
	return fmt.Sprintf("[%s, %s)", redact.Key(rg.StartKey), redact.Key(rg.EndKey))
}

// ZapRanges make zap fields for logging Range slice.
func ZapRanges(ranges []KeyRange) zapcore.Field {
	return zap.Object("ranges", rangesMarshaler(ranges))
}

type rangesMarshaler []KeyRange

func (rs rangesMarshaler) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, r := range rs {
		encoder.AppendString(r.String())
	}
	return nil
}

func (rs rangesMarshaler) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	total := len(rs)
	encoder.AddInt("total", total)
	elements := make([]string, 0, total)
	for _, r := range rs {
		elements = append(elements, r.String())
	}
	_ = encoder.AddArray("ranges", logutil.AbbreviatedArrayMarshaler(elements))
	return nil
}
