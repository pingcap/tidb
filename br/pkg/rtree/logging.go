// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package rtree

import (
	"fmt"

	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"go.uber.org/zap/zapcore"
)

// String formats a range to a string.
func (rg KeyRange) String() string {
	return fmt.Sprintf("[%s, %s)", redact.Key(rg.StartKey), redact.Key(rg.EndKey))
}

// ZapRanges make zap fields for logging Range slice.
func ZapRanges(ranges []KeyRange) zapcore.Field {
	return logutil.AbbreviatedStringers("ranges", ranges)
}
