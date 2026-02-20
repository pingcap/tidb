// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/tikv/client-go/v2/util"
)

// SlowLogFieldAccessor defines how to get or set a specific field in SlowQueryLogItems.
// - Parse converts a user-provided threshold string into the proper type for comparison.
// - Setter is optional and pre-fills the field before matching if it needs explicit preparation.
// - Match evaluates whether the field in SlowQueryLogItems meets a specific threshold.
//   - threshold is the value to compare against when determining a match.
type SlowLogFieldAccessor struct {
	Parse  func(string) (any, error)
	Setter func(ctx context.Context, seVars *SessionVars, items *SlowQueryLogItems)
	Match  func(seVars *SessionVars, items *SlowQueryLogItems, threshold any) bool
}

func makeExecDetailAccessor(parse func(string) (any, error),
	match func(*execdetails.ExecDetails, any) bool) SlowLogFieldAccessor {
	return SlowLogFieldAccessor{
		Parse: parse,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			if items.ExecDetail == nil {
				execDetail := seVars.StmtCtx.GetExecDetails()
				items.ExecDetail = &execDetail
			}
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			if items.ExecDetail == nil {
				return matchZero(threshold)
			}
			return match(items.ExecDetail, threshold)
		},
	}
}

func makeKVExecDetailAccessor(parse func(string) (any, error),
	match func(*util.ExecDetails, any) bool) SlowLogFieldAccessor {
	return SlowLogFieldAccessor{
		Parse: parse,
		Setter: func(ctx context.Context, _ *SessionVars, items *SlowQueryLogItems) {
			if items.KVExecDetail == nil {
				tikvExecDetailRaw := ctx.Value(util.ExecDetailsKey)
				if tikvExecDetailRaw != nil {
					items.KVExecDetail = tikvExecDetailRaw.(*util.ExecDetails)
				}
			}
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			if items.KVExecDetail == nil {
				return matchZero(threshold)
			}
			return match(items.KVExecDetail, threshold)
		},
	}
}

// numericComparable defines a set of numeric types that support ordering operations (like >=).
type numericComparable interface {
	~int | ~int64 | ~uint64 | ~float64
}

// MatchEqual compares a value `v` with a threshold and returns true if they are equal.
func MatchEqual[T comparable](threshold any, v T) bool {
	tv, ok := threshold.(T)
	return ok && v == tv
}

func matchGE[T numericComparable](threshold any, v T) bool {
	tv, ok := threshold.(T)
	return ok && v >= tv
}

func matchZero(threshold any) bool {
	switch v := threshold.(type) {
	case int:
		return v == 0
	case uint64:
		return v == 0
	case int64:
		return v == 0
	case float64:
		return v == 0
	default:
		return false
	}
}

// ParseString returns the input string as-is.
func ParseString(v string) (any, error) { return v, nil }
func parseInt64(v string) (any, error) {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, fmt.Errorf("threshold value must be non-negative, got %d", n)
	}
	return n, nil
}
func parseUint64(v string) (any, error) { return strconv.ParseUint(v, 10, 64) }
func parseFloat64(v string) (any, error) {
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return nil, err
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, fmt.Errorf("threshold value must be finite, got %v", f)
	}
	if f < 0 {
		return nil, fmt.Errorf("threshold value must be non-negative, got %v", f)
	}
	return f, nil
}
func parseBool(v string) (any, error) { return strconv.ParseBool(v) }

// SlowLogRuleFieldAccessors defines the set of field accessors for SlowQueryLogItems
// that are relevant to evaluating and triggering SlowLogRules.
// It's exporting for testing.
var SlowLogRuleFieldAccessors = map[string]SlowLogFieldAccessor{
	strings.ToLower(SlowLogConnIDStr): {
		Parse: parseUint64,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, seVars.ConnectionID)
		},
	},
	strings.ToLower(SlowLogSessAliasStr): {
		Parse: ParseString,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return MatchEqual(threshold, seVars.SessionAlias)
		},
	},
	strings.ToLower(SlowLogDBStr): {
		Parse: ParseString,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return MatchEqual(strings.ToLower(threshold.(string)), strings.ToLower(seVars.CurrentDB))
		},
	},
	strings.ToLower(SlowLogExecRetryCount): {
		Parse: parseUint64,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.ExecRetryCount = seVars.StmtCtx.ExecRetryCount
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, items.ExecRetryCount)
		},
	},
	strings.ToLower(SlowLogQueryTimeStr): {
		Parse: parseFloat64,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.TimeTotal = seVars.GetTotalCostDuration()
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, items.TimeTotal.Seconds())
		},
	},
	strings.ToLower(SlowLogParseTimeStr): {
		Parse: parseFloat64,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, seVars.DurationParse.Seconds())
		},
	},
	strings.ToLower(SlowLogCompileTimeStr): {
		Parse: parseFloat64,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, seVars.DurationCompile.Seconds())
		},
	},
	strings.ToLower(SlowLogRewriteTimeStr): {
		Parse: parseFloat64,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.RewriteInfo = seVars.RewritePhaseInfo
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, items.RewriteInfo.DurationRewrite.Seconds())
		},
	},
	strings.ToLower(SlowLogOptimizeTimeStr): {
		Parse: parseFloat64,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, seVars.DurationOptimizer.Total.Seconds())
		},
	},
	strings.ToLower(SlowLogWaitTSTimeStr): {
		Parse: parseFloat64,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, seVars.DurationWaitTS.Seconds())
		},
	},
	strings.ToLower(SlowLogIsInternalStr): {
		Parse: parseBool,
		Match: func(seVars *SessionVars, _ *SlowQueryLogItems, threshold any) bool {
			return MatchEqual(threshold, seVars.InRestrictedSQL)
		},
	},
	strings.ToLower(SlowLogDigestStr): {
		Parse: ParseString,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			_, digest := seVars.StmtCtx.SQLDigest()
			items.Digest = digest.String()
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return MatchEqual(threshold, items.Digest)
		},
	},
	strings.ToLower(SlowLogNumCopTasksStr): {
		Parse: parseInt64,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			copTasksDetail := seVars.StmtCtx.CopTasksDetails()
			items.CopTasks = copTasksDetail
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			if items.CopTasks == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, int64(items.CopTasks.NumCopTasks))
		},
	},
	strings.ToLower(SlowLogMemMax): {
		Parse: parseInt64,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.MemMax = seVars.MemTracker.MaxConsumed()
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, items.MemMax)
		},
	},
	strings.ToLower(SlowLogDiskMax): {
		Parse: parseInt64,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.DiskMax = seVars.DiskTracker.MaxConsumed()
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, items.DiskMax)
		},
	},
	strings.ToLower(SlowLogWriteSQLRespTotal): {
		Parse: parseFloat64,
		Setter: func(ctx context.Context, _ *SessionVars, items *SlowQueryLogItems) {
			stmtDetailRaw := ctx.Value(execdetails.StmtExecDetailKey)
			if stmtDetailRaw != nil {
				stmtDetail := *(stmtDetailRaw.(*execdetails.StmtExecDetails))
				items.WriteSQLRespTotal = stmtDetail.WriteSQLRespDuration
			}
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, items.WriteSQLRespTotal.Seconds())
		},
	},
	strings.ToLower(SlowLogSucc): {
		Parse: parseBool,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.Succ = seVars.StmtCtx.ExecSuccess
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return MatchEqual(threshold, items.Succ)
		},
	},
	strings.ToLower(SlowLogResourceGroup): {
		Parse: ParseString,
		Setter: func(_ context.Context, seVars *SessionVars, items *SlowQueryLogItems) {
			items.ResourceGroupName = seVars.StmtCtx.ResourceGroupName
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return MatchEqual(strings.ToLower(threshold.(string)), strings.ToLower(items.ResourceGroupName))
		},
	},
	// The following fields are related to util.ExecDetails.
	strings.ToLower(SlowLogKVTotal): makeKVExecDetailAccessor(
		parseFloat64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, time.Duration(d.WaitKVRespDuration).Seconds())
		},
	),
	strings.ToLower(SlowLogPDTotal): makeKVExecDetailAccessor(
		parseFloat64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, time.Duration(d.WaitPDRespDuration).Seconds())
		},
	),
	strings.ToLower(SlowLogUnpackedBytesSentTiKVTotal): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesSentKVTotal)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesReceivedTiKVTotal): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesReceivedKVTotal)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesSentTiKVCrossZone): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesSentKVCrossZone)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesReceivedTiKVCrossZone): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesReceivedKVCrossZone)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesSentTiFlashTotal): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesSentMPPTotal)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesReceivedTiFlashTotal): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesReceivedMPPTotal)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesSentTiFlashCrossZone): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesSentMPPCrossZone)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesReceivedTiFlashCrossZone): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.UnpackedBytesReceivedMPPCrossZone)
		},
	),
	// The following fields are related to execdetails.ExecDetails.
	strings.ToLower(execdetails.ProcessTimeStr): makeExecDetailAccessor(
		parseFloat64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.TimeDetail.ProcessTime.Seconds())
		}),
	strings.ToLower(execdetails.BackoffTimeStr): makeExecDetailAccessor(
		parseFloat64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			return matchGE(threshold, d.BackoffTime.Seconds())
		}),
	strings.ToLower(execdetails.TotalKeysStr): makeExecDetailAccessor(
		parseUint64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.ScanDetail == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, d.ScanDetail.TotalKeys)
		}),
	strings.ToLower(execdetails.ProcessKeysStr): makeExecDetailAccessor(
		parseUint64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.ScanDetail == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, d.ScanDetail.ProcessedKeys)
		}),
	strings.ToLower(SlowLogCopMVCCReadAmplification): makeExecDetailAccessor(
		parseFloat64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.ScanDetail == nil || d.ScanDetail.ProcessedKeys <= 0 {
				return matchZero(threshold)
			}
			return matchGE(threshold, float64(d.ScanDetail.TotalKeys)/float64(d.ScanDetail.ProcessedKeys))
		}),
	strings.ToLower(execdetails.PreWriteTimeStr): makeExecDetailAccessor(
		parseFloat64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.CommitDetail == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, d.CommitDetail.PrewriteTime.Seconds())
		}),
	strings.ToLower(execdetails.CommitTimeStr): makeExecDetailAccessor(
		parseFloat64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.CommitDetail == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, d.CommitDetail.CommitTime.Seconds())
		}),
	strings.ToLower(execdetails.WriteKeysStr): makeExecDetailAccessor(
		parseUint64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.CommitDetail == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, int64(d.CommitDetail.WriteKeys))
		}),
	strings.ToLower(execdetails.WriteSizeStr): makeExecDetailAccessor(
		parseUint64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.CommitDetail == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, int64(d.CommitDetail.WriteSize))
		}),
	strings.ToLower(execdetails.PrewriteRegionStr): makeExecDetailAccessor(
		parseUint64,
		func(d *execdetails.ExecDetails, threshold any) bool {
			if d.CommitDetail == nil {
				return matchZero(threshold)
			}
			return matchGE(threshold, int64(atomic.LoadInt32(&d.CommitDetail.PrewriteRegionNum)))
		}),
}
