// Copyright 2025 PingCAP, Inc.
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
	"hash/crc64"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/slowlogrule"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/tikv/client-go/v2/util"
)

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
			items.ExecDetail = seVars.StmtCtx.GetExecDetails()
		},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return match(&items.ExecDetail, threshold)
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

// ParseString converts the input string to lowercase and returns it.
func ParseString(v string) (any, error)  { return v, nil }
func parseInt64(v string) (any, error)   { return strconv.ParseInt(v, 10, 64) }
func parseUint64(v string) (any, error)  { return strconv.ParseUint(v, 10, 64) }
func parseFloat64(v string) (any, error) { return strconv.ParseFloat(v, 64) }
func parseBool(v string) (any, error)    { return strconv.ParseBool(v) }

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
		Parse:  parseUint64,
		Setter: func(_ context.Context, _ *SessionVars, _ *SlowQueryLogItems) {},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, uint64(items.ExecRetryCount))
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
			return matchGE(threshold, seVars.DurationOptimization.Seconds())
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
	strings.ToLower(SlowLogMemArbitration): {
		Parse:  parseFloat64,
		Setter: func(_ context.Context, _ *SessionVars, _ *SlowQueryLogItems) {},
		Match: func(_ *SessionVars, items *SlowQueryLogItems, threshold any) bool {
			return matchGE(threshold, items.MemArbitration)
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
		Parse:  parseBool,
		Setter: func(_ context.Context, _ *SessionVars, _ *SlowQueryLogItems) {},
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
			return matchZero(threshold)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesReceivedTiKVTotal): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchZero(threshold)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesSentTiKVCrossZone): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchZero(threshold)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesReceivedTiKVCrossZone): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchZero(threshold)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesSentTiFlashTotal): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchZero(threshold)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesReceivedTiFlashTotal): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchZero(threshold)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesSentTiFlashCrossZone): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchZero(threshold)
		},
	),
	strings.ToLower(SlowLogUnpackedBytesReceivedTiFlashCrossZone): makeKVExecDetailAccessor(
		parseInt64,
		func(d *util.ExecDetails, threshold any) bool {
			return matchZero(threshold)
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

// slowLogFieldRe is uses to compile field:value
var slowLogFieldRe = regexp.MustCompile(`\s*(\w+)\s*:\s*([^,]+)\s*`)

// UnsetConnID is a sentinel value (-1) for slow log rules without an explicit connection binding.
//
// Semantics:
//   - Session scope: represents the current session.
//   - Global scope: means no specific connection ID is set, i.e. the rule applies globally.
const UnsetConnID = int64(-1)

// ParseSlowLogFieldValue is exporting for testing.
func ParseSlowLogFieldValue(fieldName string, value string) (any, error) {
	parser, ok := SlowLogRuleFieldAccessors[strings.ToLower(fieldName)]
	if !ok {
		return nil, errors.Errorf("unknown slow log field name:%s", fieldName)
	}

	return parser.Parse(value)
}

func extractMsgFromSQLWarn(sqlWarn *contextutil.SQLWarn) string {
	// CollectWarningsForSlowLog guarantees sqlWarn is non-nil.
	warn := errors.Cause(sqlWarn.Err)
	if x, ok := warn.(*terror.Error); ok && x != nil {
		sqlErr := terror.ToSQLError(x)
		return sqlErr.Message
	}
	return warn.Error()
}

// CollectWarningsForSlowLog collects warnings from statement context and formats them for slow log output.
func CollectWarningsForSlowLog(stmtCtx *stmtctx.StatementContext) []JSONSQLWarnForSlowLog {
	warnings := stmtCtx.GetWarnings()
	extraWarnings := stmtCtx.GetExtraWarnings()
	res := make([]JSONSQLWarnForSlowLog, len(warnings)+len(extraWarnings))
	for i := range warnings {
		res[i].Level = warnings[i].Level
		res[i].Message = extractMsgFromSQLWarn(&warnings[i])
	}
	for i := range extraWarnings {
		idx := len(warnings) + i
		res[idx].Level = extraWarnings[i].Level
		res[idx].Message = extractMsgFromSQLWarn(&extraWarnings[i])
		res[idx].IsExtra = true
	}
	return res
}

func parseSlowLogRuleEntry(rawRule string, allowConnID bool) (int64, *slowlogrule.SlowLogRule, error) {
	connID := UnsetConnID
	rawRule = strings.TrimSpace(rawRule)
	if rawRule == "" {
		return connID, nil, nil
	}

	matches := slowLogFieldRe.FindAllStringSubmatch(rawRule, -1)
	if len(matches) == 0 {
		return connID, nil, fmt.Errorf("invalid slow log rule format:%s", rawRule)
	}
	fieldMap := make(map[string]any, len(matches))
	for _, match := range matches {
		if len(match) != 3 {
			return connID, nil, errors.Errorf("invalid slow log condition format:%s", match)
		}

		fieldName := strings.ToLower(strings.TrimSpace(match[1]))
		value := strings.TrimSpace(match[2])
		fieldValue, err := ParseSlowLogFieldValue(fieldName, strings.Trim(value, "\"'"))
		if err != nil {
			return connID, nil, errors.Errorf("invalid slow log format, value:%s, err:%s", value, err)
		}

		if strings.EqualFold(fieldName, SlowLogConnIDStr) {
			if !allowConnID {
				return connID, nil, errors.Errorf("do not allow ConnID value:%s", value)
			}

			connID = int64(fieldValue.(uint64))
		}

		fieldMap[fieldName] = fieldValue
	}

	slowLogRule := &slowlogrule.SlowLogRule{Conditions: make([]slowlogrule.SlowLogCondition, 0, len(fieldMap))}
	for fieldName, fieldValue := range fieldMap {
		slowLogRule.Conditions = append(slowLogRule.Conditions, slowlogrule.SlowLogCondition{
			Field:     fieldName,
			Threshold: fieldValue,
		})
	}

	return connID, slowLogRule, nil
}

// parseSlowLogRuleSet parses a raw slow log rules string into a map keyed by ConnID.
// Input format:
//   - Multiple rules are separated by semicolons (';').
//   - Inside each rule, fields are expressed as key:value pairs, separated by commas (',').
//   - Example: "field1:val1,field2:val2;field3:val3"
//
// Behavior:
//   - Returns a map where the key is ConnID, and the value is a set of rules for that ConnID.
//   - UnsetConnID (-1) is used for rules not bound to a specific connection.
//   - If allowConnID is false, rules containing an explicit ConnID will be rejected.
func parseSlowLogRuleSet(rawRules string, allowConnID bool) (map[int64]*slowlogrule.SlowLogRules, error) {
	rawRules = strings.TrimSpace(rawRules)
	if rawRules == "" {
		return nil, nil
	}
	rules := strings.Split(rawRules, ";")
	if len(rules) > 10 {
		return nil, errors.Errorf("invalid slow log rules count:%d, limit is 10", len(rules))
	}

	result := make(map[int64]*slowlogrule.SlowLogRules)
	for _, raw := range rules {
		connID, slowLogRule, err := parseSlowLogRuleEntry(raw, allowConnID)
		if err != nil {
			return nil, err
		}
		if slowLogRule == nil {
			continue
		}

		slowLogRules, ok := result[connID]
		if !ok {
			slowLogRules = &slowlogrule.SlowLogRules{
				Fields: make(map[string]struct{}),
				Rules:  make([]*slowlogrule.SlowLogRule, 0, len(rules)),
			}
			result[connID] = slowLogRules
		}
		for _, cond := range slowLogRule.Conditions {
			slowLogRules.Fields[cond.Field] = struct{}{}
		}
		slowLogRules.Rules = append(slowLogRules.Rules, slowLogRule)
	}
	return result, nil
}

// ParseSessionSlowLogRules parses raw rules into the default (UnsetConnID) slow log rules.
// Returns nil if no rules for UnsetConnID are found.
func ParseSessionSlowLogRules(rawRules string) (*slowlogrule.SlowLogRules, error) {
	globalRules, err := parseSlowLogRuleSet(rawRules, false)
	if err != nil {
		return nil, err
	}
	if globalRules == nil || globalRules[UnsetConnID] == nil {
		return nil, nil
	}

	globalRules[UnsetConnID].RawRules = encodeRules(globalRules[UnsetConnID])

	return globalRules[UnsetConnID], nil
}

func encodeRules(rules *slowlogrule.SlowLogRules) string {
	if rules == nil || len(rules.Rules) == 0 {
		return ""
	}

	var strB strings.Builder
	for i, rule := range rules.Rules {
		for j, cond := range rule.Conditions {
			if j > 0 {
				strB.WriteByte(',')
			}
			strB.WriteString(cond.Field)
			strB.WriteByte(':')
			strB.WriteString(fmt.Sprintf("%v", cond.Threshold))
		}

		if i < len(rules.Rules)-1 {
			strB.WriteByte(';')
		}
	}

	return strB.String()
}

var crc64Table = crc64.MakeTable(crc64.ECMA)

// ParseGlobalSlowLogRules parses raw rules and constructs a GlobalSlowLogRules object.
// The result contains both the raw string and the rules map keyed by ConnID.
// allowConnID = true is used here to support both ConnID-bound and default rules.
func ParseGlobalSlowLogRules(rawRules string) (*slowlogrule.GlobalSlowLogRules, error) {
	rulesMap, err := parseSlowLogRuleSet(rawRules, true)
	if err != nil {
		return nil, err
	}

	if rulesMap == nil {
		rulesMap = make(map[int64]*slowlogrule.SlowLogRules)
	}

	rawSlice := make([]string, 0, len(rulesMap))
	for _, rules := range rulesMap {
		rawSlice = append(rawSlice, encodeRules(rules))
	}

	rawRules = strings.Join(rawSlice, ";")
	return &slowlogrule.GlobalSlowLogRules{
		RawRules:     rawRules,
		RawRulesHash: crc64.Checksum([]byte(rawRules), crc64Table),
		RulesMap:     rulesMap,
	}, nil
}
