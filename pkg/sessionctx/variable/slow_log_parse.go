// Copyright 2018 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"hash/crc64"
	"regexp"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/slowlogrule"
)

func writeSlowLogItem(buf *bytes.Buffer, key, value string) {
	buf.WriteString(SlowLogRowPrefixStr + key + SlowLogSpaceMarkStr + value + "\n")
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
