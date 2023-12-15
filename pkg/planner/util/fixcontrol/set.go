// Copyright 2023 PingCAP, Inc.
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

package fixcontrol

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
)

// ParseToMap parses the user input to an optimizer fix control map.
func ParseToMap(s string) (result map[uint64]string, warnMsgs []string, err error) {
	m := make(map[uint64]string)
	for ; len(s) > 0; s = strings.TrimSpace(s) {
		// find the colon
		colonIdx := strings.IndexByte(s, ':')
		if colonIdx < 0 {
			err = errors.New("invalid fix control: expected colon not found")
			return nil, nil, err
		}
		// 1. the part before the colon is the fix control number and also the key in the map
		keyStr := strings.TrimSpace(s[0:colonIdx])
		key, err := strconv.ParseUint(keyStr, 10, 64)
		if err != nil {
			return nil, nil, err
		}
		// 2. the part after the colon is the value set for this fix
		s = strings.TrimSpace(s[colonIdx+1:])
		var value string
		// if it starts with quote, we need to find the closing quote, and the content between is the value for this fix
		if s[0] == '\'' || s[0] == '"' {
			quote := s[0]
			endIdx := strings.IndexByte(s[1:], quote)
			if endIdx < 0 {
				err = errors.New("invalid fix control: expected quote not found")
				return nil, nil, err
			}
			endIdx = endIdx + 1
			value = s[1:endIdx]
			s = s[endIdx+1:]
		}
		// if there's no comma, the remaining content belong to this fix
		endIdx := len(s)
		nextStartIdx := len(s)
		commaIdx := strings.IndexByte(s, ',')
		// if we find a comma, we need to locate the end for this fix and the start of the next fix
		if commaIdx >= 0 {
			endIdx = commaIdx
			nextStartIdx = commaIdx + 1
		}
		// if there's no quote, get the value for this fix now.
		if len(value) == 0 {
			value = strings.TrimSpace(s[:endIdx])
		}

		// 3. key and value are found, now handling repeated key and set into map
		originalValue, ok := m[key]
		if ok && originalValue != value {
			warnMsg := fmt.Sprintf("repeated assignment for fix control: %d. existing value: %q. new value: %q.",
				key,
				originalValue,
				value)
			warnMsgs = append(warnMsgs, warnMsg)
		}
		m[key] = value

		// 4. prepare to parse remaining content
		s = s[nextStartIdx:]
	}
	return m, warnMsgs, nil
}
