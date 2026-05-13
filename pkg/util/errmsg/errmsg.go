// Copyright 2026 PingCAP, Inc.
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

package errmsg

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

var regexpCache sync.Map

// ExtendErrorMessage appends a configured message suffix to selected SQL errors.
func ExtendErrorMessage(m *mysql.SQLError) {
	if m == nil {
		return
	}

	extendedMsgs := config.GetGlobalConfig().ExtendedErrorMsgs
	if len(extendedMsgs) == 0 {
		return
	}

	patterns := make([]string, 0, len(extendedMsgs))
	for pattern := range extendedMsgs {
		patterns = append(patterns, pattern)
	}
	sort.Slice(patterns, func(i, j int) bool {
		if len(patterns[i]) != len(patterns[j]) {
			return len(patterns[i]) > len(patterns[j])
		}
		return patterns[i] < patterns[j]
	})

	for _, pattern := range patterns {
		extendedMsg := extendedMsgs[pattern]
		if extendedMsg == "" {
			continue
		}
		re, err := compileRegexp(pattern)
		if err != nil {
			continue
		}
		if re.MatchString(m.Message) {
			extendErrorMessage(m, extendedMsg)
			return
		}
	}
}

func compileRegexp(pattern string) (*regexp.Regexp, error) {
	cachedRegexp, ok := regexpCache.Load(pattern)
	if ok {
		return cachedRegexp.(*regexp.Regexp), nil
	}

	compiledRegexp, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	actual, _ := regexpCache.LoadOrStore(pattern, compiledRegexp)
	return actual.(*regexp.Regexp), nil
}

func extendErrorMessage(m *mysql.SQLError, msg string) {
	m.Message = fmt.Sprintf("%s, %s.", strings.TrimSuffix(m.Message, "."), strings.TrimSuffix(msg, "."))
}
