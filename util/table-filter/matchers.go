// Copyright 2021 PingCAP, Inc.
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

package filter

import (
	"regexp"
	"strings"

	"github.com/pingcap/errors"
)

// tableRule of a tableFilter, consisting of a schema and table pattern, and may be an
// accept-list (positive) or deny-list (negative).
type tableRule struct {
	schema   matcher
	table    matcher
	positive bool
}

// columnRule of a columnFilter, consisting of a column pattern, and may be an
// accept-list (positive) or deny-list (negative).
type columnRule struct {
	column   matcher
	positive bool
}

// matcher matches a name against a pattern.
type matcher interface {
	matchString(name string) bool
	matchAllStrings() bool
	toLower() matcher
}

// stringMatcher is a matcher with a literal string.
type stringMatcher string

func (m stringMatcher) matchString(name string) bool {
	return string(m) == name
}

func (stringMatcher) matchAllStrings() bool {
	return false
}

func (m stringMatcher) toLower() matcher {
	return stringMatcher(strings.ToLower(string(m)))
}

// trueMatcher is a matcher which matches everything. The `*` pattern.
type trueMatcher struct{}

func (trueMatcher) matchString(string) bool {
	return true
}

func (trueMatcher) matchAllStrings() bool {
	return true
}

func (m trueMatcher) toLower() matcher {
	return m
}

// regexpMatcher is a matcher based on a regular expression.
type regexpMatcher struct {
	pattern *regexp.Regexp
}

func newRegexpMatcher(pat string) (matcher, error) {
	if pat == "(?s)^.*$" {
		// special case for '*'
		return trueMatcher{}, nil
	}
	pattern, err := regexp.Compile(pat)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return regexpMatcher{pattern: pattern}, nil
}

func (m regexpMatcher) matchString(name string) bool {
	return m.pattern.MatchString(name)
}

func (regexpMatcher) matchAllStrings() bool {
	return false
}

func (m regexpMatcher) toLower() matcher {
	pattern := regexp.MustCompile("(?i)" + m.pattern.String())
	return regexpMatcher{pattern: pattern}
}
