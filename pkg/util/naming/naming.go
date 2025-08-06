// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package naming

import (
	"fmt"
	"regexp"
)

// Check if the name is valid.
// Valid name must be 64 characters or fewer and consist only of letters (a-z, A-Z),
// numbers (0-9), hyphens (-), and underscores (_).
// currently, we enforce this rule to tidb_service_scope and keyspace_name
func Check(name string) error {
	return CheckWithMaxLen(name, 64)
}

// CheckWithMaxLen checks if the name is valid with the specified maximum length.
func CheckWithMaxLen(name string, maxLen int) error {
	namePattern := fmt.Sprintf(`^[a-zA-Z0-9_-]{0,%d}$`, maxLen)
	nameRe := regexp.MustCompile(namePattern)
	if !nameRe.MatchString(name) {
		return fmt.Errorf("the value '%s' is invalid. It must be %d characters or fewer and consist only of letters (a-z, A-Z), numbers (0-9), hyphens (-), and underscores (_)", name, maxLen)
	}
	return nil
}
