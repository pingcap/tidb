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

package servicescope

import (
	"fmt"
	"regexp"
)

// CheckServiceScope check if the tidb-service-scope set by users is valid.
func CheckServiceScope(scope string) error {
	re := regexp.MustCompile(`^[a-zA-Z0-9_-]{0,64}$`)
	if !re.MatchString(scope) {
		return fmt.Errorf("the tidb-service-scope value '%s' is invalid. It must be 64 characters or fewer and consist only of letters (a-z, A-Z), numbers (0-9), hyphens (-), and underscores (_)", scope)
	}
	return nil
}
