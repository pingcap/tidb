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

package parser

import (
	"fmt"
	"strconv"
	"strings"
)

const maxPasswordLockTimeValue = 32767

func parsePasswordLockTimeValue(value string) (int64, string, error) {
	if len(value) < 2 {
		return 0, "", fmt.Errorf("invalid PASSWORD_LOCK_TIME value %q", value)
	}

	unit := strings.ToLower(value[len(value)-1:])
	if unit != "m" && unit != "h" && unit != "d" {
		return 0, "", fmt.Errorf("invalid PASSWORD_LOCK_TIME unit %q", value[len(value)-1:])
	}
	count, err := strconv.ParseInt(value[:len(value)-1], 10, 64)
	if err != nil || count < 0 || count > maxPasswordLockTimeValue {
		return 0, "", fmt.Errorf("invalid PASSWORD_LOCK_TIME value %q", value)
	}
	return count, unit, nil
}
