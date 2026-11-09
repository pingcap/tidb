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

package privileges

import (
	"fmt"
	"math"
)

const (
	passwordLockSecsPerMinute = int64(60)
	passwordLockSecsPerHour   = 60 * passwordLockSecsPerMinute
	passwordLockSecsPerDay    = 24 * passwordLockSecsPerHour
	passwordLockMaxSecs       = int64(32767) * passwordLockSecsPerDay
)

// PasswordLockTimeToSeconds converts the SQL value to seconds for storage and authentication.
func PasswordLockTimeToSeconds(count int64, unit string) (int64, error) {
	if count < 0 || count > math.MaxInt16 {
		return 0, fmt.Errorf("invalid PASSWORD_LOCK_TIME value %d", count)
	}
	multiplier := passwordLockSecsPerDay
	switch unit {
	case "", "d":
		multiplier = passwordLockSecsPerDay
	case "h":
		multiplier = passwordLockSecsPerHour
	case "m":
		multiplier = passwordLockSecsPerMinute
	default:
		return 0, fmt.Errorf("invalid PASSWORD_LOCK_TIME unit %q", unit)
	}
	return count * multiplier, nil
}

// FormatPasswordLockTime formats seconds using the supported SQL units, rounding up to minutes.
func FormatPasswordLockTime(seconds int64) string {
	minutes := (seconds + passwordLockSecsPerMinute - 1) / passwordLockSecsPerMinute
	switch {
	case minutes%(24*60) == 0:
		return fmt.Sprintf("%dd", minutes/(24*60))
	case minutes%60 == 0:
		return fmt.Sprintf("%dh", minutes/60)
	default:
		return fmt.Sprintf("%dm", minutes)
	}
}

// PasswordLockTimeDaysToSeconds converts the legacy persisted day value safely.
func PasswordLockTimeDaysToSeconds(days int64) int64 {
	if days == -1 {
		return -1
	}
	if days < -1 {
		return -1
	}
	if days > math.MaxInt16 {
		days = math.MaxInt16
	}
	return days * passwordLockSecsPerDay
}

// PasswordLockTimeSecondsToDays truncates seconds to legacy whole days, preserving UNBOUNDED.
func PasswordLockTimeSecondsToDays(seconds int64) int64 {
	if seconds == -1 {
		return -1
	}
	return seconds / passwordLockSecsPerDay
}

func clampPasswordLockSecs(seconds int64) int64 {
	if seconds < -1 {
		return -1
	}
	if seconds > passwordLockMaxSecs {
		return passwordLockMaxSecs
	}
	return seconds
}
