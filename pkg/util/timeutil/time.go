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

package timeutil

import (
	"context"
	"time"
)

// Sleep is a function like time.Sleep(),
// the difference is it will return early when ctx finished.
func Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IsUniqueLocalTime reports whether t's local wall-clock reading occurs only once
// in t.Location(). Callers parsing civil input must also check that it round-trips:
// time.Date and time.ParseInLocation can normalize nonexistent local times.
func IsUniqueLocalTime(t time.Time) bool {
	_, offset := t.Zone()
	start, end := t.ZoneBounds()
	if !start.IsZero() && repeatsInAdjacentOffset(t, offset, start.Add(-time.Nanosecond)) {
		return false
	}
	if !end.IsZero() && repeatsInAdjacentOffset(t, offset, end) {
		return false
	}
	return true
}

// repeatsInAdjacentOffset reports whether t's wall-clock reading also occurs
// with the UTC offset in effect at adjacent.
func repeatsInAdjacentOffset(t time.Time, offset int, adjacent time.Time) bool {
	_, adjacentOffset := adjacent.Zone()
	if adjacentOffset == offset {
		return false
	}
	// Shifting by the offset difference preserves the wall-clock reading
	// exactly when the candidate instant uses the adjacent offset.
	_, candidateOffset := t.Add(time.Duration(offset-adjacentOffset) * time.Second).Zone()
	return candidateOffset == adjacentOffset
}
