// Copyright 2022 PingCAP, Inc.
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
package grunning

import "time"

// Time returns the time spent by the current goroutine in the running state.
func Time() time.Duration {
	return time.Duration(grunningnanos())
}

// Difference is a helper function to compute the absolute difference between
// two durations. It's commonly used to measure how much running time is spent
// doing some piece of work.
func Difference(a, b time.Duration) time.Duration {
	diff := a.Nanoseconds() - b.Nanoseconds()
	if diff < 0 {
		diff = -diff
	}
	return time.Duration(diff)
}

// Supported returns true iff per-goroutine running time is available in this
// build. We use a patched Go runtime for all platforms officially supported for
// CRDB when built using Bazel. Engineers commonly building CRDB also use happen
// to use two platforms we don't use a patched Go for:
// - FreeBSD (we don't have cross-compilers setup), and
// - M1/M2 Macs (we don't have a code-signing pipeline, yet).
// We use '(darwin && arm64) || freebsd || !bazel' as the build tag to exclude
// unsupported platforms.
func Supported() bool {
	return supported()
}
