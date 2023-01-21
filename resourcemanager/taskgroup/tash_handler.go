// Copyright 2022 PingCAP, Inc.
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

package taskgroup

import "time"

type Handle struct {
	// cpuStart captures the running time of the calling goroutine when this
	// handle is constructed.
	cpuStart time.Duration
	// allotted captures how much on-CPU running time this CPU handle permits
	// before indicating that it's over limit. It measures the duration since
	// cpuStart.
	allotted time.Duration

	// This handle is used in tight loops that are sensitive to per-iteration
	// overhead (checking against the running time too can have an effect). To
	// reduce the overhead, the handle internally maintains an estimate for how
	// many iterations at the caller correspond to 1ms of running time. It uses
	// the following set of variables to do so.

	// The number of iterations since we last measured the running time, and how
	// many iterations until we need to do so again.
	itersSinceLastCheck, itersUntilCheck int
}
