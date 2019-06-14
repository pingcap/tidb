// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package backoff

// Maximum total sleep time(in ms) for kv/cop commands.
const (
	WaitScatterRegionFinishBackoff = 120000
)

// Time is used for session to set back off time(in ms).
type Time struct {
	waitScatterRegionFinish int
}

// GetWaitScatterRegionFinishBackoff gets the back off time of waitScatterRegionFinish.
func (b *Time) GetWaitScatterRegionFinishBackoff() int {
	if b.waitScatterRegionFinish > 0 {
		return b.waitScatterRegionFinish
	}
	return WaitScatterRegionFinishBackoff
}

// SetWaitScatterRegionFinishBackoff sets the back off time of waitScatterRegionFinish.
func (b *Time) SetWaitScatterRegionFinishBackoff(t int) {
	if t > 0 {
		b.waitScatterRegionFinish = t
	}
}
