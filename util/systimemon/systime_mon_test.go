// Copyright 2017 PingCAP, Inc.
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

package systimemon

import (
	"testing"
	"time"
)

func TestSystimeMonitor(t *testing.T) {
	jumpForward := false
	trigged := false
	go StartMonitor(
		func() time.Time {
			if !trigged {
				trigged = true
				return time.Now()
			}

			return time.Now().Add(-2 * time.Second)
		}, func() {
			jumpForward = true
		})

	time.Sleep(1 * time.Second)

	if !jumpForward {
		t.Error("should detect time error")
	}
}
