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

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestSystimeMonitor(t *testing.T) {
	t.Parallel()

	errTriggered := atomic.NewBool(false)
	nowTriggered := atomic.NewBool(false)
	go StartMonitor(
		func() time.Time {
			if !nowTriggered.Load() {
				nowTriggered.Store(true)
				return time.Now()
			}

			return time.Now().Add(-2 * time.Second)
		}, func() {
			errTriggered.Store(true)
		}, func() {})

	require.Eventually(t, errTriggered.Load, time.Second, 10*time.Millisecond)
}
