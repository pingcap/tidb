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

package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPhaseTracer(t *testing.T) {
	tm := time.Now()
	getTime := func() time.Time {
		return tm
	}

	lastReportStatus := ""
	lastReportDuration := time.Duration(0)
	resetReport := func() {
		lastReportStatus = ""
		lastReportDuration = time.Duration(0)
	}

	tracer := newPhaseTracer(getTime, func(status string, duration time.Duration) {
		require.Equal(t, "", lastReportStatus)
		require.Equal(t, int64(0), lastReportDuration.Nanoseconds())
		lastReportStatus = status
		lastReportDuration = duration
	})

	resetReport()
	tm = tm.Add(time.Second * 2)
	tracer.EnterPhase("p1")
	require.Equal(t, "", lastReportStatus)
	require.Equal(t, int64(0), lastReportDuration.Nanoseconds())
	require.Equal(t, "p1", tracer.Phase())

	tm = tm.Add(time.Second * 5)
	tracer.EnterPhase("p2")
	require.Equal(t, "p1", lastReportStatus)
	require.Equal(t, time.Second*5, lastReportDuration)
	require.Equal(t, "p2", tracer.Phase())

	resetReport()
	tm = tm.Add(time.Second * 10)
	tracer.EnterPhase("p2")
	require.Equal(t, "p2", lastReportStatus)
	require.Equal(t, time.Second*10, lastReportDuration)
	require.Equal(t, "p2", tracer.Phase())

	resetReport()
	tm = tm.Add(time.Second * 20)
	tracer.EndPhase()
	require.Equal(t, "p2", lastReportStatus)
	require.Equal(t, time.Second*20, lastReportDuration)
	require.Equal(t, "", tracer.Phase())
}
