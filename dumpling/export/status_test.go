// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetParameters(t *testing.T) {
	conf := defaultConfigForTest(t)
	d := &Dumper{conf: conf, speedRecorder: NewSpeedRecorder()}
	d.metrics = newMetrics(conf.PromFactory, nil)

	mid := d.GetStatus()
	require.EqualValues(t, float64(0), mid.CompletedTables)
	require.EqualValues(t, float64(0), mid.FinishedBytes)
	require.EqualValues(t, float64(0), mid.FinishedRows)
	require.EqualValues(t, float64(0), mid.EstimateTotalRows)

	AddCounter(d.metrics.finishedTablesCounter, 10)
	AddGauge(d.metrics.finishedSizeGauge, 20)
	AddGauge(d.metrics.finishedRowsGauge, 30)
	AddCounter(d.metrics.estimateTotalRowsCounter, 40)

	mid = d.GetStatus()
	require.EqualValues(t, float64(10), mid.CompletedTables)
	require.EqualValues(t, float64(20), mid.FinishedBytes)
	require.EqualValues(t, float64(30), mid.FinishedRows)
	require.EqualValues(t, float64(40), mid.EstimateTotalRows)
}

func TestSpeedRecorder(t *testing.T) {
	testCases := []struct {
		spentTime int64
		finished  float64
		expected  float64
	}{
		{spentTime: 1, finished: 100, expected: 100},
		{spentTime: 2, finished: 200, expected: 50},
		// already finished, will return last speed
		{spentTime: 3, finished: 200, expected: 50},
	}
	speedRecorder := NewSpeedRecorder()
	for _, tc := range testCases {
		time.Sleep(time.Duration(tc.spentTime) * time.Second)
		recentSpeed := speedRecorder.GetSpeed(tc.finished)
		if math.Abs(tc.expected-recentSpeed)/tc.expected > 0.1 {
			require.FailNow(t, "speed is unexpected", "expected: %5.2f, recent: %5.2f", tc.expected, recentSpeed)
		}
	}
}
