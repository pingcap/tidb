// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"math"
	"testing"
	"testing/synctest"
	"time"

	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/dumpling/log"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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
	d.RefreshStatus()
	initial := d.GetStatus()

	AddCounter(d.metrics.finishedTablesCounter, 10)
	AddGauge(d.metrics.finishedSizeGauge, 20)
	AddGauge(d.metrics.finishedRowsGauge, 30)
	AddCounter(d.metrics.estimateTotalRowsCounter, 40)

	require.Equal(t, initial, d.GetStatus())
	d.RefreshStatus()
	mid = d.GetStatus()
	require.EqualValues(t, float64(10), mid.CompletedTables)
	require.EqualValues(t, float64(20), mid.FinishedBytes)
	require.EqualValues(t, float64(30), mid.FinishedRows)
	require.EqualValues(t, float64(40), mid.EstimateTotalRows)
	require.Zero(t, initial.FinishedBytes)
}

func TestGetStatusReturnsIndependentSnapshots(t *testing.T) {
	conf := defaultConfigForTest(t)
	d := &Dumper{conf: conf, speedRecorder: NewSpeedRecorder()}
	d.metrics = newMetrics(conf.PromFactory, nil)
	AddGauge(d.metrics.finishedSizeGauge, 500)
	d.metrics.totalChunks.Store(4)
	d.metrics.completedChunks.Store(1)
	d.metrics.progressReady.Store(true)
	d.RefreshStatus()

	first, second := d.GetStatus(), d.GetStatus()
	first.FinishedBytes = 999
	*first.ProgressPercent = 100
	for _, snapshot := range []*DumpStatus{second, d.GetStatus()} {
		require.EqualValues(t, 500, snapshot.FinishedBytes)
		require.EqualValues(t, 25, *snapshot.ProgressPercent)
	}
}

func TestStopLogProgressPublishesFinalStatus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conf := defaultConfigForTest(t)
		d := &Dumper{conf: conf, speedRecorder: NewSpeedRecorder()}
		d.metrics = newMetrics(conf.PromFactory, nil)
		d.metrics.totalChunks.Store(4)
		d.metrics.completedChunks.Store(1)
		d.metrics.progressReady.Store(true)
		stop := d.startLogProgress(tcontext.Background())
		defer stop()
		synctest.Wait()
		require.EqualValues(t, 25, *d.GetStatus().ProgressPercent)

		AddGauge(d.metrics.finishedSizeGauge, 500)
		d.metrics.completedChunks.Store(4)
		// Use the same stop function that Dump defers; callers must not need
		// an additional wait before reading the final snapshot.
		stop()
		final := d.GetStatus()
		require.EqualValues(t, 500, final.FinishedBytes)
		require.EqualValues(t, 100, *final.ProgressPercent)
	})
}

func TestRunLogProgressRefreshesStatus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		core, logs := observer.New(zap.InfoLevel)
		tctx, cancel := tcontext.Background().WithLogger(log.NewAppLogger(zap.New(core))).WithCancel()
		defer cancel()
		conf := defaultConfigForTest(t)
		d := &Dumper{tctx: tctx, conf: conf, speedRecorder: NewSpeedRecorder(), totalTables: 1}
		d.metrics = newMetrics(conf.PromFactory, nil)
		d.metrics.totalChunks.Store(4)
		d.metrics.progressReady.Store(true)
		done := make(chan struct{})
		go func() {
			defer close(done)
			d.runLogProgress(tctx)
		}()
		synctest.Wait()
		initial := d.GetStatus()
		require.NotNil(t, initial.ProgressPercent)

		AddGauge(d.metrics.finishedSizeGauge, 500)
		d.metrics.completedChunks.Store(1)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		snapshot := d.GetStatus()
		require.EqualValues(t, 500, snapshot.FinishedBytes)
		require.EqualValues(t, 100, snapshot.CurrentSpeedBPS)
		require.EqualValues(t, 25, *snapshot.ProgressPercent)
		require.Zero(t, initial.FinishedBytes)
		require.Zero(t, *initial.ProgressPercent)
		require.Zero(t, logs.Len())

		// A log tick must use live bytes for its interval average, even if it
		// runs before the simultaneous status refresh.
		time.Sleep(114 * time.Second)
		synctest.Wait()
		require.Zero(t, logs.Len())
		AddGauge(d.metrics.finishedSizeGauge, 1500)
		time.Sleep(time.Second)
		synctest.Wait()
		require.EqualValues(t, 2000, d.GetStatus().FinishedBytes)
		require.InDelta(t, 1500.0/115, d.GetStatus().CurrentSpeedBPS, 1e-9)
		entries := logs.FilterMessage("progress").All()
		require.Len(t, entries, 1)
		require.InDelta(t, 2000.0/120/1048576, entries[0].ContextMap()["average speed(MiB/s)"], 1e-12)

		AddGauge(d.metrics.finishedSizeGauge, 1000)
		d.metrics.completedChunks.Store(4)
		cancel()
		<-done
		final := d.GetStatus()
		require.EqualValues(t, 3000, final.FinishedBytes)
		require.EqualValues(t, 100, *final.ProgressPercent)
		AddGauge(d.metrics.finishedSizeGauge, 1000)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		require.Equal(t, final, d.GetStatus())
		require.Len(t, logs.FilterMessage("progress").All(), 1)
	})
}

func TestRunLogProgressFailpointRefreshesStatus(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/dumpling/export/EnableLogProgress", "return()")
	synctest.Test(t, func(t *testing.T) {
		core, logs := observer.New(zap.InfoLevel)
		tctx, cancel := tcontext.Background().WithLogger(log.NewAppLogger(zap.New(core))).WithCancel()
		defer cancel()
		conf := defaultConfigForTest(t)
		d := &Dumper{tctx: tctx, conf: conf, speedRecorder: NewSpeedRecorder(), totalTables: 1}
		d.metrics = newMetrics(conf.PromFactory, nil)
		done := make(chan struct{})
		go func() {
			defer close(done)
			d.runLogProgress(tctx)
		}()
		synctest.Wait()
		require.Empty(t, d.GetStatus().Progress)

		// Short integration dumps finish before the normal five-second refresh.
		d.metrics.totalChunks.Store(4)
		d.metrics.completedChunks.Store(1)
		d.metrics.progressReady.Store(true)
		time.Sleep(time.Second)
		synctest.Wait()
		cancel()
		<-done
		entries := logs.FilterMessage("progress").All()
		require.Len(t, entries, 1)
		require.Equal(t, "25.00 %", entries[0].ContextMap()["chunks progress"])
	})
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
