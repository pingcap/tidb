// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetParameters(t *testing.T) {
	conf := defaultConfigForTest(t)
	d := &Dumper{conf: conf}
	d.metrics = newMetrics(conf.PromFactory, nil)

	mid := d.GetParameters()
	require.EqualValues(t, float64(0), mid.CompletedTables)
	require.EqualValues(t, float64(0), mid.FinishedBytes)
	require.EqualValues(t, float64(0), mid.FinishedRows)
	require.EqualValues(t, float64(0), mid.EstimateTotalRows)

	AddCounter(d.metrics.finishedTablesCounter, 10)
	AddGauge(d.metrics.finishedSizeGauge, 20)
	AddGauge(d.metrics.finishedRowsGauge, 30)
	AddCounter(d.metrics.estimateTotalRowsCounter, 40)

	mid = d.GetParameters()
	require.EqualValues(t, float64(10), mid.CompletedTables)
	require.EqualValues(t, float64(20), mid.FinishedBytes)
	require.EqualValues(t, float64(30), mid.FinishedRows)
	require.EqualValues(t, float64(40), mid.EstimateTotalRows)
}
