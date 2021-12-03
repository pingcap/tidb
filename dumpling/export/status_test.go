// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetParameters(t *testing.T) {
	conf := defaultConfigForTest(t)
	d := &Dumper{conf: conf}
	InitMetricsVector(conf.Labels)

	mid := d.GetParameters()
	require.EqualValues(t, float64(0), mid.CompletedTables)
	require.EqualValues(t, float64(0), mid.FinishedBytes)
	require.EqualValues(t, float64(0), mid.FinishedRows)
	require.EqualValues(t, float64(0), mid.EstimateTotalRows)

	AddCounter(finishedTablesCounter, conf.Labels, 10)
	AddGauge(finishedSizeGauge, conf.Labels, 20)
	AddGauge(finishedRowsGauge, conf.Labels, 30)
	AddCounter(estimateTotalRowsCounter, conf.Labels, 40)

	mid = d.GetParameters()
	require.EqualValues(t, float64(10), mid.CompletedTables)
	require.EqualValues(t, float64(20), mid.FinishedBytes)
	require.EqualValues(t, float64(30), mid.FinishedRows)
	require.EqualValues(t, float64(40), mid.EstimateTotalRows)
}
