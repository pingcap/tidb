// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestMetricsRegistration(t *testing.T) {
	m := newMetrics(promutil.NewDefaultFactory(), nil)
	registry := promutil.NewDefaultRegistry()
	m.registerTo(registry)
	defer m.unregisterFrom(registry)

	m.observePackedPhase(packedPhaseDecode, time.Now(), nil)
	observer, err := m.packedPhaseDurationHistogram.GetMetricWithLabelValues(packedPhaseDecode, packedResultSuccess)
	require.NoError(t, err)
	metric := &dto.Metric{}
	require.NoError(t, observer.(prometheus.Metric).Write(metric))
	require.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
}
