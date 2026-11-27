// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/promutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestMetricsRegistration(t *testing.T) {
	m := newMetrics(promutil.NewDefaultFactory(), nil)
	registry := promutil.NewDefaultRegistry()
	m.registerTo(registry)
	defer m.unregisterFrom(registry)

	m.finishedSizeGauge.WithLabelValues().Set(1)
	metric := &dto.Metric{}
	require.NoError(t, m.finishedSizeGauge.WithLabelValues().Write(metric))
	require.Equal(t, float64(1), metric.GetGauge().GetValue())
}
