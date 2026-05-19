// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestRetLabel(t *testing.T) {
	require.Equal(t, opSucc, RetLabel(nil))
	require.Equal(t, opFailed, RetLabel(errors.New("test error")))
}

func TestGrpcChannelzCollectorSingleton(t *testing.T) {
	cleanupGrpcChannelzCollectorForTest()
	t.Cleanup(cleanupGrpcChannelzCollectorForTest)

	func() {
		grpcChannelzCollector.mu.Lock()
		defer grpcChannelzCollector.mu.Unlock()

		require.NoError(t, initGrpcChannelzCollectorLocked())
		firstServer := grpcChannelzCollector.server
		firstListener := grpcChannelzCollector.listener
		firstConn := grpcChannelzCollector.conn
		firstCollector := grpcChannelzCollector.collector

		require.NoError(t, initGrpcChannelzCollectorLocked())
		require.Same(t, firstServer, grpcChannelzCollector.server)
		require.Same(t, firstListener, grpcChannelzCollector.listener)
		require.Same(t, firstConn, grpcChannelzCollector.conn)
		require.True(t, firstCollector == grpcChannelzCollector.collector)
	}()

	cleanupGrpcChannelzCollectorForTest()

	func() {
		grpcChannelzCollector.mu.Lock()
		defer grpcChannelzCollector.mu.Unlock()

		require.Nil(t, grpcChannelzCollector.server)
		require.Nil(t, grpcChannelzCollector.listener)
		require.Nil(t, grpcChannelzCollector.conn)
		require.Nil(t, grpcChannelzCollector.collector)
		require.False(t, grpcChannelzCollector.registered)
	}()
}

func TestSetupChannelzCollectorSkippedInTest(t *testing.T) {
	cleanupGrpcChannelzCollectorForTest()
	t.Cleanup(cleanupGrpcChannelzCollectorForTest)
	require.True(t, intest.InTest)

	setupChannelzCollector()

	func() {
		grpcChannelzCollector.mu.Lock()
		defer grpcChannelzCollector.mu.Unlock()

		require.Nil(t, grpcChannelzCollector.collector)
		require.False(t, grpcChannelzCollector.registered)
	}()
}

func TestGrpcChannelzCollectorGather(t *testing.T) {
	cleanupGrpcChannelzCollectorForTest()
	t.Cleanup(cleanupGrpcChannelzCollectorForTest)

	var collector prometheus.Collector
	func() {
		grpcChannelzCollector.mu.Lock()
		defer grpcChannelzCollector.mu.Unlock()

		require.NoError(t, initGrpcChannelzCollectorLocked())
		collector = grpcChannelzCollector.collector
	}()

	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(collector))
	families, err := registry.Gather()
	require.NoError(t, err)

	require.NotNil(t, findMetricFamily(families, "tidb_grpc_channelz_fetch_errors_total"))
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			require.False(t, metricHasLabelValue(metric, "target", "bufnet"))
			require.False(t, metricHasLabelValue(metric, "target", "passthrough:///bufnet"))
			if strings.HasPrefix(family.GetName(), "tidb_grpc_channelz_socket_") {
				require.False(t, metricHasLabelValue(metric, "remote", ""))
			}
		}
	}
}

func findMetricFamily(families []*dto.MetricFamily, name string) *dto.MetricFamily {
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	return nil
}

func metricHasLabelValue(metric *dto.Metric, name string, value string) bool {
	for _, label := range metric.GetLabel() {
		if label.GetName() == name && label.GetValue() == value {
			return true
		}
	}
	return false
}
