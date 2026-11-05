// Copyright 2026 PingCAP, Inc.
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

package metricsutil

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestRegisterMetricsWithKeyspaceObservabilityValues(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	gatherer := prometheus.DefaultGatherer
	t.Cleanup(func() {
		prometheus.DefaultGatherer = gatherer
		metricscommon.SetConstLabels()
	})

	labels := cloneConstLabels()
	labels["label_a"] = "value_a"
	require.Equal(t, "value_a", labels["label_a"])

	metricscommon.SetConstLabels("base_label", "base_value")
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceObservabilityValues = config.KeyspaceObservabilityValues{
			MetricLabels: map[string]string{"label_a": "value_a"},
		}
	})

	require.NoError(t, registerMetrics())
	labels = metricscommon.GetConstLabels()
	require.Equal(t, "base_value", labels["base_label"])
	require.Equal(t, "value_a", labels["label_a"])

	metricscommon.SetConstLabels("keyspace_name", "ks")
	setKeyspaceIDConstLabel(42)
	labels = metricscommon.GetConstLabels()
	require.Equal(t, "ks", labels["keyspace_name"])
	require.Equal(t, "42", labels["keyspace_id"])
}

func TestRegisterMetricsKeyspaceID(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("all-metric keyspace labeling is only enabled for Starter")
	}
	oldMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() { require.NoError(t, deploymode.Set(oldMode)) })
	id, zeroID := uint32(42), uint32(0)

	for _, tc := range []struct {
		name, keyspaceName, expectedID string
		storageID                      *uint32
	}{
		{name: "no keyspace"},
		{name: "TiDB before storage initialization", keyspaceName: "ks"},
		{name: "TiDB zero storage keyspace ID", keyspaceName: "ks", expectedID: "0", storageID: &zeroID},
		{name: "TiDB storage keyspace", keyspaceName: "ks", expectedID: "42", storageID: &id},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer config.RestoreFunc()()
			oldRegisterer, oldGatherer := prometheus.DefaultRegisterer, prometheus.DefaultGatherer
			oldLabels := cloneConstLabels()
			t.Cleanup(func() {
				prometheus.DefaultRegisterer, prometheus.DefaultGatherer = oldRegisterer, oldGatherer
				setConstLabels(oldLabels)
			})
			registry := prometheus.NewRegistry()
			prometheus.DefaultRegisterer, prometheus.DefaultGatherer = registry, registry
			registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
			metricscommon.SetConstLabels()
			config.UpdateGlobal(func(conf *config.Config) {
				conf.Store = config.StoreTypeTiKV
				// Metric registration must not parse the PD path or dial PD.
				conf.Path = "unused?disableGC=invalid"
				conf.KeyspaceName = tc.keyspaceName
				conf.KeyspaceObservabilityValues = config.KeyspaceObservabilityValues{
					MetricLabels: map[string]string{"keyspace_meta_tenant": "tenant"},
				}
			})
			before := prometheus.NewGauge(prometheus.GaugeOpts{Name: "registered_before_init"})
			before.Set(7)
			registry.MustRegister(before)
			require.NoError(t, RegisterMetrics())
			// Storage can resolve the ID after the metrics handler has been created.
			handler := promhttp.Handler()
			require.NotContains(t, metricscommon.GetConstLabels(), keyspaceIDLabel)
			initialResponse := httptest.NewRecorder()
			handler.ServeHTTP(initialResponse, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			require.Equal(t, http.StatusOK, initialResponse.Code, initialResponse.Body.String())
			require.Contains(t, initialResponse.Body.String(), `registered_before_init{keyspace_id=""} 7`)
			if tc.storageID != nil {
				SetKeyspaceID(*tc.storageID)
			}
			require.Equal(t, "tenant", metricscommon.GetConstLabels()["keyspace_meta_tenant"])

			// Late registrations and metrics already labeled for another keyspace must work too.
			counter := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "registered_after_init_total"}, []string{"a", "z"})
			counter.WithLabelValues("first", "last").Add(3)
			histogram := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "late_histogram"})
			histogram.Observe(2)
			summary := prometheus.NewSummary(prometheus.SummaryOpts{Name: "late_summary"})
			summary.Observe(5)
			other := prometheus.NewGauge(prometheus.GaugeOpts{Name: "other_keyspace", ConstLabels: prometheus.Labels{keyspaceIDLabel: "43"}})
			prometheus.MustRegister(counter, histogram, summary, other)

			check := func(t *testing.T) {
				families, err := prometheus.DefaultGatherer.Gather()
				require.NoError(t, err)
				names := make(map[string]bool)
				for _, family := range families {
					names[family.GetName()] = true
					for _, metric := range family.GetMetric() {
						count := 0
						for i, label := range metric.GetLabel() {
							if i > 0 {
								require.Less(t, metric.Label[i-1].GetName(), label.GetName())
							}
							if label.GetName() == keyspaceIDLabel {
								count++
								expected := tc.expectedID
								if family.GetName() == "other_keyspace" {
									expected = "43"
								}
								require.Equal(t, expected, label.GetValue(), family.GetName())
							}
						}
						// Check every sample, including TiDB/client, Go/process and custom metrics.
						require.Equal(t, 1, count, family.GetName())
						switch family.GetName() {
						case "registered_before_init":
							require.Equal(t, float64(7), metric.GetGauge().GetValue())
						case "registered_after_init_total":
							require.Equal(t, float64(3), metric.GetCounter().GetValue())
						case "late_histogram":
							require.Equal(t, float64(2), metric.GetHistogram().GetSampleSum())
						case "late_summary":
							require.Equal(t, float64(5), metric.GetSummary().GetSampleSum())
						}
					}
				}
				require.True(t, names["go_goroutines"])
				require.True(t, names["process_cpu_seconds_total"])
			}
			check(t)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
			require.Equal(t, 200, response.Code, response.Body.String())
			require.Contains(t, response.Body.String(), `registered_before_init{keyspace_id="`+tc.expectedID+`"} 7`)
			payload := make(chan string, 1)
			pushServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				payload <- string(body)
				w.WriteHeader(http.StatusAccepted)
			}))
			t.Cleanup(pushServer.Close)
			require.NoError(t, push.New(pushServer.URL, "tidb").Gatherer(prometheus.DefaultGatherer).
				Format(expfmt.NewFormat(expfmt.TypeTextPlain)).Push())
			require.Contains(t, <-payload, `registered_before_init{keyspace_id="`+tc.expectedID+`"} 7`)
			var wg sync.WaitGroup
			gatherErrors := make([]error, 4)
			for i := range gatherErrors {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, gatherErrors[i] = prometheus.DefaultGatherer.Gather()
				}()
			}
			wg.Wait()
			for _, err := range gatherErrors {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegisterMetricsForBRDefaultMode(t *testing.T) {
	// Standalone BR never sets its deployment mode to Starter.
	if kerneltype.IsNextGen() {
		oldMode := deploymode.Get()
		require.NoError(t, deploymode.Set(deploymode.Premium))
		t.Cleanup(func() { require.NoError(t, deploymode.Set(oldMode)) })
	}
	require.False(t, deploymode.IsStarter())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	pdServer := &metricsPDServer{addr: "http://" + listener.Addr().String()}
	pdpb.RegisterPDServer(server, pdServer)
	keyspacepb.RegisterKeyspaceServer(server, pdServer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	defer config.RestoreFunc()()
	oldRegisterer, oldGatherer := prometheus.DefaultRegisterer, prometheus.DefaultGatherer
	oldLabels := cloneConstLabels()
	t.Cleanup(func() {
		prometheus.DefaultRegisterer, prometheus.DefaultGatherer = oldRegisterer, oldGatherer
		setConstLabels(oldLabels)
	})
	registry := prometheus.NewRegistry()
	prometheus.DefaultRegisterer, prometheus.DefaultGatherer = registry, registry
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metricscommon.SetConstLabels()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceObservabilityValues = config.KeyspaceObservabilityValues{}
	})
	before := prometheus.NewGauge(prometheus.GaugeOpts{Name: "br_registered_before_init"})
	before.Set(7)
	registry.MustRegister(before)
	// BR creates this handler in its common pre-run, before metric registration.
	handler := promhttp.Handler()
	require.NoError(t, RegisterMetricsForBR([]string{pdServer.addr}, task.TLSConfig{}, "ks"))
	require.True(t, prometheus.DefaultGatherer == registry)
	require.Equal(t, "42", metricscommon.GetConstLabels()[keyspaceIDLabel])
	// The existing constant-label behavior still applies to BR's own metrics.
	after := metricscommon.NewGauge(prometheus.GaugeOpts{Name: "br_registered_after_init"})
	after.Set(3)
	registry.MustRegister(after)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "br_registered_before_init 7\n")
	require.Contains(t, response.Body.String(), `br_registered_after_init{keyspace_id="42"`)
	for _, name := range []string{"go_goroutines", "process_cpu_seconds_total"} {
		require.Contains(t, response.Body.String(), "\n"+name+" ")
	}
}

func TestRegisterMetricsNonStarter(t *testing.T) {
	modes := []deploymode.Mode{deploymode.Premium}
	if kerneltype.IsNextGen() {
		modes = append(modes, deploymode.PremiumReserved)
		oldMode := deploymode.Get()
		t.Cleanup(func() { require.NoError(t, deploymode.Set(oldMode)) })
	}
	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			if kerneltype.IsNextGen() {
				require.NoError(t, deploymode.Set(mode))
			}
			defer config.RestoreFunc()()
			oldRegisterer, oldGatherer := prometheus.DefaultRegisterer, prometheus.DefaultGatherer
			oldLabels := cloneConstLabels()
			t.Cleanup(func() {
				prometheus.DefaultRegisterer, prometheus.DefaultGatherer = oldRegisterer, oldGatherer
				setConstLabels(oldLabels)
			})
			registry := prometheus.NewRegistry()
			prometheus.DefaultRegisterer, prometheus.DefaultGatherer = registry, registry
			metricscommon.SetConstLabels()
			id := uint32(42)
			config.UpdateGlobal(func(conf *config.Config) {
				conf.KeyspaceName = "ks"
				conf.KeyspaceObservabilityValues = config.KeyspaceObservabilityValues{}
			})
			require.NoError(t, RegisterMetrics())
			SetKeyspaceID(id)
			require.True(t, prometheus.DefaultGatherer == registry, "non-Starter registration must preserve the default gatherer")
			require.NotContains(t, metricscommon.GetConstLabels(), keyspaceIDLabel)
		})
	}
}

type metricsPDServer struct {
	pdpb.UnimplementedPDServer
	keyspacepb.UnimplementedKeyspaceServer
	addr string
}

func (s *metricsPDServer) GetMembers(context.Context, *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	leader := &pdpb.Member{MemberId: 1, ClientUrls: []string{s.addr}}
	return &pdpb.GetMembersResponse{Header: &pdpb.ResponseHeader{ClusterId: 1}, Members: []*pdpb.Member{leader}, Leader: leader}, nil
}

func (*metricsPDServer) LoadKeyspace(_ context.Context, req *keyspacepb.LoadKeyspaceRequest) (*keyspacepb.LoadKeyspaceResponse, error) {
	if req.GetName() != "ks" {
		return &keyspacepb.LoadKeyspaceResponse{Header: &pdpb.ResponseHeader{Error: &pdpb.Error{Message: "unknown keyspace"}}}, nil
	}
	return &keyspacepb.LoadKeyspaceResponse{Keyspace: &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42}, Name: req.GetName(),
	}}, nil
}
