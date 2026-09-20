// Copyright 2023 PingCAP, Inc.
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

package metricsutil

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	domain_metrics "github.com/pingcap/tidb/pkg/domain/metrics"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	infoschema_metrics "github.com/pingcap/tidb/pkg/infoschema/metrics"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/metrics"
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	plannercore "github.com/pingcap/tidb/pkg/planner/core/metrics"
	server_metrics "github.com/pingcap/tidb/pkg/server/metrics"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	txninfo "github.com/pingcap/tidb/pkg/session/txninfo"
	isolation_metrics "github.com/pingcap/tidb/pkg/sessiontxn/isolation/metrics"
	statscache_metrics "github.com/pingcap/tidb/pkg/statistics/handle/cache/metrics"
	statshandler_metrics "github.com/pingcap/tidb/pkg/statistics/handle/metrics"
	kvstore "github.com/pingcap/tidb/pkg/store"
	copr_metrics "github.com/pingcap/tidb/pkg/store/copr/metrics"
	unimetrics "github.com/pingcap/tidb/pkg/store/mockstore/unistore/metrics"
	ttlmetrics "github.com/pingcap/tidb/pkg/ttl/metrics"
	"github.com/pingcap/tidb/pkg/util"
	topsqlreporter_metrics "github.com/pingcap/tidb/pkg/util/topsql/reporter/metrics"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
)

var componentName = caller.Component("tidb-metrics-util")

const keyspaceIDLabel = "keyspace_id"

// RegisterMetrics registers metrics with keyspace metadata labels when available.
func RegisterMetrics() error {
	cfg := config.GetGlobalConfig()
	if kerneltype.IsNextGen() {
		metricscommon.SetConstLabels("keyspace_name", cfg.KeyspaceName)
	}
	return registerMetrics()
}

// RegisterMetricsForBR registers metrics with keyspace metadata labels for BR.
func RegisterMetricsForBR(pdAddrs []string, tls task.TLSConfig, keyspaceName string) error {
	if keyspace.IsKeyspaceNameEmpty(keyspaceName) {
		return registerMetrics()
	}

	if kerneltype.IsNextGen() {
		metricscommon.SetConstLabels("keyspace_name", keyspaceName)
	}

	timeoutSec := 10 * time.Second
	securityOpt := pd.SecurityOption{}
	if tls.IsEnabled() {
		securityOpt = tls.ToPDSecurityOption()
	}
	pdCli, err := pd.NewClient(componentName, pdAddrs, securityOpt,
		opt.WithCustomTimeoutOption(timeoutSec), opt.WithInitMetricsOption(false))
	if err != nil {
		return err
	}
	defer pdCli.Close()

	keyspaceMeta, err := getKeyspaceMeta(pdCli, keyspaceName)
	if err != nil {
		return err
	}
	setKeyspaceIDConstLabel(keyspaceMeta.GetId())
	return registerMetrics()
}

func initMetrics() {
	metrics.InitMetrics()
	metrics.RegisterMetrics()

	copr_metrics.InitMetricsVars()
	domain_metrics.InitMetricsVars()
	executor_metrics.InitMetricsVars()
	infoschema_metrics.InitMetricsVars()
	isolation_metrics.InitMetricsVars()
	plannercore.InitMetricsVars()
	server_metrics.InitMetricsVars()
	session_metrics.InitMetricsVars()
	statshandler_metrics.InitMetricsVars()
	statscache_metrics.InitMetricsVars()
	topsqlreporter_metrics.InitMetricsVars()
	ttlmetrics.InitMetricsVars()
	txninfo.InitMetricsVars()

	if config.GetGlobalConfig().Store == config.StoreTypeUniStore {
		unimetrics.RegisterMetrics()
	}
}

func registerMetrics() error {
	labels := cloneConstLabels()
	maps.Copy(labels, config.GetGlobalConfig().GetKeyspaceObservabilityMetricLabels())
	if len(labels) > 0 {
		setConstLabels(labels)
	}
	initMetrics()
	return nil
}

func cloneConstLabels() map[string]string {
	labels := maps.Clone(metricscommon.GetConstLabels())
	if labels == nil {
		labels = make(map[string]string)
	}
	return labels
}

func setKeyspaceIDConstLabel(keyspaceID uint32) {
	labels := cloneConstLabels()
	labels[keyspaceIDLabel] = fmt.Sprint(keyspaceID)
	setConstLabels(labels)
}

func setConstLabels(labels map[string]string) {
	kv := make([]string, 0, len(labels)*2)
	for k, v := range labels {
		kv = append(kv, k, v)
	}
	metricscommon.SetConstLabels(kv...)
}

func getKeyspaceMeta(pdCli pd.Client, keyspaceName string) (*keyspacepb.KeyspaceMeta, error) {
	// Load Keyspace meta with retry.
	var keyspaceMeta *keyspacepb.KeyspaceMeta
	err := util.RunWithRetry(util.DefaultMaxRetries, util.RetryInterval, func() (bool, error) {
		var errInner error
		keyspaceMeta, errInner = pdCli.LoadKeyspace(context.TODO(), keyspaceName)
		// Retry when pd not bootstrapped or if keyspace not exists.
		if kvstore.IsNotBootstrappedError(errInner) || kvstore.IsKeyspaceNotExistError(errInner) {
			return true, errInner
		}
		// Do not retry when success or encountered unexpected error.
		return false, errInner
	})
	if err != nil {
		return nil, err
	}

	return keyspaceMeta, nil
}
