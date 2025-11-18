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
	tikvconfig "github.com/tikv/client-go/v2/config"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
)

var componentName = caller.Component("tidb-metrics-util")

// RegisterMetrics register metrics with const label 'keyspace_id' if keyspaceName set.
func RegisterMetrics() error {
	cfg := config.GetGlobalConfig()
	if keyspace.IsKeyspaceNameEmpty(cfg.KeyspaceName) || cfg.Store != config.StoreTypeTiKV {
		registerMetrics(nil) // register metrics without label 'keyspace_id'.
		return nil
	}

	if kerneltype.IsNextGen() {
		metricscommon.SetConstLabels("keyspace_name", cfg.KeyspaceName)
	}

	pdAddrs, _, _, err := tikvconfig.ParsePath("tikv://" + cfg.Path)
	if err != nil {
		return err
	}

	timeoutSec := time.Duration(cfg.PDClient.PDServerTimeout) * time.Second
	// Note: for NextGen, we need to use the side effect of `NewClient` to init the metrics' builtin const labels
	pdCli, err := pd.NewClient(componentName, pdAddrs, pd.SecurityOption{
		CAPath:   cfg.Security.ClusterSSLCA,
		CertPath: cfg.Security.ClusterSSLCert,
		KeyPath:  cfg.Security.ClusterSSLKey,
	}, opt.WithCustomTimeoutOption(timeoutSec), opt.WithMetricsLabels(metricscommon.GetConstLabels()))
	if err != nil {
		return err
	}
	defer pdCli.Close()

	if kerneltype.IsNextGen() {
		registerMetrics(nil) // metrics' const label already set
	} else {
		keyspaceMeta, err := getKeyspaceMeta(pdCli, cfg.KeyspaceName)
		if err != nil {
			return err
		}
		registerMetrics(keyspaceMeta)
	}
	return nil
}

// RegisterMetricsForBR register metrics with const label keyspace_id for BR.
func RegisterMetricsForBR(pdAddrs []string, tls task.TLSConfig, keyspaceName string) error {
	if keyspace.IsKeyspaceNameEmpty(keyspaceName) {
		registerMetrics(nil) // register metrics without label 'keyspace_id'.
		return nil
	}

	if kerneltype.IsNextGen() {
		metricscommon.SetConstLabels("keyspace_name", keyspaceName)
	}

	timeoutSec := 10 * time.Second
	securityOpt := pd.SecurityOption{}
	if tls.IsEnabled() {
		securityOpt = tls.ToPDSecurityOption()
	}
	// Note: for NextGen, pdCli is created to init the metrics' const labels
	pdCli, err := pd.NewClient(componentName, pdAddrs, securityOpt,
		opt.WithCustomTimeoutOption(timeoutSec), opt.WithMetricsLabels(metricscommon.GetConstLabels()))
	if err != nil {
		return err
	}
	defer pdCli.Close()

	if kerneltype.IsNextGen() {
		registerMetrics(nil) // metrics' const label already set
	} else {
		keyspaceMeta, err := getKeyspaceMeta(pdCli, keyspaceName)
		if err != nil {
			return err
		}
		registerMetrics(keyspaceMeta)
	}
	return nil
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

func registerMetrics(keyspaceMeta *keyspacepb.KeyspaceMeta) {
	if keyspaceMeta != nil {
		metricscommon.SetConstLabels("keyspace_id", fmt.Sprint(keyspaceMeta.GetId()))
	}
	initMetrics()
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
