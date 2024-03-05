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
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	domain_metrics "github.com/pingcap/tidb/pkg/domain/metrics"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	infoschema_metrics "github.com/pingcap/tidb/pkg/infoschema/metrics"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/metrics"
	plannercore "github.com/pingcap/tidb/pkg/planner/core/metrics"
	server_metrics "github.com/pingcap/tidb/pkg/server/metrics"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	txninfo "github.com/pingcap/tidb/pkg/session/txninfo"
	isolation_metrics "github.com/pingcap/tidb/pkg/sessiontxn/isolation/metrics"
	statshandler_metrics "github.com/pingcap/tidb/pkg/statistics/handle/metrics"
	kvstore "github.com/pingcap/tidb/pkg/store"
	copr_metrics "github.com/pingcap/tidb/pkg/store/copr/metrics"
	unimetrics "github.com/pingcap/tidb/pkg/store/mockstore/unistore/metrics"
	ttlmetrics "github.com/pingcap/tidb/pkg/ttl/metrics"
	"github.com/pingcap/tidb/pkg/util"
	topsqlreporter_metrics "github.com/pingcap/tidb/pkg/util/topsql/reporter/metrics"
	tikvconfig "github.com/tikv/client-go/v2/config"
	pd "github.com/tikv/pd/client"
)

// RegisterMetrics register metrics with const label 'keyspace_id' if keyspaceName set.
func RegisterMetrics() error {
	cfg := config.GetGlobalConfig()
	if keyspace.IsKeyspaceNameEmpty(cfg.KeyspaceName) || strings.ToLower(cfg.Store) != "tikv" {
		return registerMetrics(nil) // register metrics without label 'keyspace_id'.
	}

	pdAddrs, _, _, err := tikvconfig.ParsePath("tikv://" + cfg.Path)
	if err != nil {
		return err
	}

	timeoutSec := time.Duration(cfg.PDClient.PDServerTimeout) * time.Second
	pdCli, err := pd.NewClient(pdAddrs, pd.SecurityOption{
		CAPath:   cfg.Security.ClusterSSLCA,
		CertPath: cfg.Security.ClusterSSLCert,
		KeyPath:  cfg.Security.ClusterSSLKey,
	}, pd.WithCustomTimeoutOption(timeoutSec))
	if err != nil {
		return err
	}
	defer pdCli.Close()

	keyspaceMeta, err := getKeyspaceMeta(pdCli, cfg.KeyspaceName)
	if err != nil {
		return err
	}

	return registerMetrics(keyspaceMeta)
}

// RegisterMetricsForBR register metrics with const label keyspace_id for BR.
func RegisterMetricsForBR(pdAddrs []string, keyspaceName string) error {
	if keyspace.IsKeyspaceNameEmpty(keyspaceName) {
		return registerMetrics(nil) // register metrics without label 'keyspace_id'.
	}

	timeoutSec := 10 * time.Second
	pdCli, err := pd.NewClient(pdAddrs, pd.SecurityOption{},
		pd.WithCustomTimeoutOption(timeoutSec))
	if err != nil {
		return err
	}
	defer pdCli.Close()

	keyspaceMeta, err := getKeyspaceMeta(pdCli, keyspaceName)
	if err != nil {
		return err
	}

	return registerMetrics(keyspaceMeta)
}

func registerMetrics(keyspaceMeta *keyspacepb.KeyspaceMeta) error {
	if keyspaceMeta != nil {
		metrics.SetConstLabels("keyspace_id", fmt.Sprint(keyspaceMeta.GetId()))
	}

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
	topsqlreporter_metrics.InitMetricsVars()
	ttlmetrics.InitMetricsVars()
	txninfo.InitMetricsVars()

	if config.GetGlobalConfig().Store == "unistore" {
		unimetrics.RegisterMetrics()
	}
	return nil
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
