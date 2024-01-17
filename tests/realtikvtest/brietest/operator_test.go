// Copyright 2023 PingCAP, Inc.
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

package brietest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/task/operator"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	serviceGCSafepointPrefix = "pd/api/v1/gc/safepoint"
	schedulersPrefix         = "pd/api/v1/schedulers"
)

func getJSON(url string, response any) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(response)
}

func pdAPI(cfg operator.PauseGcConfig, path string) string {
	return fmt.Sprintf("http://%s/%s", cfg.Config.PD[0], path)
}

type GcSafePoints struct {
	SPs []struct {
		ServiceID string `json:"service_id"`
		ExpiredAt int64  `json:"expired_at"`
		SafePoint int64  `json:"safe_point"`
	} `json:"service_gc_safe_points"`
}

func verifyGCStopped(t *require.Assertions, cfg operator.PauseGcConfig) {
	var result GcSafePoints
	t.NoError(getJSON(pdAPI(cfg, serviceGCSafepointPrefix), &result))
	for _, sp := range result.SPs {
		if sp.ServiceID != "gc_worker" {
			t.Equal(int64(cfg.SafePoint)-1, sp.SafePoint, result.SPs)
		}
	}
}

func verifyGCNotStopped(t *require.Assertions, cfg operator.PauseGcConfig) {
	var result GcSafePoints
	t.NoError(getJSON(pdAPI(cfg, serviceGCSafepointPrefix), &result))
	for _, sp := range result.SPs {
		if sp.ServiceID != "gc_worker" {
			t.FailNowf("the service gc safepoint exists", "it is %#v", sp)
		}
	}
}

func verifyLightningStopped(t *require.Assertions, cfg operator.PauseGcConfig) {
	cx := context.Background()
	pdc, err := pd.NewClient(cfg.Config.PD, pd.SecurityOption{})
	t.NoError(err)
	defer pdc.Close()
	stores, err := pdc.GetAllStores(cx, pd.WithExcludeTombstone())
	t.NoError(err)
	s := stores[0]
	conn, err := grpc.DialContext(cx, s.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	t.NoError(err)
	ingestCli := import_sstpb.NewImportSSTClient(conn)
	res, err := ingestCli.Ingest(cx, &import_sstpb.IngestRequest{})
	t.NoError(err)
	t.NotNil(res.GetError(), "res = %s", res)
}

func verifySchedulersStopped(t *require.Assertions, cfg operator.PauseGcConfig) {
	var (
		schedulers       []string
		pausedSchedulers []string
		target           = pdAPI(cfg, schedulersPrefix)
	)

	t.NoError(getJSON(target, &schedulers))
	enabledSchedulers := map[string]struct{}{}
	for _, sched := range schedulers {
		enabledSchedulers[sched] = struct{}{}
	}
	t.NoError(getJSON(target+"?status=paused", &pausedSchedulers))
	for _, scheduler := range pausedSchedulers {
		t.Contains(enabledSchedulers, scheduler)
	}
}

func verifySchedulerNotStopped(t *require.Assertions, cfg operator.PauseGcConfig) {
	var (
		schedulers       []string
		pausedSchedulers []string
		target           = pdAPI(cfg, schedulersPrefix)
	)

	t.NoError(getJSON(target, &schedulers))
	enabledSchedulers := map[string]struct{}{}
	for _, sched := range schedulers {
		enabledSchedulers[sched] = struct{}{}
	}
	t.NoError(getJSON(target+"?status=paused", &pausedSchedulers))
	for _, scheduler := range pausedSchedulers {
		t.NotContains(enabledSchedulers, scheduler)
	}
}

func cleanUpGCSafepoint(cfg operator.PauseGcConfig, t *testing.T) {
	var result GcSafePoints
	pdCli, err := pd.NewClient(cfg.PD, pd.SecurityOption{})
	require.NoError(t, err)
	defer pdCli.Close()
	getJSON(pdAPI(cfg, serviceGCSafepointPrefix), &result)
	for _, sp := range result.SPs {
		if sp.ServiceID != "gc_worker" {
			sp.SafePoint = 0
			_, err := pdCli.UpdateServiceGCSafePoint(context.Background(), sp.ServiceID, 0, 0)
			require.NoError(t, err)
		}
	}
}

func TestOperator(t *testing.T) {
	req := require.New(t)
	rd := make(chan struct{})
	ex := make(chan struct{})
	cfg := operator.PauseGcConfig{
		Config: task.Config{
			PD: []string{"127.0.0.1:2379"},
		},
		TTL:       5 * time.Minute,
		SafePoint: oracle.GoTimeToTS(time.Now()),
		OnAllReady: func() {
			close(rd)
		},
		OnExit: func() {
			close(ex)
		},
	}

	cleanUpGCSafepoint(cfg, t)

	verifyGCNotStopped(req, cfg)
	verifySchedulerNotStopped(req, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		req.NoError(operator.AdaptEnvForSnapshotBackup(ctx, &cfg))
	}()
	req.Eventually(func() bool {
		select {
		case <-rd:
			return true
		default:
			return false
		}
	}, 10*time.Second, time.Second)

	cancel()
	verifyGCStopped(req, cfg)
	verifyLightningStopped(req, cfg)
	verifySchedulersStopped(req, cfg)

	req.Eventually(func() bool {
		select {
		case <-ex:
			return true
		default:
			return false
		}
	}, 10*time.Second, time.Second)

	verifySchedulerNotStopped(req, cfg)
	verifyGCNotStopped(req, cfg)
}
