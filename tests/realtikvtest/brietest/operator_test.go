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

	"github.com/google/uuid"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
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
	t.NoError(err)
	region, err := pdc.GetRegion(cx, []byte("a"))
	t.NoError(err)
	store, err := pdc.GetStore(cx, region.Leader.StoreId)
	t.NoError(err)
	conn, err := grpc.DialContext(cx, store.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	t.NoError(err)
	ingestCli := import_sstpb.NewImportSSTClient(conn)
	wcli, err := ingestCli.Write(cx)
	t.NoError(err)
	u := uuid.New()
	meta := &import_sstpb.SSTMeta{
		Uuid:        u[:],
		RegionId:    region.Meta.GetId(),
		RegionEpoch: region.Meta.GetRegionEpoch(),
		Range: &import_sstpb.Range{
			Start: []byte("a"),
			End:   []byte("b"),
		},
	}
	rpcCx := kvrpcpb.Context{
		RegionId:    region.Meta.GetId(),
		RegionEpoch: region.Meta.GetRegionEpoch(),
		Peer:        region.Leader,
	}
	t.NoError(wcli.Send(&import_sstpb.WriteRequest{Chunk: &import_sstpb.WriteRequest_Meta{Meta: meta}, Context: &rpcCx}))
	phy, log, err := pdc.GetTS(cx)
	t.NoError(err)
	wb := &import_sstpb.WriteBatch{
		CommitTs: oracle.ComposeTS(phy, log),
		Pairs: []*import_sstpb.Pair{
			{Key: []byte("a1"), Value: []byte("You may wondering, why here is such a key.")},
			{Key: []byte("a2"), Value: []byte("And what if this has been really imported?")},
			{Key: []byte("a3"), Value: []byte("I dunno too. But we need to have a try.")},
		},
	}
	t.NoError(wcli.Send(&import_sstpb.WriteRequest{Chunk: &import_sstpb.WriteRequest_Batch{Batch: wb}, Context: &rpcCx}))
	resp, err := wcli.CloseAndRecv()
	t.NoError(err)
	t.Nil(resp.Error, "res = %s", resp)
	realMeta := resp.Metas[0]

	res, err := ingestCli.Ingest(cx, &import_sstpb.IngestRequest{
		Context: &rpcCx,
		Sst:     realMeta,
	})
	t.NoError(err)
	t.Contains(res.GetError().GetMessage(), "Suspended", "res = %s", res)
	t.NotNil(res.GetError().GetServerIsBusy(), "res = %s", res)
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

	verifyGCStopped(req, cfg)
	verifyLightningStopped(req, cfg)
	verifySchedulersStopped(req, cfg)
	cancel()

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

func TestFailure(t *testing.T) {
	req := require.New(t)
	req.NoError(failpoint.Enable("github.com/pingcap/tidb/br/pkg/backup/prepare_snap/PrepareConnectionsErr", "return()"))
	// Make goleak happy.
	req.NoError(failpoint.Enable("github.com/pingcap/tidb/br/pkg/task/operator/SkipReadyHint", "return()"))
	defer func() {
		req.NoError(failpoint.Disable("github.com/pingcap/tidb/br/pkg/backup/prepare_snap/PrepareConnectionsErr"))
		req.NoError(failpoint.Disable("github.com/pingcap/tidb/br/pkg/task/operator/SkipReadyHint"))
	}()

	cfg := operator.PauseGcConfig{
		Config: task.Config{
			PD: []string{"127.0.0.1:2379"},
		},
		TTL:       5 * time.Minute,
		SafePoint: oracle.GoTimeToTS(time.Now()),
	}

	verifyGCNotStopped(req, cfg)
	verifySchedulerNotStopped(req, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := operator.AdaptEnvForSnapshotBackup(ctx, &cfg)
	require.Error(t, err)

	verifyGCNotStopped(req, cfg)
	verifySchedulerNotStopped(req, cfg)
}
