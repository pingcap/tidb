// Copyright 2026 PingCAP, Inc.
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

package tikvhandler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	_ "github.com/pingcap/tidb/pkg/autoid_service" // Initialize the mockstore's auto-ID service.
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/dxf/framework/dxfutil"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestKeyspaceActiveJobsDXFRuntime(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("cross keyspace runtimes require the nextgen kernel")
	}
	const targetKS = "dxf-new"
	const pollKS = "poll-slow"
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 2})
	t.Cleanup(func() { cluster.Terminate(t) })
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/injectETCDCli",
		func(cliP **clientv3.Client, ks string) {
			for i, name := range []string{keyspace.System, targetKS} {
				if name != ks {
					continue
				}
				cli := cluster.Client(i)
				require.NotNil(t, cli)
				cluster.TakeClient(i)
				codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
					Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: uint32(i + 1)}, Name: ks,
				})
				require.NoError(t, err)
				etcd.SetEtcdCliByNamespace(cli, keyspace.MakeKeyspaceEtcdNamespace(codec))
				*cliP = cli
				return
			}
			t.Errorf("unexpected keyspace %s", ks)
		},
	)
	sysStore, sysDom := testkit.CreateMockStoreAndDomainForKS(t, keyspace.System)
	stores := map[string]kv.Storage{keyspace.System: sysStore}
	pollStarted, unblockPoll := make(chan struct{}), make(chan struct{})
	var unblockOnce sync.Once
	unblock := func() { unblockOnce.Do(func() { close(unblockPoll) }) }
	pollErr := errors.New("poll store unavailable")
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeGetStore",
		func(open *func(string) (kv.Storage, error)) {
			*open = func(ks string) (kv.Storage, error) {
				if ks == pollKS {
					close(pollStarted)
					<-unblockPoll
					return nil, pollErr
				}
				return stores[ks], nil
			}
		},
	)
	// testkit owns the stores shared by the source domains and cross-keyspace runtimes.
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/skipCloseStore", func(closeStore *bool) {
		*closeStore = false
	})
	targetStore, _ := testkit.CreateMockStoreAndDomainForKS(t, targetKS)
	stores[targetKS] = targetStore
	config.UpdateGlobal(func(conf *config.Config) { conf.KeyspaceName = keyspace.System })
	t.Cleanup(sysDom.GetCrossKSMgr().Close)
	t.Cleanup(unblock)
	taskMgr := storage.NewTaskManager(sysDom.SysSessionPool())
	newHandler := func() *KeyspaceActiveJobsHandler {
		// Mock PD knows only its own keyspace; keep the real runtime acquisition path.
		return &KeyspaceActiveJobsHandler{acquireRuntime: sysDom.AcquireKSRuntimeWithContext}
	}
	post := func(h *KeyspaceActiveJobsHandler, ks string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/keyspaces/jobs/active",
			strings.NewReader(`{"keyspaces":["`+ks+`"]}`)))
		return w
	}

	t.Run("overlapping polls leave DXF able to acquire a cold runtime", func(t *testing.T) {
		require.NotContains(t, sysDom.GetCrossKSMgr().GetAllKeyspace(), targetKS)
		ctx, cancel := context.WithCancel(context.Background())
		pollDone := make(chan struct{})
		t.Cleanup(func() {
			cancel()
			unblock()
			select {
			case <-pollDone:
			case <-time.After(10 * time.Second):
				t.Error("polling worker did not finish")
			}
		})
		pollResponse := httptest.NewRecorder()
		go func() {
			defer close(pollDone)
			newHandler().ServeHTTP(pollResponse, httptest.NewRequest(http.MethodPost, "/keyspaces/jobs/active",
				strings.NewReader(`{"keyspaces":["poll-slow"]}`)).WithContext(ctx))
		}()
		select {
		case <-pollStarted:
		case <-time.After(10 * time.Second):
			t.Fatal("polling did not enter store acquisition")
		}

		// The hook blocks after selecting the store-open function. This exercises
		// HTTP admission and real DXF initialization, not the TiKV driver's mutex.
		var taskRuntime sqlsvrapi.Runtime
		var release func()
		var acquireErr error
		dxfDone := make(chan struct{})
		t.Cleanup(func() {
			unblock()
			select {
			case <-dxfDone:
				if release != nil {
					release()
				}
			case <-time.After(10 * time.Second):
				t.Error("DXF acquisition did not finish")
			}
		})
		go func() {
			defer close(dxfDone)
			taskRuntime, release, acquireErr = dxfutil.AcquireTaskRuntime(taskMgr, targetKS, "DXF/test/cold")
		}()
		select {
		case <-dxfDone:
		case <-time.After(10 * time.Second):
			t.Fatal("DXF could not initialize another keyspace while polling was blocked")
		}
		require.NoError(t, acquireErr)
		require.Same(t, targetStore, taskRuntime.Store())
		cancel()
		secondHandler := newHandler()
		w := post(secondHandler, targetKS)
		require.Equal(t, http.StatusTooManyRequests, w.Code, w.Body.String())
		select {
		case <-pollDone:
			t.Fatal("canceled polling returned before store acquisition finished")
		case <-time.After(100 * time.Millisecond):
		}
		require.Equal(t, http.StatusTooManyRequests, post(secondHandler, targetKS).Code,
			"canceled polling must hold admission until store acquisition returns")
		unblock()
		select {
		case <-pollDone:
		case <-time.After(10 * time.Second):
			t.Fatal("polling did not join its worker")
		}
		require.Equal(t, http.StatusOK, pollResponse.Code, pollResponse.Body.String())
		require.Contains(t, pollResponse.Body.String(), pollErr.Error())
		w = post(secondHandler, targetKS)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	})

	t.Run("polling retains the idle runtime released by DXF", func(t *testing.T) {
		taskRuntime, release, err := dxfutil.AcquireTaskRuntime(taskMgr, targetKS, "DXF/test/before-poll")
		require.NoError(t, err)
		t.Cleanup(release)
		pool := taskRuntime.SysSessionPool()
		release()
		w := post(newHandler(), targetKS)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.JSONEq(t, `{"keyspaces":[{"keyspace":"dxf-new","ddl_jobs":0,"import_jobs":0}]}`, w.Body.String())
		require.Contains(t, sysDom.GetCrossKSMgr().GetAllKeyspace(), targetKS,
			"polling must retain the idle runtime for DXF reuse")
		taskRuntime, release, err = dxfutil.AcquireTaskRuntime(taskMgr, targetKS, "DXF/test/after-poll")
		require.NoError(t, err)
		defer release()
		require.Same(t, pool, taskRuntime.SysSessionPool())
	})
}
