// Copyright 2024 PingCAP, Inc.
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

package standby_test

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/server/internal/testserverclient"
	"github.com/pingcap/tidb/pkg/server/internal/testutil"
	util2 "github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

type mockStandbyController struct {
	waitingForActivate atomic.Bool
	activate           chan struct{}
	connActiveCounter  atomic.Int32
}

func newMockStandbyController() *mockStandbyController {
	return &mockStandbyController{
		activate: make(chan struct{}),
	}
}

func (c *mockStandbyController) WaitForActivate() {
	c.waitingForActivate.Store(true)
	<-c.activate
}

func (c *mockStandbyController) Activate() {
	close(c.activate)
}

func (c *mockStandbyController) EndStandby(err error) {
}

func (c *mockStandbyController) Handler(svr *server.Server) (pathPrefix string, mux *http.ServeMux) {
	mux = http.NewServeMux()
	mux.HandleFunc("/mock-standby/status", func(w http.ResponseWriter, req *http.Request) {})
	return "/mock-standby/", mux
}

func (c *mockStandbyController) OnConnActive() {
	c.connActiveCounter.Add(1)
}

func (c *mockStandbyController) OnServerCreated(svr *server.Server) {
}

func TestStandby(t *testing.T) {
	standbyController := newMockStandbyController()
	var svr *server.Server
	serverCreated := make(chan struct{})
	go func() {
		standbyController.WaitForActivate()
		store := testkit.CreateMockStore(t)
		cfg := util2.NewTestConfig()
		cfg.Port = 0
		cfg.Status.StatusPort = 0
		cfg.Status.ReportStatus = true
		cfg.Socket = fmt.Sprintf("/tmp/tidb-mock-%d.sock", time.Now().UnixNano())
		drv := server.NewTiDBDriver(store)
		var err error
		svr, err = server.NewServer(cfg, drv)
		require.NoError(t, err)
		dom, err := session.GetDomain(store)
		require.NoError(t, err)
		svr.SetDomain(dom)
		svr.StandbyController = standbyController
		close(serverCreated)
		err = svr.Run(nil)
		require.NoError(t, err)
	}()

	require.Eventually(t, func() bool {
		return standbyController.waitingForActivate.Load()
	}, time.Second*5, time.Millisecond*100)

	standbyController.Activate()

	select {
	case <-serverCreated:
	case <-time.After(time.Second * 15):
		t.Fatal("server creation timeout")
	}

	<-server.RunInGoTestChan

	defer svr.Close()

	fmt.Println(svr)
	fmt.Println(svr.ListenAddr())

	client := testserverclient.NewTestServerClient()
	client.Port = testutil.GetPortFromTCPAddr(svr.ListenAddr())
	client.StatusPort = testutil.GetPortFromTCPAddr(svr.StatusListenerAddr())
	client.WaitUntilServerOnline()

	resp, err := client.FetchStatus("/mock-standby/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	client.RunTestInitConnect(t)
	require.Greater(t, standbyController.connActiveCounter.Load(), int32(0))
}
