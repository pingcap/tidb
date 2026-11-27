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

package server

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	autoid "github.com/pingcap/tidb/pkg/autoid_service"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

type statusReadListener struct {
	net.Listener
	reading chan struct{}
}

func (l *statusReadListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &statusReadConn{Conn: conn, reading: l.reading}, nil
}

type statusReadConn struct {
	net.Conn
	reading chan struct{}
}

func (c *statusReadConn) Read(b []byte) (int, error) {
	select {
	case c.reading <- struct{}{}:
	default:
	}
	return c.Conn.Read(b)
}

type statusShutdownController struct {
	testStandbyController
	onShutdown func(StandbyShutdownServer)
}

func (c *statusShutdownController) OnServerShutdown(s StandbyShutdownServer) {
	c.onShutdown(s)
}

type statusAutoIDStore struct {
	kv.Storage
	kv.EtcdBackend
	etcdAddrs []string
}

func (s *statusAutoIDStore) EtcdAddrs() ([]string, error) {
	return s.etcdAddrs, nil
}

func (*statusAutoIDStore) TLSConfig() *tls.Config { return nil }

type blockingStatusLeaseServer struct {
	etcdserverpb.UnimplementedLeaseServer
	entered      chan struct{}
	canceled     chan struct{}
	release      chan struct{}
	enteredOnce  sync.Once
	canceledOnce sync.Once
}

func (s *blockingStatusLeaseServer) LeaseGrant(ctx context.Context, _ *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	s.enteredOnce.Do(func() { close(s.entered) })
	select {
	case <-ctx.Done():
		s.canceledOnce.Do(func() { close(s.canceled) })
		return nil, ctx.Err()
	case <-s.release:
		// Let an uncanceled constructor complete during failing-test cleanup.
		return &etcdserverpb.LeaseGrantResponse{ID: 1, TTL: 10}, nil
	}
}

func (*blockingStatusLeaseServer) LeaseRevoke(context.Context, *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	return &etcdserverpb.LeaseRevokeResponse{}, nil
}

func (*blockingStatusLeaseServer) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if err := stream.Send(&etcdserverpb.LeaseKeepAliveResponse{ID: req.ID, TTL: 10}); err != nil {
			return err
		}
	}
}

type blockingStatusCampaignServer struct {
	blockingStatusLeaseServer
	etcdserverpb.UnimplementedKVServer
	etcdserverpb.UnimplementedWatchServer
	waiting     chan struct{}
	revoked     chan struct{}
	resign      chan struct{}
	waitingOnce sync.Once
	revokedOnce sync.Once
}

func (s *blockingStatusCampaignServer) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	if len(req.Success) > 0 && req.Success[0].GetRequestDeleteRange() != nil {
		// A canceled election resigns using the session client's context. Keep
		// that transaction stalled unless it is canceled or cleanup releases it.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.resign:
		}
	}
	return &etcdserverpb.TxnResponse{Header: &etcdserverpb.ResponseHeader{Revision: 2}, Succeeded: true}, nil
}

func (*blockingStatusCampaignServer) Range(_ context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	// An older candidate forces Election.Campaign to wait for its deletion.
	return &etcdserverpb.RangeResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 2},
		Kvs:    []*mvccpb.KeyValue{{Key: []byte(string(req.Key) + "existing-owner"), CreateRevision: 1}},
	}, nil
}

func (s *blockingStatusCampaignServer) Watch(stream etcdserverpb.Watch_WatchServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		created := req.GetCreateRequest() != nil
		if err := stream.Send(&etcdserverpb.WatchResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: 2}, WatchId: 1, Created: created, Canceled: !created,
		}); err != nil {
			return err
		}
		if created {
			s.waitingOnce.Do(func() { close(s.waiting) })
		}
	}
}

func (s *blockingStatusCampaignServer) LeaseRevoke(context.Context, *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	s.revokedOnce.Do(func() { close(s.revoked) })
	return &etcdserverpb.LeaseRevokeResponse{}, nil
}

func TestStatusServerHTTPMethods(t *testing.T) {
	cfg := util.NewTestConfig()
	cfg.Status.StatusPort = 0
	s := NewTestServer(cfg)
	require.NoError(t, s.listenStatusHTTPServer())
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-Method", r.Method)
		w.WriteHeader(http.StatusNoContent)
	})
	servingDone := make(chan struct{})
	go func() {
		s.startStatusServerAndRPCServer(mux)
		close(servingDone)
	}()
	defer func() {
		s.Close()
		select {
		case <-servingDone:
		case <-time.After(5 * time.Second):
			t.Error("status serving did not finish")
		}
	}()
	// Each method must be classified on its own connection by cmux.
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 5 * time.Second}
	for _, method := range []string{
		http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace, http.MethodPatch,
	} {
		t.Run(method, func(t *testing.T) {
			req, err := http.NewRequest(method, "http://"+s.statusListener.Addr().String()+"/", nil)
			require.NoError(t, err)
			resp, err := client.Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusNoContent, resp.StatusCode)
			require.Equal(t, method, resp.Header.Get("X-Request-Method"))
		})
	}
}

func TestStatusServerClose(t *testing.T) {
	t.Run("AutoID without etcd endpoints", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		cfg := util.NewTestConfig()
		cfg.Store = config.StoreTypeTiKV
		cfg.Path = "%"
		s := NewTestServer(cfg)
		s.driver = NewTiDBDriver(store)
		defer s.Close()
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		logger, props, err := log.InitLogger(new(log.Config), zap.WithFatalHook(zapcore.WriteThenPanic))
		require.NoError(t, err)
		defer log.ReplaceGlobals(logger, props)()

		require.NotPanics(t, func() {
			require.Nil(t, s.setupAutoIDService(context.Background(), grpcServer))
		})
		require.Empty(t, grpcServer.GetServiceInfo(), "stores without etcd endpoints must not register an AutoID service")
	})

	t.Run("AutoID initialization error", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		cfg := util.NewTestConfig()
		cfg.Store = config.StoreTypeTiKV
		// AutoID initialization must reuse the open store without reopening this path.
		cfg.Path = "%"
		s := NewTestServer(cfg)
		// A malformed URL escape fails client construction without dialing etcd.
		s.driver = NewTiDBDriver(&statusAutoIDStore{Storage: store, etcdAddrs: []string{"%"}})
		defer s.Close()
		grpcServer := grpc.NewServer()
		defer grpcServer.Stop()
		logger, props, err := log.InitLogger(new(log.Config), zap.WithFatalHook(zapcore.WriteThenPanic))
		require.NoError(t, err)
		defer log.ReplaceGlobals(logger, props)()

		require.PanicsWithValue(t, "initialize auto ID service failed", func() {
			s.setupAutoIDService(context.Background(), grpcServer)
		})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NotPanics(t, func() {
			require.Nil(t, s.setupAutoIDService(ctx, grpcServer))
		})
		require.Empty(t, grpcServer.GetServiceInfo(), "failed AutoID initialization must not register a service")
	})

	t.Run("blocked AutoID lease grant", func(t *testing.T) {
		ignoreCurrent := goleak.IgnoreCurrent()
		t.Cleanup(func() { goleak.VerifyNone(t, ignoreCurrent) })
		store := testkit.CreateMockStore(t)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()
		leaseServer := &blockingStatusLeaseServer{
			entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}),
		}
		grpcServer := grpc.NewServer()
		etcdserverpb.RegisterLeaseServer(grpcServer, leaseServer)
		go func() { _ = grpcServer.Serve(listener) }()
		defer grpcServer.Stop()
		cfg := util.NewTestConfig()
		cfg.Status.StatusPort = 0
		cfg.Store = config.StoreTypeTiKV
		cfg.Path = "%"
		s := NewTestServer(cfg)
		s.driver = NewTiDBDriver(&statusAutoIDStore{Storage: store, etcdAddrs: []string{listener.Addr().String()}})
		require.NoError(t, s.listenStatusHTTPServer())
		closed := make(chan struct{})
		servingDone := make(chan struct{})
		closeServer := sync.OnceFunc(func() {
			s.Close()
			close(closed)
		})
		defer func() {
			close(leaseServer.release)
			go closeServer()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Error("status shutdown did not finish during cleanup")
			}
			select {
			case <-servingDone:
			case <-time.After(5 * time.Second):
				t.Error("status initializer did not finish during cleanup")
			}
		}()
		go func() {
			s.startStatusServerAndRPCServer(http.NewServeMux())
			close(servingDone)
		}()
		select {
		case <-leaseServer.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("AutoID initialization did not request a lease")
		}
		go closeServer()
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("shutdown must cancel the blocked AutoID lease grant")
		}
		select {
		case <-leaseServer.canceled:
		case <-time.After(5 * time.Second):
			t.Fatal("AutoID lease grant was not canceled")
		}
		require.Nil(t, s.autoIDService)
		require.Nil(t, s.statusServer.Load())
		require.Nil(t, s.grpcServer)
	})

	for _, initializing := range []bool{true, false} {
		name := "after initialization"
		if initializing {
			name = "during initialization"
		}
		t.Run("blocked AutoID campaign/"+name, func(t *testing.T) {
			ignoreCurrent := goleak.IgnoreCurrent()
			t.Cleanup(func() { goleak.VerifyNone(t, ignoreCurrent) })
			store := testkit.CreateMockStore(t)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()
			campaignServer := &blockingStatusCampaignServer{
				blockingStatusLeaseServer: blockingStatusLeaseServer{
					entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}),
				},
				waiting: make(chan struct{}), revoked: make(chan struct{}), resign: make(chan struct{}),
			}
			close(campaignServer.release)
			grpcServer := grpc.NewServer()
			etcdserverpb.RegisterLeaseServer(grpcServer, campaignServer)
			etcdserverpb.RegisterKVServer(grpcServer, campaignServer)
			etcdserverpb.RegisterWatchServer(grpcServer, campaignServer)
			go func() { _ = grpcServer.Serve(listener) }()
			defer grpcServer.Stop()
			cfg := util.NewTestConfig()
			cfg.Status.StatusPort = 0
			cfg.Store = config.StoreTypeTiKV
			cfg.Path = "%"
			s := NewTestServer(cfg)
			s.driver = NewTiDBDriver(&statusAutoIDStore{Storage: store, etcdAddrs: []string{listener.Addr().String()}})
			require.NoError(t, s.listenStatusHTTPServer())
			closed := make(chan struct{})
			servingDone := make(chan struct{})
			closeServer := sync.OnceFunc(func() {
				s.Close()
				close(closed)
			})
			defer func() {
				close(campaignServer.resign)
				go closeServer()
				select {
				case <-closed:
				case <-time.After(5 * time.Second):
					t.Error("status shutdown did not finish during cleanup")
				}
				select {
				case <-servingDone:
				case <-time.After(5 * time.Second):
					t.Error("status initializer did not finish during cleanup")
				}
			}()
			if initializing {
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/autoid_service/afterAutoIDCampaign", func(*autoid.Service) {
					<-s.statusInitializationContext().Done()
				})
			}
			go func() {
				s.startStatusServerAndRPCServer(http.NewServeMux())
				close(servingDone)
			}()
			select {
			case <-campaignServer.waiting:
			case <-time.After(5 * time.Second):
				t.Fatal("AutoID campaign did not wait behind the existing owner")
			}
			if !initializing {
				s.statusMu.Lock()
				service := s.autoIDService
				s.statusMu.Unlock()
				require.NotNil(t, service)
			}
			go closeServer()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown must cancel the blocked AutoID campaign")
			}
			require.True(t, isClosed(campaignServer.revoked), "shutdown must still revoke the AutoID lease")
			_, err = s.statusListener.Accept()
			require.ErrorIs(t, err, net.ErrClosed)
			require.Nil(t, s.autoIDService)
			require.Nil(t, s.statusServer.Load())
			require.Nil(t, s.grpcServer)
		})
	}

	t.Run("before initialization", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
		cfg := util.NewTestConfig()
		cfg.Status.StatusPort = 0
		cfg.Store = config.StoreTypeTiKV
		// If initialization is incorrectly attempted, fail before opening a store.
		cfg.Path = "%"
		s := NewTestServer(cfg)
		require.NoError(t, s.listenStatusHTTPServer())
		defer s.Close()
		autoIDStarted := false
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/server/beforeAutoIDService", func() {
			autoIDStarted = true
		})
		s.Close()
		s.startStatusServerAndRPCServer(http.NewServeMux())
		require.False(t, autoIDStarted, "shutdown must prevent late AutoID initialization")
		require.Nil(t, s.statusServer.Load())
		require.Nil(t, s.grpcServer)
	})

	for _, tc := range []struct {
		name       string
		diagnostic bool
	}{
		{name: "normal"},
		{name: "diagnostic", diagnostic: true},
	} {
		t.Run("startup after close/"+tc.name, func(t *testing.T) {
			t.Cleanup(diagnosticmode.SetForTest(tc.diagnostic))
			cfg := util.NewTestConfig()
			cfg.Status.StatusPort = 0
			s := NewTestServer(cfg)
			if !tc.diagnostic {
				store, dom := testkit.CreateMockStoreAndDomain(t)
				s.driver = NewTiDBDriver(store)
				s.dom = dom
			}
			defer s.Close()
			s.Close()
			require.ErrorIs(t, s.startStatusHTTP(), net.ErrClosed)
			require.Nil(t, s.statusListener)
		})
	}

	t.Run("during asynchronous initialization", func(t *testing.T) {
		ignoreCurrent := goleak.IgnoreCurrent()
		t.Cleanup(func() { goleak.VerifyNone(t, ignoreCurrent) })
		store, dom := testkit.CreateMockStoreAndDomain(t)
		cfg := util.NewTestConfig()
		cfg.Status.StatusPort = 0
		s := NewTestServer(cfg)
		s.driver = NewTiDBDriver(store)
		s.dom = dom
		initializing := make(chan struct{})
		resume := make(chan struct{})
		closed := make(chan struct{})
		closeServer := sync.OnceFunc(func() {
			s.Close()
			close(closed)
		})
		defer func() {
			if !isClosed(resume) {
				close(resume)
			}
			go closeServer()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Error("status shutdown did not finish during cleanup")
			}
		}()
		shutdownObserved := false
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/server/beforeStatusServerInit", func() {
			close(initializing)
			<-resume
			// Close must not hold the client lock while joining initialization.
			s.rwlock.Lock()
			shutdownObserved = s.inShutdownMode.Load()
			s.rwlock.Unlock()
		})
		resourcesClosed := make(chan struct{}, 1)
		s.authTokenCancelFunc = func() {
			select {
			case resourcesClosed <- struct{}{}:
			default:
			}
		}
		require.NoError(t, s.startStatusHTTP())
		select {
		case <-initializing:
		case <-time.After(5 * time.Second):
			t.Fatal("status initialization did not start")
		}
		go closeServer()
		select {
		case <-resourcesClosed:
		case <-time.After(5 * time.Second):
			t.Fatal("status resources were not closed")
		}
		select {
		case <-closed:
			t.Error("Close returned while status initialization was still running")
		case <-time.After(50 * time.Millisecond):
		}
		close(resume)
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("shutdown did not finish after initialization was released")
		}
		require.True(t, shutdownObserved)
		require.Nil(t, s.statusServer.Load())
		require.Nil(t, s.grpcServer)
	})

	t.Run("during AutoID initialization", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
		cfg := util.NewTestConfig()
		cfg.Status.StatusPort = 0
		cfg.Store = config.StoreTypeTiKV
		cfg.Path = "%"
		s := NewTestServer(cfg)
		s.health.Store(true)
		require.NoError(t, s.listenStatusHTTPServer())
		initializing := make(chan struct{})
		resume := make(chan struct{})
		closed := make(chan struct{})
		closeServer := sync.OnceFunc(func() {
			s.Close()
			close(closed)
		})
		defer func() {
			if !isClosed(resume) {
				close(resume)
			}
			go closeServer()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Error("status shutdown did not finish during cleanup")
			}
		}()
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/server/beforeAutoIDService", func() {
			close(initializing)
			<-resume
		})
		servingDone := make(chan struct{})
		go func() {
			s.startStatusServerAndRPCServer(http.NewServeMux())
			close(servingDone)
		}()
		select {
		case <-initializing:
		case <-time.After(5 * time.Second):
			t.Fatal("AutoID initialization did not start")
		}
		go closeServer()
		require.Eventually(t, func() bool { return !s.Health() }, 5*time.Second, time.Millisecond)
		select {
		case <-closed:
			t.Error("Close returned while AutoID initialization was still running")
		case <-time.After(50 * time.Millisecond):
		}
		close(resume)
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("shutdown did not finish after AutoID initialization was released")
		}
		select {
		case <-servingDone:
		case <-time.After(5 * time.Second):
			t.Fatal("status serving did not finish")
		}
		require.Nil(t, s.statusServer.Load())
		require.Nil(t, s.grpcServer)
	})

	t.Run("during Starter drain", func(t *testing.T) {
		if kerneltype.IsClassic() {
			t.Skip("only for nextgen kernel")
		}
		originalMode := deploymode.Get()
		require.NoError(t, deploymode.Set(deploymode.Starter))
		t.Cleanup(func() { require.NoError(t, deploymode.Set(originalMode)) })
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
		cfg := util.NewTestConfig()
		cfg.Status.StatusPort = 0
		cfg.Store = config.StoreTypeTiKV
		cfg.Path = "%"
		s := NewTestServer(cfg)
		require.NoError(t, s.listenStatusHTTPServer())
		autoIDClosed := make(chan struct{})
		drained := make(chan struct{})
		s.StandbyController = &statusShutdownController{onShutdown: func(server StandbyShutdownServer) {
			server.AutoIDServiceClose()
			close(autoIDClosed)
			<-drained
		}}
		autoIDStarted := false
		testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/server/beforeAutoIDService", func() {
			autoIDStarted = true
		})
		closed := make(chan struct{})
		go func() {
			s.Close()
			close(closed)
		}()
		defer func() {
			close(drained)
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Error("Starter shutdown did not finish")
			}
		}()
		select {
		case <-autoIDClosed:
		case <-time.After(5 * time.Second):
			t.Fatal("Starter did not relinquish AutoID ownership")
		}
		initialized := make(chan struct{})
		go func() {
			s.startStatusServerAndRPCServer(http.NewServeMux())
			close(initialized)
		}()
		select {
		case <-initialized:
		case <-time.After(5 * time.Second):
			t.Fatal("status initialization was not rejected while Starter was draining")
		}
		require.False(t, autoIDStarted)
		require.Nil(t, s.statusServer.Load())
		require.Nil(t, s.grpcServer)
	})

	for _, tc := range []struct {
		name   string
		tls    bool
		prefix string
	}{
		{name: "silent TCP"},
		{name: "partial HTTP", prefix: "GET"},
		{name: "silent TLS", tls: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			cfg := util.NewTestConfig()
			cfg.Status.StatusPort = 0
			s := NewTestServer(cfg)
			require.NoError(t, s.listenStatusHTTPServer())
			if tc.tls {
				fixture := httptest.NewTLSServer(http.NotFoundHandler())
				tlsConfig := fixture.TLS.Clone()
				fixture.Close()
				listener := s.statusListener.(*statusListener)
				listener.Listener = tls.NewListener(listener.Listener, tlsConfig)
			}
			reading := make(chan struct{}, 1)
			s.statusListener = &statusReadListener{Listener: s.statusListener, reading: reading}
			servingDone := make(chan struct{})
			go func() {
				s.startStatusServerAndRPCServer(http.NewServeMux())
				close(servingDone)
			}()
			defer func() {
				s.Close()
				select {
				case <-servingDone:
				case <-time.After(5 * time.Second):
					t.Error("status serving did not finish")
				}
			}()
			conn, err := net.DialTimeout("tcp", s.statusListener.Addr().String(), 5*time.Second)
			require.NoError(t, err)
			defer conn.Close()
			if tc.prefix != "" {
				_, err = conn.Write([]byte(tc.prefix))
				require.NoError(t, err)
			}
			// Wait until cmux is reading the connection, rather than merely queued in Accept.
			select {
			case <-reading:
			case <-time.After(5 * time.Second):
				t.Fatal("status connection was not read")
			}
			closed := make(chan struct{})
			go func() {
				s.Close()
				close(closed)
			}()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("status shutdown did not finish")
			}
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
			_, err = conn.Read(make([]byte, 1))
			require.Error(t, err)
			if netErr, ok := err.(net.Error); ok {
				require.False(t, netErr.Timeout(), "shutdown left the status connection open")
			}
		})
	}
}

type delayedStatusListener struct {
	net.Listener
	conn    net.Conn
	entered chan struct{}
	release chan struct{}
}

func (l *delayedStatusListener) Accept() (net.Conn, error) {
	close(l.entered)
	<-l.release
	return l.conn, nil
}

func TestStatusListenerCloseDuringAccept(t *testing.T) {
	root, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(5*time.Second)))
	inner := &delayedStatusListener{
		Listener: root,
		conn:     serverConn,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	l := newStatusListener(inner)
	defer l.Close()
	acceptDone := make(chan error, 1)
	go func() {
		conn, err := l.Accept()
		if conn != nil {
			_ = conn.Close()
		}
		acceptDone <- err
	}()
	<-inner.entered
	require.NoError(t, l.Close())
	close(inner.release)
	select {
	case err := <-acceptDone:
		require.ErrorIs(t, err, net.ErrClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("Accept did not finish after shutdown")
	}
	_, err = clientConn.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, l.Close())
}
