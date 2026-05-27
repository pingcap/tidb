// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package standby

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/tidbmanager"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/signal"
	"go.uber.org/zap"
)

const (
	standbyState     = "standby"
	activatedState   = "activated"
	terminatingState = "terminating"

	connNormalClosed         = "normal closed"
	tidbNormalRestartLogPath = "/tmp/tidb-normal-restart.log"

	httpPathPrefix = "/tidb-pool/"

	maxCloseConnWait = time.Hour

	maxRepresentableCloseConnWaitSeconds = int64(1<<63-1) / int64(time.Second)

	managerFreeMaxAttempts   = 3
	managerFreeRetryInterval = 200 * time.Millisecond
)

var tidbExit = signal.TiDBExit

// ActivateRequest is the request body for activating the tidb server.
type ActivateRequest struct {
	KeyspaceName   string `json:"keyspace_name"`
	ExportID       string `json:"export_id"`
	MaxIdleSeconds uint   `json:"max_idle_seconds"`

	// analyze table
	RunAutoAnalyze bool `json:"run_auto_analyze"`

	// DDL
	TiDBEnableDDL bool `json:"tidb_enable_ddl"`
}

// LoadKeyspaceController controls the tidb server to be in standby mode or activated.
type LoadKeyspaceController struct {
	serverStartCh  chan struct{}
	startServerErr error
	endOnce        sync.Once
	mgrCli         tidbmanager.Client
	closeConnWait  atomic.Int64

	lastActive int64
}

// NewLoadKeyspaceController creates a new StandbyController.
// mgrCli can be nil when manager notification is disabled.
func NewLoadKeyspaceController(mgrCli tidbmanager.Client) *LoadKeyspaceController {
	return &LoadKeyspaceController{
		serverStartCh: make(chan struct{}),
		mgrCli:        mgrCli,
	}
}

var (
	mu              sync.RWMutex
	state           = standbyState
	activateRequest ActivateRequest

	// activationTimeout specifies the maximum allowed time for tidb to activate from standby mode.
	activationTimeout uint

	preTidbNormalRestartKeyspaceName, preTidbNormalRestartMsg string
)

var activateCh = make(chan struct{}, 1)

// KeyspaceMismatch is the response body when the keyspace name in http request
// does not match the local keyspace name.
type KeyspaceMismatch struct {
	Remote string `json:"remote"`
	Local  string `json:"local"`
}

type statusResponse struct {
	State        string `json:"state"`
	KeyspaceName string `json:"keyspace_name"`
	ExportID     string `json:"export_id,omitempty"`
}

type exitOptions struct {
	graceful        bool
	wait            time.Duration
	skipAutoIDOwner bool
	needMgrFree     bool
}

type invalidExitOptionError struct {
	option string
}

func (e invalidExitOptionError) Error() string {
	return "invalid " + e.option
}

func keyspaceValidateMiddleware(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		remote := r.URL.Query().Get("keyspace")
		local := config.GetGlobalKeyspaceName()
		if remote != local {
			w.WriteHeader(http.StatusPreconditionFailed)
			mismatch := KeyspaceMismatch{
				Remote: remote,
				Local:  local,
			}
			body, err := json.Marshal(mismatch)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, err = w.Write(body)
			if err != nil {
				logutil.BgLogger().Error("failed to write response", zap.Error(err))
			}
			return
		}
		next.ServeHTTP(w, r)
	}
}

func loadTiDBNormalRestartInfoAndRemove() {
	data, err := os.ReadFile(tidbNormalRestartLogPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logutil.BgLogger().Error("failed to read tidb normal restart log file", zap.Error(err))
		}
		return
	}

	parts := strings.SplitN(string(data), ":", 2)
	if len(parts) < 2 {
		logutil.BgLogger().Error("invalid tidb normal restart log file")
		return
	}

	preTidbNormalRestartKeyspaceName = parts[0]
	preTidbNormalRestartMsg = parts[1]
	logutil.BgLogger().Info("load tidb normal restart log file",
		zap.String("preTidbNormalRestartKeyspaceName", preTidbNormalRestartKeyspaceName),
		zap.String("preTidbNormalRestartMsg", preTidbNormalRestartMsg))

	if err := os.Remove(tidbNormalRestartLogPath); err != nil {
		logutil.BgLogger().Error("failed to remove tidb normal restart log file", zap.Error(err))
	}
}

func loadTiDBNormalRestartLog() ([]byte, error) {
	return os.ReadFile(tidbNormalRestartLogPath)
}

// SaveTidbNormalRestartInfo saves tidb normal restart info to file.
func SaveTidbNormalRestartInfo(msg string) {
	keyspaceName := keyspace.GetKeyspaceNameBySettings()
	if keyspaceName == "" {
		return
	}

	if err := os.WriteFile(tidbNormalRestartLogPath, []byte(keyspaceName+":"+msg), 0644); err != nil {
		logutil.BgLogger().Error("failed to write tidb normal restart log file", zap.Error(err))
	}
}

// IsPreTidbNormalRestart returns whether tidb is restarted normally before.
func IsPreTidbNormalRestart(keyspaceName string) (bool, string) {
	if keyspaceName == "" || preTidbNormalRestartKeyspaceName != keyspaceName {
		return false, ""
	}

	return true, preTidbNormalRestartMsg
}

// Handler returns a handler to query tidb pool status or activate or exit the tidb server.
func (c *LoadKeyspaceController) Handler(svr *server.Server) (string, *http.ServeMux) {
	mux := http.NewServeMux()
	mux.HandleFunc(httpPathPrefix+"status", statusHandler)
	mux.HandleFunc(httpPathPrefix+"activate", func(w http.ResponseWriter, r *http.Request) {
		var req ActivateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.KeyspaceName == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		mu.Lock()
		switch {
		case state == standbyState:
			state = activatedState
			activateRequest = req
			activateCh <- struct{}{}
		case deploymode.IsStarter() && state == terminatingState:
			mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			_, err := w.Write([]byte("server is going to shutdown"))
			if err != nil {
				logutil.BgLogger().Warn("failed to write response", zap.Error(err))
			}
			return
		case svr != nil && !svr.Health():
			mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			_, err := w.Write([]byte("server is going to shutdown"))
			if err != nil {
				logutil.BgLogger().Error("failed to write response", zap.Error(err))
			}
			return
		case activateRequest.KeyspaceName != req.KeyspaceName:
			mu.Unlock()
			w.WriteHeader(http.StatusPreconditionFailed)
			_, err := w.Write([]byte("server is not in standby mode"))
			if err != nil {
				logutil.BgLogger().Error("failed to write response", zap.Error(err))
			}
			return
		}
		// if client tries to activate with same keyspace name, wait for ready signal and return 200.
		mu.Unlock()

		var timeout <-chan time.Time
		if activationTimeout > 0 {
			timeout = time.After(time.Duration(activationTimeout) * time.Second)
		}

		select {
		case <-r.Context().Done(): // client closed connection.
			go func() {
				c.EndStandby(errors.New("client closed connection"))
				tidbExit(syscall.SIGTERM)
			}()
		case <-timeout: // reach hardlimit timeout from config.
			logutil.BgLogger().Warn("timeout waiting for activation")
			w.WriteHeader(http.StatusRequestTimeout)
			_, err := w.Write([]byte("timeout waiting for activation"))
			if err != nil {
				logutil.BgLogger().Error("failed to write response", zap.Error(err))
			}
			go func() {
				c.EndStandby(errors.New("timeout waiting for activation"))
				tidbExit(syscall.SIGTERM)
			}()
		case <-c.serverStartCh:
			if c.startServerErr != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_, err := w.Write([]byte(c.startServerErr.Error()))
				if err != nil {
					logutil.BgLogger().Error("failed to write response", zap.Error(err))
				}
				return
			}
			statusHandler(w, r)
		}
	})
	// Terminate the tidb server by sending a request. For example, in a cloud environment, we may need to delete pod to free up resources.
	mux.HandleFunc(httpPathPrefix+"exit", keyspaceValidateMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		options, err := parseExitOptions(r.URL.Query())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		logutil.BgLogger().Info("receiving exit request",
			zap.Bool("graceful", options.graceful),
			zap.Duration("wait", options.wait),
			zap.Bool("skip_auto_id_owner", options.skipAutoIDOwner),
			zap.Bool("need_mgr_free", options.needMgrFree),
		)
		if svr != nil {
			if deploymode.IsStarter() {
				if options.needMgrFree && c.mgrCli == nil {
					http.Error(w, "manager notifier is unavailable", http.StatusServiceUnavailable)
					return
				}
				if options.skipAutoIDOwner && svr.IsAutoIDOwner() {
					logutil.BgLogger().Info("auto id service is owner, skip exit")
					w.WriteHeader(http.StatusNotModified)
					_, err := w.Write([]byte("auto id service is owner"))
					if err != nil {
						logutil.BgLogger().Warn("failed to write response", zap.Error(err))
					}
					return
				}
				if !options.graceful {
					svr.SetForceShutdown()
					SaveTidbNormalRestartInfo("received force exit request")
					w.WriteHeader(http.StatusOK)
					// Consider the server is going to force shutdown, send a high priority signal to kill tidb.
					tidbExit(syscall.SIGINT)
					return
				}
				c.setCloseConnWait(options.wait)
				if options.needMgrFree {
					svr.SetNeedRequestMgrFree()
				}
			}
			SaveTidbNormalRestartInfo("received exit request")
		}
		w.WriteHeader(http.StatusOK)
		if deploymode.IsStarter() {
			tidbExit(syscall.SIGTERM)
			return
		}
		// Consider the server is going to force shutdown, send a high priority signal to kill tidb.
		tidbExit(syscall.SIGINT)
	})))
	mux.HandleFunc(httpPathPrefix+"checkconn", func(w http.ResponseWriter, r *http.Request) {
		keyspaceName, connID := r.URL.Query().Get("keyspace_name"), r.URL.Query().Get("conn_id")
		if keyspaceName == "" || connID == "" {
			w.WriteHeader(http.StatusBadRequest)
			_, err := w.Write([]byte("keyspace_name or conn_id is empty"))
			if err != nil {
				logutil.BgLogger().Error("failed to write response", zap.Error(err))
			}
			return
		}
		logger := logutil.BgLogger().With(zap.String("keyspace_name", keyspaceName), zap.String("conn_id", connID))
		logger.Info("check connection")
		if svr != nil {
			if msg := svr.GetNormalClosedConn(keyspaceName, connID); msg != "" {
				logger.Info("connection is normal closed", zap.String("msg", msg))
				_, err := w.Write([]byte(connNormalClosed))
				if err != nil {
					logutil.BgLogger().Error("failed to write response", zap.Error(err))
				}
				return
			}
		}
		if ok, msg := IsPreTidbNormalRestart(keyspaceName); ok {
			logger.Info("connection is normal closed", zap.String("msg", msg))
			_, err := w.Write([]byte(connNormalClosed))
			if err != nil {
				logutil.BgLogger().Error("failed to write response", zap.Error(err))
			}
			return
		}
		logger.Info("connection is unconfirmed")
		_, err := w.Write([]byte(`unconfirmed`))
		if err != nil {
			logutil.BgLogger().Error("failed to write response", zap.Error(err))
		}
	})
	return httpPathPrefix, mux
}

func parseExitOptions(query url.Values) (exitOptions, error) {
	graceful, err := parseExitBool(query.Get("graceful"), "graceful")
	if err != nil {
		return exitOptions{}, err
	}
	skipAutoIDOwner, err := parseExitBool(query.Get("skip_auto_id_owner"), "skip_auto_id_owner")
	if err != nil {
		return exitOptions{}, err
	}
	needMgrFree, err := parseExitBool(query.Get("need_mgr_free"), "need_mgr_free")
	if err != nil {
		return exitOptions{}, err
	}
	wait, err := parseExitWait(query.Get("wait"))
	if err != nil {
		return exitOptions{}, err
	}
	return exitOptions{
		graceful:        graceful,
		wait:            wait,
		skipAutoIDOwner: skipAutoIDOwner,
		needMgrFree:     needMgrFree,
	}, nil
}

func parseExitBool(value, name string) (bool, error) {
	if value == "" {
		return false, nil
	}
	ret, err := strconv.ParseBool(value)
	if err != nil {
		return false, invalidExitOptionError{option: name}
	}
	return ret, nil
}

func parseExitWait(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	waitSeconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || waitSeconds < 0 {
		return 0, invalidExitOptionError{option: "wait"}
	}
	if waitSeconds == 0 {
		return 0, nil
	}
	if waitSeconds > maxRepresentableCloseConnWaitSeconds || waitSeconds > int64(maxCloseConnWait/time.Second) {
		return 0, invalidExitOptionError{option: "wait"}
	}
	return time.Duration(waitSeconds) * time.Second, nil
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	mu.RLock()
	defer mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	resp := statusResponse{
		State:        state,
		KeyspaceName: activateRequest.KeyspaceName,
	}
	if deploymode.IsStarter() && activateRequest.ExportID != "" {
		resp.ExportID = activateRequest.ExportID
	}
	w.WriteHeader(http.StatusOK)
	err := json.NewEncoder(w).Encode(resp)
	if err != nil {
		logutil.BgLogger().Error("failed to write response", zap.Error(err))
	}
}

func (c *LoadKeyspaceController) setCloseConnWait(wait time.Duration) {
	c.closeConnWait.Store(int64(wait))
}

func (c *LoadKeyspaceController) getCloseConnWait() time.Duration {
	return time.Duration(c.closeConnWait.Load())
}

var httpServer *http.Server

// WaitForActivate starts a http server to listen and wait for activation signal.
func (c *LoadKeyspaceController) WaitForActivate() {
	host := config.GetGlobalConfig().Status.StatusHost
	port := config.GetGlobalConfig().Status.StatusPort
	timeout := config.GetGlobalConfig().Standby.ActivationTimeout

	_, mux := c.Handler(nil)
	// handle liveness probe.
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	// handle health
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(`{"status":"standby"}`))
		if err != nil {
			logutil.BgLogger().Error("failed to write response", zap.Error(err))
		}
	})
	httpServer = &http.Server{
		Handler: mux,
	}
	activationTimeout = timeout
	loadTiDBNormalRestartInfoAndRemove()
	logutil.BgLogger().Info("tidb-server is now running as standby, waiting for activation...", zap.String("addr", httpServer.Addr))
	go func() {
		addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
		l, err := net.Listen("tcp", addr)
		if err != nil {
			logutil.BgLogger().Warn("failed to listen", zap.Error(err))
			os.Exit(1)
		}
		clusterSecurity := config.GetGlobalConfig().Security.ClusterSecurity()
		tlsConfig, err := clusterSecurity.ToTLSConfig()
		if err != nil {
			logutil.BgLogger().Warn("failed to get tls config", zap.Error(err))
			os.Exit(1)
		}
		if tlsConfig != nil {
			l = tls.NewListener(l, tlsConfig)
		}
		if err := httpServer.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logutil.BgLogger().Warn("failed to start tidb-server as standby", zap.Error(err))
			os.Exit(1)
		}
	}()

	<-activateCh

	logutil.BgLogger().Info("standby receive activate request", zap.Any("activate-request", activateRequest))

	config.UpdateGlobal(func(c *config.Config) {
		c.KeyspaceName = activateRequest.KeyspaceName
		if deploymode.IsStarter() && activateRequest.ExportID != "" {
			c.Standby.ExportID = activateRequest.ExportID
		}
		if activateRequest.MaxIdleSeconds > 0 {
			c.Standby.MaxIdleSeconds = activateRequest.MaxIdleSeconds
		}

		// DDL config
		if activateRequest.TiDBEnableDDL {
			c.Instance.TiDBEnableDDL = *config.NewAtomicBool(activateRequest.TiDBEnableDDL)
		}

		// ananlyze table
		if activateRequest.RunAutoAnalyze {
			c.Performance.RunAutoAnalyze = activateRequest.RunAutoAnalyze
		}
	})
}

// EndStandby is used to notify the temp http server that the tidb server is ready or failed to init.
func (c *LoadKeyspaceController) EndStandby(err error) {
	c.endOnce.Do(func() {
		c.startServerErr = err
		close(c.serverStartCh)
		if httpServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := httpServer.Shutdown(ctx)
			if err != nil {
				logutil.BgLogger().Error("failed to shutdown standby http server", zap.Error(err))
			}
		}
	})
}

// OnServerShutdown is called when the server is going to shut down.
func (c *LoadKeyspaceController) OnServerShutdown(svr server.StandbyShutdownServer) {
	if !deploymode.IsStarter() {
		return
	}

	mu.Lock()
	state = terminatingState
	mu.Unlock()

	// Give up auto ID ownership before waiting for TiProxy to migrate traffic.
	svr.AutoIDServiceClose()

	if svr.GetNeedRequestMgrFree() {
		exitReason, err := loadTiDBNormalRestartLog()
		if err != nil && !os.IsNotExist(err) {
			exitReason = []byte(fmt.Sprintf("failed to load normal restart log: %v", err))
			logutil.BgLogger().Warn("failed to load tidb normal restart log", zap.ByteString("exitReason", exitReason))
		}
		c.reportManagerFree(string(exitReason))
	}

	if svr.GetForceShutdown() {
		return
	}

	maxWaitTime := c.getCloseConnWait()
	if maxWaitTime <= 0 {
		return
	}

	logutil.BgLogger().Info("waiting for tiproxy to migrate and close all connections", zap.Duration("maxWaitTime", maxWaitTime))
	done := make(chan struct{}, 1)
	go func() {
		svr.WaitZeroConn()
		done <- struct{}{}
	}()
	select {
	case <-time.After(maxWaitTime):
		logutil.BgLogger().Info("tiproxy connection close timed out")
	case <-done:
		logutil.BgLogger().Info("tiproxy has closed all connections")
	}
}

func (c *LoadKeyspaceController) reportManagerFree(exitReason string) {
	if c.mgrCli == nil {
		logutil.BgLogger().Warn("manager notifier is unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), tidbmanager.DefaultTimeout)
	defer cancel()
	var lastErr error
	for attempt := 1; attempt <= managerFreeMaxAttempts; attempt++ {
		if err := c.mgrCli.Free(ctx, exitReason); err != nil {
			lastErr = err
			logutil.BgLogger().Warn("failed to report free",
				zap.Int("attempt", attempt),
				zap.Int("maxAttempts", managerFreeMaxAttempts),
				zap.Error(err))
			if attempt < managerFreeMaxAttempts {
				select {
				case <-ctx.Done():
					logutil.BgLogger().Warn("manager free report timed out", zap.Error(ctx.Err()))
					return
				case <-time.After(managerFreeRetryInterval):
				}
			}
			continue
		}
		if attempt > 1 {
			logutil.BgLogger().Info("reported free after retry", zap.Int("attempt", attempt))
		}
		return
	}
	logutil.BgLogger().Warn("failed to report free after retries", zap.Error(lastErr))
}
