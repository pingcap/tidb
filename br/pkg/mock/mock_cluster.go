// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package mock

import (
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/pkg/tempurl"
	"go.uber.org/zap"
)

var pprofOnce sync.Once

// Cluster is mock tidb cluster, includes tikv and pd.
type Cluster struct {
	*server.Server
	testutils.Cluster
	kv.Storage
	*server.TiDBDriver
	*domain.Domain
	DSN      string
	PDClient pd.Client
}

// NewCluster create a new mock cluster.
func NewCluster() (*Cluster, error) {
	pprofOnce.Do(func() {
		go func() {
			// Make sure pprof is registered.
			_ = pprof.Handler
			addr := "0.0.0.0:12235"
			log.Info("start pprof", zap.String("addr", addr))
			if e := http.ListenAndServe(addr, nil); e != nil {
				log.Warn("fail to start pprof", zap.String("addr", addr), zap.Error(e))
			}
		}()
	})

	var mockCluster testutils.Cluster
	storage, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			mockCluster = c
		}),
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(storage)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Cluster{
		Storage:  storage,
		Cluster:  mockCluster,
		Domain:   dom,
		PDClient: storage.(tikv.Storage).GetRegionCache().PDClient(),
	}, nil
}

// Start runs a mock cluster.
func (mock *Cluster) Start() error {
	statusURL, err := url.Parse(tempurl.Alloc())
	if err != nil {
		return errors.Trace(err)
	}
	statusPort, err := strconv.ParseInt(statusURL.Port(), 10, 32)
	if err != nil {
		return errors.Trace(err)
	}

	addrURL, err := url.Parse(tempurl.Alloc())
	if err != nil {
		return errors.Trace(err)
	}
	addrPort, err := strconv.ParseInt(addrURL.Port(), 10, 32)
	if err != nil {
		return errors.Trace(err)
	}
	_ = addrPort

	mock.TiDBDriver = server.NewTiDBDriver(mock.Storage)
	cfg := config.NewConfig()
	cfg.Port = uint(addrPort)
	cfg.Store = "tikv"
	cfg.Status.StatusPort = uint(statusPort)
	cfg.Status.ReportStatus = true

	svr, err := server.NewServer(cfg, mock.TiDBDriver)
	if err != nil {
		return errors.Trace(err)
	}
	mock.Server = svr
	go func() {
		if err1 := svr.Run(); err != nil {
			panic(err1)
		}
	}()
	mock.DSN = waitUntilServerOnline(addrURL.Host, cfg.Status.StatusPort)
	return nil
}

// Stop stops a mock cluster.
func (mock *Cluster) Stop() {
	if mock.Domain != nil {
		mock.Domain.Close()
	}
	if mock.Storage != nil {
		mock.Storage.Close()
	}
	if mock.Server != nil {
		mock.Server.Close()
	}
}

type configOverrider func(*mysql.Config)

const retryTime = 100

var defaultDSNConfig = mysql.Config{
	User: "root",
	Net:  "tcp",
	Addr: "127.0.0.1:4001",
}

// getDSN generates a DSN string for MySQL connection.
func getDSN(overriders ...configOverrider) string {
	cfg := defaultDSNConfig
	for _, overrider := range overriders {
		if overrider != nil {
			overrider(&cfg)
		}
	}
	return cfg.FormatDSN()
}

func waitUntilServerOnline(addr string, statusPort uint) string {
	// connect server
	retry := 0
	dsn := getDSN(func(cfg *mysql.Config) {
		cfg.Addr = addr
	})
	for ; retry < retryTime; retry++ {
		time.Sleep(time.Millisecond * 10)
		db, err := sql.Open("mysql", dsn)
		if err == nil {
			db.Close()
			break
		}
	}
	if retry == retryTime {
		log.Panic("failed to connect DB in every 10 ms", zap.Int("retryTime", retryTime))
	}
	// connect http status
	statusURL := fmt.Sprintf("http://127.0.0.1:%d/status", statusPort)
	for retry = 0; retry < retryTime; retry++ {
		resp, err := http.Get(statusURL) // nolint:noctx
		if err == nil {
			// Ignore errors.
			_, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if retry == retryTime {
		log.Panic("failed to connect HTTP status in every 10 ms",
			zap.Int("retryTime", retryTime))
	}
	return strings.SplitAfter(dsn, "/")[0]
}
