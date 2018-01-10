// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/juju/errors"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/gcworker"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tidb/util/systimemon"
	"github.com/pingcap/tidb/x-server"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// Flag Names
const (
	nmVersion         = "V"
	nmConfig          = "config"
	nmStore           = "store"
	nmStorePath       = "path"
	nmHost            = "host"
	nmPort            = "P"
	nmSocket          = "socket"
	nmBinlogSocket    = "binlog-socket"
	nmRunDDL          = "run-ddl"
	nmLogLevel        = "L"
	nmLogFile         = "log-file"
	nmLogSlowQuery    = "log-slow-query"
	nmReportStatus    = "report-status"
	nmStatusPort      = "status"
	nmMetricsAddr     = "metrics-addr"
	nmMetricsInterval = "metrics-interval"
	nmDdlLease        = "lease"
	nmTokenLimit      = "token-limit"

	nmProxyProtocolNetworks      = "proxy-protocol-networks"
	nmProxyProtocolHeaderTimeout = "proxy-protocol-header-timeout"
)

var (
	version    = flagBoolean(nmVersion, false, "print version information and exit")
	configPath = flag.String(nmConfig, "", "config file path")

	// Base
	store        = flag.String(nmStore, "mocktikv", "registered store name, [memory, goleveldb, boltdb, tikv, mocktikv]")
	storePath    = flag.String(nmStorePath, "/tmp/tidb", "tidb storage path")
	host         = flag.String(nmHost, "0.0.0.0", "tidb server host")
	port         = flag.String(nmPort, "4000", "tidb server port")
	socket       = flag.String(nmSocket, "", "The socket file to use for connection.")
	binlogSocket = flag.String(nmBinlogSocket, "", "socket file to write binlog")
	runDDL       = flagBoolean(nmRunDDL, true, "run ddl worker on this tidb-server")
	ddlLease     = flag.String(nmDdlLease, "10s", "schema lease duration, very dangerous to change only if you know what you do")
	tokenLimit   = flag.Int(nmTokenLimit, 1000, "the limit of concurrent executed sessions")

	// Log
	logLevel     = flag.String(nmLogLevel, "info", "log level: info, debug, warn, error, fatal")
	logFile      = flag.String(nmLogFile, "", "log file path")
	logSlowQuery = flag.String(nmLogSlowQuery, "", "slow query file path")

	// Status
	reportStatus    = flagBoolean(nmReportStatus, true, "If enable status report HTTP service.")
	statusPort      = flag.String(nmStatusPort, "10080", "tidb server status port")
	metricsAddr     = flag.String(nmMetricsAddr, "", "prometheus pushgateway address, leaves it empty will disable prometheus push.")
	metricsInterval = flag.Int(nmMetricsInterval, 15, "prometheus client push interval in second, set \"0\" to disable prometheus push.")

	// PROXY Protocol
	proxyProtocolNetworks      = flag.String(nmProxyProtocolNetworks, "", "proxy protocol networks allowed IP or *, empty mean disable proxy protocol support")
	proxyProtocolHeaderTimeout = flag.Int(nmProxyProtocolHeaderTimeout, 5, "proxy protocol header read timeout, unit is second.")

	timeJumpBackCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "monitor",
			Name:      "time_jump_back_total",
			Help:      "Counter of system time jumps backward.",
		})
)

var (
	cfg      *config.Config
	storage  kv.Storage
	dom      *domain.Domain
	svr      *server.Server
	xsvr     *xserver.Server
	graceful bool
)

func main() {
	flag.Parse()
	if *version {
		printer.PrintRawTiDBInfo()
		os.Exit(0)
	}

	registerStores()
	loadConfig()
	overrideConfig()
	validateConfig()
	setGlobalVars()
	setupLog()
	setupTracing() // Should before createServer and after setup config.
	printInfo()
	setupBinlogClient()
	createStoreAndDomain()
	createServer()
	setupSignalHandler()
	setupMetrics()
	runServer()
	cleanup()
	os.Exit(0)
}

func registerStores() {
	err := tidb.RegisterStore("tikv", tikv.Driver{})
	terror.MustNil(err)
	tikv.NewGCHandlerFunc = gcworker.NewGCWorker
	err = tidb.RegisterStore("mocktikv", tikv.MockDriver{})
	terror.MustNil(err)
}

func createStoreAndDomain() {
	fullPath := fmt.Sprintf("%s://%s", cfg.Store, cfg.Path)
	var err error
	storage, err = tidb.NewStore(fullPath)
	terror.MustNil(err)
	// Bootstrap a session to load information schema.
	dom, err = tidb.BootstrapSession(storage)
	terror.MustNil(err)
}

func setupBinlogClient() {
	if cfg.BinlogSocket == "" {
		return
	}
	dialerOpt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("unix", addr, timeout)
	})
	clientConn, err := tidb.DialPumpClientWithRetry(cfg.BinlogSocket, util.DefaultMaxRetries, dialerOpt)
	terror.MustNil(err)
	binloginfo.SetPumpClient(binlog.NewPumpClient(clientConn))
	log.Infof("created binlog client at %s", cfg.BinlogSocket)
}

// Prometheus push.
const zeroDuration = time.Duration(0)

// pushMetric pushs metircs in background.
func pushMetric(addr string, interval time.Duration) {
	if interval == zeroDuration || len(addr) == 0 {
		log.Info("disable Prometheus push client")
		return
	}
	log.Infof("start Prometheus push client with server addr %s and interval %s", addr, interval)
	go prometheusPushClient(addr, interval)
}

// prometheusPushClient pushs metrics to Prometheus Pushgateway.
func prometheusPushClient(addr string, interval time.Duration) {
	// TODO: TiDB do not have uniq name, so we use host+port to compose a name.
	job := "tidb"
	for {
		err := push.AddFromGatherer(
			job,
			map[string]string{"instance": instanceName()},
			addr,
			prometheus.DefaultGatherer,
		)
		if err != nil {
			log.Errorf("could not push metrics to Prometheus Pushgateway: %v", err)
		}
		time.Sleep(interval)
	}
}

func instanceName() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return fmt.Sprintf("%s_%d", hostname, cfg.Port)
}

// parseLease parses lease argument string.
func parseLease(lease string) time.Duration {
	dur, err := time.ParseDuration(lease)
	if err != nil {
		dur, err = time.ParseDuration(lease + "s")
	}
	if err != nil || dur < 0 {
		log.Fatalf("invalid lease duration %s", lease)
	}
	return dur
}

func hasRootPrivilege() bool {
	return os.Geteuid() == 0
}

func flagBoolean(name string, defaultVal bool, usage string) *bool {
	if defaultVal == false {
		// Fix #4125, golang do not print default false value in usage, so we append it.
		usage = fmt.Sprintf("%s (default false)", usage)
		return flag.Bool(name, defaultVal, usage)
	}
	return flag.Bool(name, defaultVal, usage)
}

func loadConfig() {
	cfg = config.GetGlobalConfig()
	if *configPath != "" {
		err := cfg.Load(*configPath)
		terror.MustNil(err)
	}
}

func overrideConfig() {
	actualFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		actualFlags[f.Name] = true
	})

	// Base
	if actualFlags[nmHost] {
		cfg.Host = *host
	}
	var err error
	if actualFlags[nmPort] {
		cfg.Port, err = strconv.Atoi(*port)
		terror.MustNil(err)
	}
	if actualFlags[nmStore] {
		cfg.Store = *store
	}
	if actualFlags[nmStorePath] {
		cfg.Path = *storePath
	}
	if actualFlags[nmSocket] {
		cfg.Socket = *socket
	}
	if actualFlags[nmBinlogSocket] {
		cfg.BinlogSocket = *binlogSocket
	}
	if actualFlags[nmRunDDL] {
		cfg.RunDDL = *runDDL
	}
	if actualFlags[nmDdlLease] {
		cfg.Lease = *ddlLease
	}
	if actualFlags[nmTokenLimit] {
		cfg.TokenLimit = *tokenLimit
	}

	// Log
	if actualFlags[nmLogLevel] {
		cfg.Log.Level = *logLevel
	}
	if actualFlags[nmLogFile] {
		cfg.Log.File.Filename = *logFile
	}
	if actualFlags[nmLogSlowQuery] {
		cfg.Log.SlowQueryFile = *logSlowQuery
	}

	// Status
	if actualFlags[nmReportStatus] {
		cfg.Status.ReportStatus = *reportStatus
	}
	if actualFlags[nmStatusPort] {
		cfg.Status.StatusPort, err = strconv.Atoi(*statusPort)
		terror.MustNil(err)
	}
	if actualFlags[nmMetricsAddr] {
		cfg.Status.MetricsAddr = *metricsAddr
	}
	if actualFlags[nmMetricsInterval] {
		cfg.Status.MetricsInterval = *metricsInterval
	}

	// PROXY Protocol
	if actualFlags[nmProxyProtocolNetworks] {
		cfg.ProxyProtocol.Networks = *proxyProtocolNetworks
	}
	if actualFlags[nmProxyProtocolHeaderTimeout] {
		cfg.ProxyProtocol.HeaderTimeout = *proxyProtocolHeaderTimeout
	}
}

func validateConfig() {
	if cfg.Security.SkipGrantTable && !hasRootPrivilege() {
		log.Error("TiDB run with skip-grant-table need root privilege.")
		os.Exit(-1)
	}
}

func setGlobalVars() {
	ddlLeaseDuration := parseLease(cfg.Lease)
	tidb.SetSchemaLease(ddlLeaseDuration)
	runtime.GOMAXPROCS(cfg.Performance.MaxProcs)
	statsLeaseDuration := parseLease(cfg.Performance.StatsLease)
	tidb.SetStatsLease(statsLeaseDuration)
	domain.RunAutoAnalyze = cfg.Performance.RunAutoAnalyze
	ddl.RunWorker = cfg.RunDDL
	ddl.EnableSplitTableRegion = cfg.SplitTable
	tidb.SetCommitRetryLimit(cfg.Performance.RetryLimit)
	plan.JoinConcurrency = cfg.Performance.JoinConcurrency
	plan.AllowCartesianProduct = cfg.Performance.CrossJoin
	privileges.SkipWithGrant = cfg.Security.SkipGrantTable

	plan.PlanCacheEnabled = cfg.PlanCache.Enabled
	if plan.PlanCacheEnabled {
		plan.PlanCacheCapacity = cfg.PlanCache.Capacity
		plan.PlanCacheShards = cfg.PlanCache.Shards
		plan.GlobalPlanCache = kvcache.NewShardedLRUCache(plan.PlanCacheCapacity, plan.PlanCacheShards)
	}

	plan.PreparedPlanCacheEnabled = cfg.PreparedPlanCache.Enabled
	if plan.PreparedPlanCacheEnabled {
		plan.PreparedPlanCacheCapacity = cfg.PreparedPlanCache.Capacity
	}

	if cfg.TiKVClient.GrpcConnectionCount > 0 {
		tikv.MaxConnectionCount = cfg.TiKVClient.GrpcConnectionCount
	}
}

func setupLog() {
	err := logutil.InitLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)
}

func printInfo() {
	// Make sure the TiDB info is always printed.
	level := log.GetLevel()
	log.SetLevel(log.InfoLevel)
	printer.PrintTiDBInfo()
	log.SetLevel(level)
}

func createServer() {
	var driver server.IDriver
	driver = server.NewTiDBDriver(storage)
	var err error
	svr, err = server.NewServer(cfg, driver)
	terror.MustNil(err)
	if cfg.XProtocol.XServer {
		xcfg := &xserver.Config{
			Addr:       fmt.Sprintf("%s:%d", cfg.XProtocol.XHost, cfg.XProtocol.XPort),
			Socket:     cfg.XProtocol.XSocket,
			TokenLimit: cfg.TokenLimit,
		}
		xsvr, err = xserver.NewServer(xcfg)
		terror.MustNil(err)
	}
}

func setupSignalHandler() {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("Got signal [%s] to exit.", sig)
		if sig == syscall.SIGTERM {
			graceful = true
		}

		if xsvr != nil {
			xsvr.Close() // Should close xserver before server.
		}
		svr.Close()
	}()
}

func setupMetrics() {
	prometheus.MustRegister(timeJumpBackCounter)
	go systimemon.StartMonitor(time.Now, func() {
		timeJumpBackCounter.Inc()
	})

	pushMetric(cfg.Status.MetricsAddr, time.Duration(cfg.Status.MetricsInterval)*time.Second)
}

func setupTracing() {
	tracingCfg := cfg.OpenTracing.ToTracingConfig()
	tracer, _, err := tracingCfg.New("TiDB")
	if err != nil {
		log.Fatal("cannot initialize Jaeger Tracer", err)
	}
	opentracing.SetGlobalTracer(tracer)
}

func runServer() {
	err := svr.Run()
	terror.MustNil(err)
	if cfg.XProtocol.XServer {
		err := xsvr.Run()
		terror.MustNil(err)
	}
}

func cleanup() {
	if graceful {
		svr.GracefulDown()
	}
	dom.Close()
	err := storage.Close()
	terror.Log(errors.Trace(err))
}
