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

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/localstore/boltdb"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tidb/util/systimemon"
	"github.com/pingcap/tidb/x-server"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
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
	nmSlowThreshold   = "slow-threshold"
	nmQueryLogMaxLen  = "query-log-max-len"
	nmSkipGrantTable  = "skip-grant-table"
	nmSSLCA           = "ssl-ca"
	nmSSLCert         = "ssl-cert"
	nmSSLKey          = "ssl-key"
	nmReportStatus    = "report-status"
	nmStatusPort      = "status"
	nmMetricsAddr     = "metrics-addr"
	nmMetricsInterval = "metrics-interval"
	nmDdlLease        = "lease"
	nmStatsLease      = "statsLease"
	nmJoinConcurrency = "join-concurrency"
	nmCrossJoin       = "cross-join"
	nmRetryLimit      = "retry-limit"
	nmTcpKeepAlive    = "tcp-keep-alive"
	nmStartXServer    = "xserver"
	nmXHost           = "xhost"
	nmXPort           = "xP"
	nmXSocket         = "xsocket"
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

	// Log
	logLevel       = flag.String(nmLogLevel, "info", "log level: info, debug, warn, error, fatal")
	logFile        = flag.String(nmLogFile, "", "log file path")
	slowThreshold  = flag.Int(nmSlowThreshold, 300, "Queries with execution time greater than this value will be logged. (Milliseconds)")
	queryLogMaxlen = flag.Int(nmQueryLogMaxLen, 2048, "Maximum query length recorded in log")

	// Security
	skipGrantTable = flagBoolean(nmSkipGrantTable, false, "This option causes the server to start without using the privilege system at all.")
	sslCAPath      = flag.String(nmSSLCA, "", "Path of file that contains list of trusted SSL CAs")
	sslCertPath    = flag.String(nmSSLCert, "", "Path of file that contains X509 certificate in PEM format")
	sslKeyPath     = flag.String(nmSSLKey, "", "Path of file that contains X509 key in PEM format")

	// Status
	reportStatus    = flagBoolean(nmReportStatus, true, "If enable status report HTTP service.")
	statusPort      = flag.String(nmStatusPort, "10080", "tidb server status port")
	metricsAddr     = flag.String(nmMetricsAddr, "", "prometheus pushgateway address, leaves it empty will disable prometheus push.")
	metricsInterval = flag.Int(nmMetricsInterval, 15, "prometheus client push interval in second, set \"0\" to disable prometheus push.")

	// Performance
	ddlLease     = flag.String(nmDdlLease, "10s", "schema lease duration, very dangerous to change only if you know what you do")
	statsLease   = flag.String(nmStatsLease, "3s", "stats lease duration, which inflences the time of analyze and stats load.")
	joinCon      = flag.Int(nmJoinConcurrency, 5, "the number of goroutines that participate joining.")
	crossJoin    = flagBoolean(nmCrossJoin, true, "whether support cartesian product or not.")
	retryLimit   = flag.Int(nmRetryLimit, 10, "the maximum number of retries when commit a transaction")
	tcpKeepAlive = flagBoolean(nmTcpKeepAlive, false, "set keep alive option for tcp connection.")

	// XProtocol
	startXServer = flagBoolean(nmStartXServer, false, "start tidb x protocol server")
	xhost        = flag.String(nmXHost, "0.0.0.0", "tidb x protocol server host")
	xport        = flag.String(nmXPort, "14000", "tidb x protocol server port")
	xsocket      = flag.String(nmXSocket, "", "The socket file to use for x protocol connection.")

	// To be removed.
	enablePrivilege = flagBoolean("privilege", true, "If enable privilege check feature. This flag will be removed in the future.")

	timeJumpBackCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "monitor",
			Name:      "time_jump_back_total",
			Help:      "Counter of system time jumps backward.",
		})
)

var (
	cfg     *config.Config
	storage kv.Storage
	dom     *domain.Domain
	svr     *server.Server
	xsvr    *xserver.Server
)

func main() {
	flag.Parse()
	if *version {
		printer.PrintRawTiDBInfo()
		os.Exit(0)
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	registerStores()
	loadConfig()
	overrideConfig()
	validateConfig()
	setGlobalVars()
	setupLog()
	printInfo()
	createStoreAndDomain()
	setupBinlogClient()
	createServer()
	setupSignalHandler()
	setupMetrics()
	runServer()

	dom.Close()
	os.Exit(0)
}

func registerStores() {
	tidb.RegisterLocalStore("boltdb", boltdb.Driver{})
	tidb.RegisterStore("tikv", tikv.Driver{})
	tidb.RegisterStore("mocktikv", tikv.MockDriver{})
}

func createStoreAndDomain() {
	fullPath := fmt.Sprintf("%s://%s", cfg.Store, cfg.Path)
	var err error
	storage, err = tidb.NewStore(fullPath)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	// Bootstrap a session to load information schema.
	dom, err = tidb.BootstrapSession(storage)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
}

func setupBinlogClient() {
	if cfg.BinlogSocket == "" {
		return
	}
	dialerOpt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("unix", addr, timeout)
	})
	clientCon, err := grpc.Dial(cfg.BinlogSocket, dialerOpt, grpc.WithInsecure())
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	binloginfo.SetPumpClient(binlog.NewPumpClient(clientCon))
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
		if err != nil {
			log.Fatal(err)
		}
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
		if err != nil {
			log.Fatal(err)
		}
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

	// Log
	if actualFlags[nmLogLevel] {
		cfg.Log.Level = *logLevel
	}
	if actualFlags[nmLogFile] {
		cfg.Log.File.Filename = *logFile
	}
	if actualFlags[nmSlowThreshold] {
		cfg.Log.SlowThreshold = *slowThreshold
	}
	if actualFlags[nmQueryLogMaxLen] {
		cfg.Log.QueryLogMaxLen = *queryLogMaxlen
	}

	// Security
	if actualFlags[nmSkipGrantTable] {
		cfg.Security.SkipGrantTable = *skipGrantTable
	}
	if actualFlags[nmSSLCA] {
		cfg.Security.SSLCA = *sslCAPath
	}
	if actualFlags[nmSSLCert] {
		cfg.Security.SSLCert = *sslCertPath
	}
	if actualFlags[nmSSLKey] {
		cfg.Security.SSLKey = *sslKeyPath
	}

	// Status
	if actualFlags[nmReportStatus] {
		cfg.Status.ReportStatus = *reportStatus
	}
	if actualFlags[nmStatusPort] {
		cfg.Status.StatusPort, err = strconv.Atoi(*statusPort)
		if err != nil {
			log.Fatal(err)
		}
	}
	if actualFlags[nmMetricsAddr] {
		cfg.Status.MetricsAddr = *metricsAddr
	}
	if actualFlags[nmMetricsInterval] {
		cfg.Status.MetricsInterval = *metricsInterval
	}

	// Performance
	if actualFlags[nmStatsLease] {
		cfg.Performance.StatsLease = *statsLease
	}
	if actualFlags[nmJoinConcurrency] {
		cfg.Performance.JoinConcurrency = *joinCon
	}
	if actualFlags[nmCrossJoin] {
		cfg.Performance.CrossJoin = *crossJoin
	}
	if actualFlags[nmRetryLimit] {
		cfg.Performance.RetryLimit = *retryLimit
	}
	if actualFlags[nmTcpKeepAlive] {
		cfg.Performance.TCPKeepAlive = *tcpKeepAlive
	}

	// XProtocol
	if actualFlags[nmStartXServer] {
		cfg.XProtocol.XServer = *startXServer
	}
	if actualFlags[nmXHost] {
		cfg.XProtocol.XHost = *xhost
	}
	if actualFlags[nmXPort] {
		cfg.XProtocol.XPort, err = strconv.Atoi(*xport)
		if err != nil {
			log.Fatal(err)
		}
	}
	if actualFlags[nmXSocket] {
		cfg.XProtocol.XSocket = *xsocket
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
	statsLeaseDuration := parseLease(cfg.Performance.StatsLease)
	tidb.SetStatsLease(statsLeaseDuration)
	ddl.RunWorker = cfg.RunDDL
	tidb.SetCommitRetryLimit(cfg.Performance.RetryLimit)
	plan.JoinConcurrency = cfg.Performance.JoinConcurrency
	plan.AllowCartesianProduct = cfg.Performance.CrossJoin
	privileges.SkipWithGrant = cfg.Security.SkipGrantTable
}

func setupLog() {
	err := logutil.InitLogger(cfg.Log.ToLogConfig())
	if err != nil {
		log.Fatal(err)
	}
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
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	if cfg.XProtocol.XServer {
		xcfg := &xserver.Config{
			Addr:   fmt.Sprintf("%s:%d", cfg.XProtocol.XHost, cfg.XProtocol.XPort),
			Socket: cfg.XProtocol.XSocket,
		}
		xsvr, err = xserver.NewServer(xcfg)
		if err != nil {
			log.Fatal(errors.ErrorStack(err))
		}
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
		log.Infof("Got signal [%d] to exit.", sig)
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

func runServer() {
	if err := svr.Run(); err != nil {
		log.Error(err)
	}
	if cfg.XProtocol.XServer {
		if err := xsvr.Run(); err != nil {
			log.Error(err)
		}
	}
}
