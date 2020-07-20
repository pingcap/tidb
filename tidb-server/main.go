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
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	pd "github.com/pingcap/pd/v3/client"
	pumpcli "github.com/pingcap/tidb-tools/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	kvstore "github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/gcworker"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tidb/util/signal"
	"github.com/pingcap/tidb/util/storeutil"
	"github.com/pingcap/tidb/util/systimemon"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/struCoder/pidusage"
	"go.uber.org/zap"
)

// Flag Names
const (
	nmVersion                = "V"
	nmConfig                 = "config"
	nmConfigCheck            = "config-check"
	nmConfigStrict           = "config-strict"
	nmStore                  = "store"
	nmStorePath              = "path"
	nmHost                   = "host"
	nmAdvertiseAddress       = "advertise-address"
	nmPort                   = "P"
	nmCors                   = "cors"
	nmSocket                 = "socket"
	nmEnableBinlog           = "enable-binlog"
	nmRunDDL                 = "run-ddl"
	nmLogLevel               = "L"
	nmLogFile                = "log-file"
	nmLogSlowQuery           = "log-slow-query"
	nmReportStatus           = "report-status"
	nmStatusHost             = "status-host"
	nmStatusPort             = "status"
	nmMetricsAddr            = "metrics-addr"
	nmMetricsInterval        = "metrics-interval"
	nmDdlLease               = "lease"
	nmTokenLimit             = "token-limit"
	nmPluginDir              = "plugin-dir"
	nmPluginLoad             = "plugin-load"
	nmRequireSecureTransport = "require-secure-transport"

	nmProxyProtocolNetworks      = "proxy-protocol-networks"
	nmProxyProtocolHeaderTimeout = "proxy-protocol-header-timeout"
)

var (
	version      = flagBoolean(nmVersion, false, "print version information and exit")
	configPath   = flag.String(nmConfig, "", "config file path")
	configCheck  = flagBoolean(nmConfigCheck, false, "check config file validity and exit")
	configStrict = flagBoolean(nmConfigStrict, false, "enforce config file validity")

	// Base
	store            = flag.String(nmStore, "mocktikv", "registered store name, [tikv, mocktikv]")
	storePath        = flag.String(nmStorePath, "/tmp/tidb", "tidb storage path")
	host             = flag.String(nmHost, "0.0.0.0", "tidb server host")
	advertiseAddress = flag.String(nmAdvertiseAddress, "", "tidb server advertise IP")
	port             = flag.String(nmPort, "4000", "tidb server port")
	cors             = flag.String(nmCors, "", "tidb server allow cors origin")
	socket           = flag.String(nmSocket, "", "The socket file to use for connection.")
	enableBinlog     = flagBoolean(nmEnableBinlog, false, "enable generate binlog")
	runDDL           = flagBoolean(nmRunDDL, true, "run ddl worker on this tidb-server")
	ddlLease         = flag.String(nmDdlLease, "45s", "schema lease duration, very dangerous to change only if you know what you do")
	tokenLimit       = flag.Int(nmTokenLimit, 1000, "the limit of concurrent executed sessions")
	pluginDir        = flag.String(nmPluginDir, "/data/deploy/plugin", "the folder that hold plugin")
	pluginLoad       = flag.String(nmPluginLoad, "", "wait load plugin name(separated by comma)")
	requireTLS       = flag.Bool(nmRequireSecureTransport, false, "require client use secure transport")

	// Log
	logLevel     = flag.String(nmLogLevel, "info", "log level: info, debug, warn, error, fatal")
	logFile      = flag.String(nmLogFile, "", "log file path")
	logSlowQuery = flag.String(nmLogSlowQuery, "", "slow query file path")

	// Status
	reportStatus    = flagBoolean(nmReportStatus, true, "If enable status report HTTP service.")
	statusHost      = flag.String(nmStatusHost, "0.0.0.0", "tidb server status host")
	statusPort      = flag.String(nmStatusPort, "10080", "tidb server status port")
	metricsAddr     = flag.String(nmMetricsAddr, "", "prometheus pushgateway address, leaves it empty will disable prometheus push.")
	metricsInterval = flag.Uint(nmMetricsInterval, 15, "prometheus client push interval in second, set \"0\" to disable prometheus push.")

	// PROXY Protocol
	proxyProtocolNetworks      = flag.String(nmProxyProtocolNetworks, "", "proxy protocol networks allowed IP or *, empty mean disable proxy protocol support")
	proxyProtocolHeaderTimeout = flag.Uint(nmProxyProtocolHeaderTimeout, 5, "proxy protocol header read timeout, unit is second.")
)

var (
	cfg      *config.Config
	storage  kv.Storage
	dom      *domain.Domain
	svr      *server.Server
	graceful bool
)

func main() {
	flag.Parse()
	if *version {
		fmt.Println(printer.GetTiDBInfo())
		os.Exit(0)
	}
	registerStores()
	registerMetrics()
	configWarning := loadConfig()
	overrideConfig()
	if err := cfg.Valid(); err != nil {
		fmt.Fprintln(os.Stderr, "invalid config", err)
		os.Exit(1)
	}
	if *configCheck {
		fmt.Println("config check successful")
		os.Exit(0)
	}
	setGlobalVars()
	setupLog()
	// If configStrict had been specified, and there had been an error, the server would already
	// have exited by now. If configWarning is not an empty string, write it to the log now that
	// it's been properly set up.
	if configWarning != "" {
		log.Warn(configWarning)
	}
	setupTracing() // Should before createServer and after setup config.
	printInfo()
	setupBinlogClient()
	setupMetrics()
	createStoreAndDomain()
	createServer()
	signal.SetupSignalHandler(serverShutdown)
	runServer()
	cleanup()
	syncLog()
}

func exit() {
	syncLog()
	os.Exit(0)
}

func syncLog() {
	if err := log.Sync(); err != nil {
		fmt.Fprintln(os.Stderr, "sync log err:", err)
		os.Exit(1)
	}
}

func registerStores() {
	err := kvstore.Register("tikv", tikv.Driver{})
	terror.MustNil(err)
	tikv.NewGCHandlerFunc = gcworker.NewGCWorker
	err = kvstore.Register("mocktikv", mockstore.MockDriver{})
	terror.MustNil(err)
}

func registerMetrics() {
	metrics.RegisterMetrics()
}

func createStoreAndDomain() {
	fullPath := fmt.Sprintf("%s://%s", cfg.Store, cfg.Path)
	var err error
	storage, err = kvstore.New(fullPath)
	terror.MustNil(err)
	// Bootstrap a session to load information schema.
	dom, err = session.BootstrapSession(storage)
	terror.MustNil(err)
}

func setupBinlogClient() {
	if !cfg.Binlog.Enable {
		return
	}

	if cfg.Binlog.IgnoreError {
		binloginfo.SetIgnoreError(true)
	}

	var (
		client *pumpcli.PumpsClient
		err    error
	)

	securityOption := pd.SecurityOption{
		CAPath:   cfg.Security.ClusterSSLCA,
		CertPath: cfg.Security.ClusterSSLCert,
		KeyPath:  cfg.Security.ClusterSSLKey,
	}

	if len(cfg.Binlog.BinlogSocket) == 0 {
		client, err = pumpcli.NewPumpsClient(cfg.Path, cfg.Binlog.Strategy, parseDuration(cfg.Binlog.WriteTimeout), securityOption)
	} else {
		client, err = pumpcli.NewLocalPumpsClient(cfg.Path, cfg.Binlog.BinlogSocket, parseDuration(cfg.Binlog.WriteTimeout), securityOption)
	}

	terror.MustNil(err)

	err = pumpcli.InitLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)

	binloginfo.SetPumpsClient(client)
	log.Info("tidb-server", zap.Bool("create pumps client success, ignore binlog error", cfg.Binlog.IgnoreError))
}

// Prometheus push.
const zeroDuration = time.Duration(0)

// pushMetric pushes metrics in background.
func pushMetric(addr string, interval time.Duration) {
	if interval == zeroDuration || len(addr) == 0 {
		log.Info("disable Prometheus push client")
		return
	}
	log.Info("start prometheus push client", zap.String("server addr", addr), zap.String("interval", interval.String()))
	go prometheusPushClient(addr, interval)
}

// prometheusPushClient pushes metrics to Prometheus Pushgateway.
func prometheusPushClient(addr string, interval time.Duration) {
	// TODO: TiDB do not have uniq name, so we use host+port to compose a name.
	job := "tidb"
	pusher := push.New(addr, job)
	pusher = pusher.Gatherer(prometheus.DefaultGatherer)
	pusher = pusher.Grouping("instance", instanceName())
	for {
		err := pusher.Push()
		if err != nil {
			log.Error("could not push metrics to prometheus pushgateway", zap.String("err", err.Error()))
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

// parseDuration parses lease argument string.
func parseDuration(lease string) time.Duration {
	dur, err := time.ParseDuration(lease)
	if err != nil {
		dur, err = time.ParseDuration(lease + "s")
	}
	if err != nil || dur < 0 {
		log.Fatal("invalid lease duration", zap.String("lease", lease))
	}
	return dur
}

func flagBoolean(name string, defaultVal bool, usage string) *bool {
	if !defaultVal {
		// Fix #4125, golang do not print default false value in usage, so we append it.
		usage = fmt.Sprintf("%s (default false)", usage)
		return flag.Bool(name, defaultVal, usage)
	}
	return flag.Bool(name, defaultVal, usage)
}

var deprecatedConfig = map[string]struct{}{
	"pessimistic-txn.ttl": {},
	"log.file.log-rotate": {},
}

func isDeprecatedConfigItem(items []string) bool {
	for _, item := range items {
		if _, ok := deprecatedConfig[item]; !ok {
			return false
		}
	}
	return true
}

func loadConfig() string {
	cfg = config.GetGlobalConfig()
	if *configPath != "" {
		// Not all config items are supported now.
		config.SetConfReloader(*configPath, reloadConfig, hotReloadConfigItems...)

		err := cfg.Load(*configPath)
		if err == nil {
			return ""
		}

		// Unused config item erro turns to warnings.
		if tmp, ok := err.(*config.ErrConfigValidationFailed); ok {
			if isDeprecatedConfigItem(tmp.UndecodedItems) {
				return err.Error()
			}
			// This block is to accommodate an interim situation where strict config checking
			// is not the default behavior of TiDB. The warning message must be deferred until
			// logging has been set up. After strict config checking is the default behavior,
			// This should all be removed.
			if !*configCheck && !*configStrict {
				return err.Error()
			}
		}

		terror.MustNil(err)
	} else {
		// configCheck should have the config file specified.
		if *configCheck {
			fmt.Fprintln(os.Stderr, "config check failed", errors.New("no config file specified for config-check"))
			os.Exit(1)
		}
	}
	return ""
}

// hotReloadConfigItems lists all config items which support hot-reload.
var hotReloadConfigItems = []string{"Performance.MaxProcs", "Performance.MaxMemory", "Performance.CrossJoin",
	"Performance.FeedbackProbability", "Performance.QueryFeedbackLimit", "Performance.PseudoEstimateRatio",
	"OOMAction", "MemQuotaQuery", "StmtSummary.MaxStmtCount", "StmtSummary.MaxSQLLength", "TiKVClient.StoreLimit"}

func reloadConfig(nc, c *config.Config) {
	// Just a part of config items need to be reload explicitly.
	// Some of them like OOMAction are always used by getting from global config directly
	// like config.GetGlobalConfig().OOMAction.
	// These config items will become available naturally after the global config pointer
	// is updated in function ReloadGlobalConfig.
	if nc.Performance.MaxProcs != c.Performance.MaxProcs {
		runtime.GOMAXPROCS(int(nc.Performance.MaxProcs))
	}
	if nc.Performance.MaxMemory != c.Performance.MaxMemory {
		plannercore.PreparedPlanCacheMaxMemory.Store(nc.Performance.MaxMemory)
	}
	if nc.Performance.CrossJoin != c.Performance.CrossJoin {
		plannercore.AllowCartesianProduct.Store(nc.Performance.CrossJoin)
	}
	if nc.Performance.FeedbackProbability != c.Performance.FeedbackProbability {
		statistics.FeedbackProbability.Store(nc.Performance.FeedbackProbability)
	}
	if nc.Performance.QueryFeedbackLimit != c.Performance.QueryFeedbackLimit {
		handle.MaxQueryFeedbackCount.Store(int64(nc.Performance.QueryFeedbackLimit))
	}
	if nc.Performance.PseudoEstimateRatio != c.Performance.PseudoEstimateRatio {
		statistics.RatioOfPseudoEstimate.Store(nc.Performance.PseudoEstimateRatio)
	}
	if nc.TiKVClient.StoreLimit != c.TiKVClient.StoreLimit {
		storeutil.StoreLimit.Store(nc.TiKVClient.StoreLimit)
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
	if actualFlags[nmAdvertiseAddress] {
		cfg.AdvertiseAddress = *advertiseAddress
	}
	var err error
	if actualFlags[nmPort] {
		var p int
		p, err = strconv.Atoi(*port)
		terror.MustNil(err)
		cfg.Port = uint(p)
	}
	if actualFlags[nmCors] {
		fmt.Println(cors)
		cfg.Cors = *cors
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
	if actualFlags[nmEnableBinlog] {
		cfg.Binlog.Enable = *enableBinlog
	}
	if actualFlags[nmRunDDL] {
		cfg.RunDDL = *runDDL
	}
	if actualFlags[nmDdlLease] {
		cfg.Lease = *ddlLease
	}
	if actualFlags[nmTokenLimit] {
		cfg.TokenLimit = uint(*tokenLimit)
	}
	if actualFlags[nmPluginLoad] {
		cfg.Plugin.Load = *pluginLoad
	}
	if actualFlags[nmPluginDir] {
		cfg.Plugin.Dir = *pluginDir
	}
	if actualFlags[nmRequireSecureTransport] {
		cfg.Security.RequireSecureTransport = *requireTLS
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
	if actualFlags[nmStatusHost] {
		cfg.Status.StatusHost = *statusHost
	}
	if actualFlags[nmStatusPort] {
		var p int
		p, err = strconv.Atoi(*statusPort)
		terror.MustNil(err)
		cfg.Status.StatusPort = uint(p)
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

func setGlobalVars() {
	ddlLeaseDuration := parseDuration(cfg.Lease)
	session.SetSchemaLease(ddlLeaseDuration)
	runtime.GOMAXPROCS(int(cfg.Performance.MaxProcs))
	statsLeaseDuration := parseDuration(cfg.Performance.StatsLease)
	session.SetStatsLease(statsLeaseDuration)
	bindinfo.Lease = parseDuration(cfg.Performance.BindInfoLease)
	domain.RunAutoAnalyze = cfg.Performance.RunAutoAnalyze
	statistics.FeedbackProbability.Store(cfg.Performance.FeedbackProbability)
	handle.MaxQueryFeedbackCount.Store(int64(cfg.Performance.QueryFeedbackLimit))
	statistics.RatioOfPseudoEstimate.Store(cfg.Performance.PseudoEstimateRatio)
	ddl.RunWorker = cfg.RunDDL
	if cfg.SplitTable {
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	}
	plannercore.AllowCartesianProduct.Store(cfg.Performance.CrossJoin)
	privileges.SkipWithGrant = cfg.Security.SkipGrantTable
	kv.TxnEntryCountLimit = cfg.Performance.TxnEntryCountLimit
	kv.TxnTotalSizeLimit = cfg.Performance.TxnTotalSizeLimit

	priority := mysql.Str2Priority(cfg.Performance.ForcePriority)
	variable.ForcePriority = int32(priority)
	variable.SysVars[variable.TiDBForcePriority].Value = mysql.Priority2Str[priority]

	variable.SysVars[variable.TIDBMemQuotaQuery].Value = strconv.FormatInt(cfg.MemQuotaQuery, 10)
	variable.SysVars["lower_case_table_names"].Value = strconv.Itoa(cfg.LowerCaseTableNames)
	variable.SysVars[variable.LogBin].Value = variable.BoolToIntStr(config.GetGlobalConfig().Binlog.Enable)

	variable.SysVars[variable.Port].Value = fmt.Sprintf("%d", cfg.Port)
	variable.SysVars[variable.Socket].Value = cfg.Socket
	variable.SysVars[variable.DataDir].Value = cfg.Path
	variable.SysVars[variable.TiDBSlowQueryFile].Value = cfg.Log.SlowQueryFile
	variable.SysVars[variable.TiDBIsolationReadEngines].Value = strings.Join(cfg.IsolationRead.Engines, ", ")

	// For CI environment we default enable prepare-plan-cache.
	plannercore.SetPreparedPlanCache(config.CheckTableBeforeDrop || cfg.PreparedPlanCache.Enabled)
	if plannercore.PreparedPlanCacheEnabled() {
		plannercore.PreparedPlanCacheCapacity = cfg.PreparedPlanCache.Capacity
		plannercore.PreparedPlanCacheMemoryGuardRatio = cfg.PreparedPlanCache.MemoryGuardRatio
		if plannercore.PreparedPlanCacheMemoryGuardRatio < 0.0 || plannercore.PreparedPlanCacheMemoryGuardRatio > 1.0 {
			plannercore.PreparedPlanCacheMemoryGuardRatio = 0.1
		}
		plannercore.PreparedPlanCacheMaxMemory.Store(cfg.Performance.MaxMemory)
		total, err := memory.MemTotal()
		terror.MustNil(err)
		if plannercore.PreparedPlanCacheMaxMemory.Load() > total || plannercore.PreparedPlanCacheMaxMemory.Load() <= 0 {
			plannercore.PreparedPlanCacheMaxMemory.Store(total)
		}
	}

	tikv.CommitMaxBackoff = int(parseDuration(cfg.TiKVClient.CommitTimeout).Seconds() * 1000)
	tikv.RegionCacheTTLSec = int64(cfg.TiKVClient.RegionCacheTTL)
}

func setupLog() {
	err := logutil.InitZapLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)

	err = logutil.InitLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)
	// trigger internal http(s) client init.
	util.InternalHTTPClient()
}

func printInfo() {
	// Make sure the TiDB info is always printed.
	level := log.GetLevel()
	log.SetLevel(zap.InfoLevel)
	printer.PrintTiDBInfo()
	log.SetLevel(level)
}

func createServer() {
	driver := server.NewTiDBDriver(storage)
	var err error
	svr, err = server.NewServer(cfg, driver)
	// Both domain and storage have started, so we have to clean them before exiting.
	terror.MustNil(err, closeDomainAndStorage)
	go dom.ExpensiveQueryHandle().SetSessionManager(svr).Run()
}

func serverShutdown(isgraceful bool) {
	if isgraceful {
		graceful = true
	}
	svr.Close()
}

func setupMetrics() {
	// Enable the mutex profile, 1/10 of mutex blocking event sampling.
	runtime.SetMutexProfileFraction(10)
	systimeErrHandler := func() {
		metrics.TimeJumpBackCounter.Inc()
	}
	callBackCount := 0
	sucessCallBack := func() {
		callBackCount++
		// It is callback by monitor per second, we increase metrics.KeepAliveCounter per 5s.
		if callBackCount >= 5 {
			callBackCount = 0
			metrics.KeepAliveCounter.Inc()
			updateCPUUsageMetrics()
		}
	}
	go systimemon.StartMonitor(time.Now, systimeErrHandler, sucessCallBack)

	pushMetric(cfg.Status.MetricsAddr, time.Duration(cfg.Status.MetricsInterval)*time.Second)
}

func updateCPUUsageMetrics() {
	sysInfo, err := pidusage.GetStat(os.Getpid())
	if err != nil {
		return
	}
	metrics.CPUUsagePercentageGauge.Set(sysInfo.CPU)
}

func setupTracing() {
	tracingCfg := cfg.OpenTracing.ToTracingConfig()
	tracer, _, err := tracingCfg.New("TiDB")
	if err != nil {
		log.Fatal("setup jaeger tracer failed", zap.String("error message", err.Error()))
	}
	opentracing.SetGlobalTracer(tracer)
}

func runServer() {
	err := svr.Run()
	terror.MustNil(err)
}

func closeDomainAndStorage() {
	atomic.StoreUint32(&tikv.ShuttingDown, 1)
	dom.Close()
	err := storage.Close()
	terror.Log(errors.Trace(err))
}

func cleanup() {
	if graceful {
		svr.GracefulDown(context.Background(), nil)
	} else {
		svr.TryGracefulDown()
	}
	plugin.Shutdown(context.Background())
	closeDomainAndStorage()
}
