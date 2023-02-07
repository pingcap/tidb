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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	parsertypes "github.com/pingcap/tidb/parser/types"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/resourcemanager"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	kvstore "github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/store/driver"
	"github.com/pingcap/tidb/store/mockstore"
	uni_metrics "github.com/pingcap/tidb/store/mockstore/unistore/metrics"
	pumpcli "github.com/pingcap/tidb/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/cpuprofile"
	"github.com/pingcap/tidb/util/deadlockhistory"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/domainutil"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tidb/util/sem"
	"github.com/pingcap/tidb/util/signal"
	"github.com/pingcap/tidb/util/sys/linux"
	storageSys "github.com/pingcap/tidb/util/sys/storage"
	"github.com/pingcap/tidb/util/systimemon"
	"github.com/pingcap/tidb/util/tiflashcompute"
	"github.com/pingcap/tidb/util/topsql"
	"github.com/pingcap/tidb/util/versioninfo"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	pd "github.com/tikv/pd/client"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
)

// Flag Names
const (
	nmVersion          = "V"
	nmConfig           = "config"
	nmConfigCheck      = "config-check"
	nmConfigStrict     = "config-strict"
	nmStore            = "store"
	nmStorePath        = "path"
	nmHost             = "host"
	nmAdvertiseAddress = "advertise-address"
	nmPort             = "P"
	nmCors             = "cors"
	nmSocket           = "socket"
	nmEnableBinlog     = "enable-binlog"
	nmRunDDL           = "run-ddl"
	nmLogLevel         = "L"
	nmLogFile          = "log-file"
	nmLogSlowQuery     = "log-slow-query"
	nmReportStatus     = "report-status"
	nmStatusHost       = "status-host"
	nmStatusPort       = "status"
	nmMetricsAddr      = "metrics-addr"
	nmMetricsInterval  = "metrics-interval"
	nmDdlLease         = "lease"
	nmTokenLimit       = "token-limit"
	nmPluginDir        = "plugin-dir"
	nmPluginLoad       = "plugin-load"
	nmRepairMode       = "repair-mode"
	nmRepairList       = "repair-list"
	nmTempDir          = "temp-dir"

	nmProxyProtocolNetworks      = "proxy-protocol-networks"
	nmProxyProtocolHeaderTimeout = "proxy-protocol-header-timeout"
	nmAffinityCPU                = "affinity-cpus"

	nmInitializeSecure            = "initialize-secure"
	nmInitializeInsecure          = "initialize-insecure"
	nmInitializeSQLFile           = "initialize-sql-file"
	nmDisconnectOnExpiredPassword = "disconnect-on-expired-password"
	nmKeyspaceName                = "keyspace-name"
)

var (
	version      = flagBoolean(nmVersion, false, "print version information and exit")
	configPath   = flag.String(nmConfig, "", "config file path")
	configCheck  = flagBoolean(nmConfigCheck, false, "check config file validity and exit")
	configStrict = flagBoolean(nmConfigStrict, false, "enforce config file validity")

	// Base
	store            = flag.String(nmStore, "unistore", "registered store name, [tikv, mocktikv, unistore]")
	storePath        = flag.String(nmStorePath, "/tmp/tidb", "tidb storage path")
	host             = flag.String(nmHost, "0.0.0.0", "tidb server host")
	advertiseAddress = flag.String(nmAdvertiseAddress, "", "tidb server advertise IP")
	port             = flag.String(nmPort, "4000", "tidb server port")
	cors             = flag.String(nmCors, "", "tidb server allow cors origin")
	socket           = flag.String(nmSocket, "/tmp/tidb-{Port}.sock", "The socket file to use for connection.")
	enableBinlog     = flagBoolean(nmEnableBinlog, false, "enable generate binlog")
	runDDL           = flagBoolean(nmRunDDL, true, "run ddl worker on this tidb-server")
	ddlLease         = flag.String(nmDdlLease, "45s", "schema lease duration, very dangerous to change only if you know what you do")
	tokenLimit       = flag.Int(nmTokenLimit, 1000, "the limit of concurrent executed sessions")
	pluginDir        = flag.String(nmPluginDir, "/data/deploy/plugin", "the folder that hold plugin")
	pluginLoad       = flag.String(nmPluginLoad, "", "wait load plugin name(separated by comma)")
	affinityCPU      = flag.String(nmAffinityCPU, "", "affinity cpu (cpu-no. separated by comma, e.g. 1,2,3)")
	repairMode       = flagBoolean(nmRepairMode, false, "enable admin repair mode")
	repairList       = flag.String(nmRepairList, "", "admin repair table list")
	tempDir          = flag.String(nmTempDir, config.DefTempDir, "tidb temporary directory")

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
	proxyProtocolHeaderTimeout = flag.Uint(nmProxyProtocolHeaderTimeout, 5, "proxy protocol header read timeout, unit is second. (Deprecated: as proxy protocol using lazy mode, header read timeout no longer used)")

	// Bootstrap and security
	initializeSecure            = flagBoolean(nmInitializeSecure, false, "bootstrap tidb-server in secure mode")
	initializeInsecure          = flagBoolean(nmInitializeInsecure, true, "bootstrap tidb-server in insecure mode")
	initializeSQLFile           = flag.String(nmInitializeSQLFile, "", "SQL file to execute on first bootstrap")
	disconnectOnExpiredPassword = flagBoolean(nmDisconnectOnExpiredPassword, true, "the server disconnects the client when the password is expired")
	keyspaceName                = flag.String(nmKeyspaceName, "", "keyspace name.")
)

func main() {
	help := flag.Bool("help", false, "show the usage")
	flag.Parse()
	if *help {
		flag.Usage()
		os.Exit(0)
	}
	config.InitializeConfig(*configPath, *configCheck, *configStrict, overrideConfig)
	if *version {
		setVersions()
		fmt.Println(printer.GetTiDBInfo())
		os.Exit(0)
	}
	registerStores()
	registerMetrics()
	if variable.EnableTmpStorageOnOOM.Load() {
		config.GetGlobalConfig().UpdateTempStoragePath()
		err := disk.InitializeTempDir()
		terror.MustNil(err)
		checkTempStorageQuota()
	}
	setupLog()
	setupExtensions()

	err := cpuprofile.StartCPUProfiler()
	terror.MustNil(err)

	if config.GetGlobalConfig().DisaggregatedTiFlash && config.GetGlobalConfig().UseAutoScaler {
		clusterID, err := config.GetAutoScalerClusterID()
		terror.MustNil(err)

		err = tiflashcompute.InitGlobalTopoFetcher(
			config.GetGlobalConfig().TiFlashComputeAutoScalerType,
			config.GetGlobalConfig().TiFlashComputeAutoScalerAddr,
			clusterID,
			config.GetGlobalConfig().IsTiFlashComputeFixedPool)
		terror.MustNil(err)
	}

	// Enable failpoints in tikv/client-go if the test API is enabled.
	// It appears in the main function to be set before any use of client-go to prevent data race.
	if _, err := failpoint.Status("github.com/pingcap/tidb/server/enableTestAPI"); err == nil {
		warnMsg := "tikv/client-go failpoint is enabled, this should NOT happen in the production environment"
		logutil.BgLogger().Warn(warnMsg)
		tikv.EnableFailpoints()
	}
	setGlobalVars()
	setCPUAffinity()
	setupTracing() // Should before createServer and after setup config.
	printInfo()
	setupBinlogClient()
	setupMetrics()

	keyspaceName := config.GetGlobalKeyspaceName()

	resourcemanager.InstanceResourceManager.Start()
	storage, dom := createStoreAndDomain(keyspaceName)
	svr := createServer(storage, dom)
	err = driver.TrySetupGlobalResourceController(context.Background(), dom.ServerID(), storage)
	if err != nil {
		logutil.BgLogger().Warn("failed to setup global resource controller", zap.Error(err))
	}

	// Register error API is not thread-safe, the caller MUST NOT register errors after initialization.
	// To prevent misuse, set a flag to indicate that register new error will panic immediately.
	// For regression of issue like https://github.com/pingcap/tidb/issues/28190
	terror.RegisterFinish()

	exited := make(chan struct{})
	signal.SetupSignalHandler(func(graceful bool) {
		svr.Close()
		cleanup(svr, storage, dom, graceful)
		cpuprofile.StopCPUProfiler()
		resourcemanager.InstanceResourceManager.Stop()
		close(exited)
	})
	topsql.SetupTopSQL()
	terror.MustNil(svr.Run())
	<-exited
	syncLog()
}

func syncLog() {
	if err := log.Sync(); err != nil {
		// Don't complain about /dev/stdout as Fsync will return EINVAL.
		if pathErr, ok := err.(*fs.PathError); ok {
			if pathErr.Path == "/dev/stdout" {
				os.Exit(0)
			}
		}
		fmt.Fprintln(os.Stderr, "sync log err:", err)
		os.Exit(1)
	}
}

func checkTempStorageQuota() {
	// check capacity and the quota when EnableTmpStorageOnOOM is enabled
	c := config.GetGlobalConfig()
	if c.TempStorageQuota < 0 {
		// means unlimited, do nothing
	} else {
		capacityByte, err := storageSys.GetTargetDirectoryCapacity(c.TempStoragePath)
		if err != nil {
			log.Fatal(err.Error())
		} else if capacityByte < uint64(c.TempStorageQuota) {
			log.Fatal(fmt.Sprintf("value of [tmp-storage-quota](%d byte) exceeds the capacity(%d byte) of the [%s] directory", c.TempStorageQuota, capacityByte, c.TempStoragePath))
		}
	}
}

func setCPUAffinity() {
	if affinityCPU == nil || len(*affinityCPU) == 0 {
		return
	}
	var cpu []int
	for _, af := range strings.Split(*affinityCPU, ",") {
		af = strings.TrimSpace(af)
		if len(af) > 0 {
			c, err := strconv.Atoi(af)
			if err != nil {
				fmt.Fprintf(os.Stderr, "wrong affinity cpu config: %s", *affinityCPU)
				os.Exit(1)
			}
			cpu = append(cpu, c)
		}
	}
	err := linux.SetAffinity(cpu)
	if err != nil {
		fmt.Fprintf(os.Stderr, "set cpu affinity failure: %v", err)
		os.Exit(1)
	}
	runtime.GOMAXPROCS(len(cpu))
	metrics.MaxProcs.Set(float64(runtime.GOMAXPROCS(0)))
}

func registerStores() {
	err := kvstore.Register("tikv", driver.TiKVDriver{})
	terror.MustNil(err)
	err = kvstore.Register("mocktikv", mockstore.MockTiKVDriver{})
	terror.MustNil(err)
	err = kvstore.Register("unistore", mockstore.EmbedUnistoreDriver{})
	terror.MustNil(err)
}

func registerMetrics() {
	metrics.RegisterMetrics()
	if config.GetGlobalConfig().Store == "unistore" {
		uni_metrics.RegisterMetrics()
	}
}

func createStoreAndDomain(keyspaceName string) (kv.Storage, *domain.Domain) {
	cfg := config.GetGlobalConfig()
	var fullPath string
	if keyspaceName == "" {
		fullPath = fmt.Sprintf("%s://%s", cfg.Store, cfg.Path)
	} else {
		fullPath = fmt.Sprintf("%s://%s?keyspaceName=%s", cfg.Store, cfg.Path, keyspaceName)
	}
	var err error
	storage, err := kvstore.New(fullPath)
	terror.MustNil(err)
	copr.GlobalMPPFailedStoreProber.Run()
	err = infosync.CheckTiKVVersion(storage, *semver.New(versioninfo.TiKVMinVersion))
	terror.MustNil(err)
	// Bootstrap a session to load information schema.
	dom, err := session.BootstrapSession(storage)
	terror.MustNil(err)
	return storage, dom
}

func setupBinlogClient() {
	cfg := config.GetGlobalConfig()
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

	err = logutil.InitLogger(cfg.Log.ToLogConfig())
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
	cfg := config.GetGlobalConfig()
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

// overrideConfig considers command arguments and overrides some config items in the Config.
func overrideConfig(cfg *config.Config) {
	actualFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		actualFlags[f.Name] = true
	})

	// Base
	if actualFlags[nmHost] {
		cfg.Host = *host
	}
	if actualFlags[nmAdvertiseAddress] {
		var err error
		if len(strings.Split(*advertiseAddress, " ")) > 1 {
			err = errors.Errorf("Only support one advertise-address")
		}
		terror.MustNil(err)
		cfg.AdvertiseAddress = *advertiseAddress
	}
	if len(cfg.AdvertiseAddress) == 0 && cfg.Host == "0.0.0.0" {
		cfg.AdvertiseAddress = util.GetLocalIP()
	}
	if len(cfg.AdvertiseAddress) == 0 {
		cfg.AdvertiseAddress = cfg.Host
	}
	var err error
	if actualFlags[nmPort] {
		var p int
		p, err = strconv.Atoi(*port)
		terror.MustNil(err)
		cfg.Port = uint(p)
	}
	if actualFlags[nmCors] {
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
		cfg.Instance.TiDBEnableDDL.Store(*runDDL)
	}
	if actualFlags[nmDdlLease] {
		cfg.Lease = *ddlLease
	}
	if actualFlags[nmTokenLimit] {
		cfg.TokenLimit = uint(*tokenLimit)
	}
	if actualFlags[nmPluginLoad] {
		cfg.Instance.PluginLoad = *pluginLoad
	}
	if actualFlags[nmPluginDir] {
		cfg.Instance.PluginDir = *pluginDir
	}

	if actualFlags[nmRepairMode] {
		cfg.RepairMode = *repairMode
	}
	if actualFlags[nmRepairList] {
		if cfg.RepairMode {
			cfg.RepairTableList = stringToList(*repairList)
		}
	}
	if actualFlags[nmTempDir] {
		cfg.TempDir = *tempDir
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

	// Sanity check: can't specify both options
	if actualFlags[nmInitializeSecure] && actualFlags[nmInitializeInsecure] {
		err = fmt.Errorf("the options -initialize-insecure and -initialize-secure are mutually exclusive")
		terror.MustNil(err)
	}
	// The option --initialize-secure=true ensures that a secure bootstrap is used.
	if actualFlags[nmInitializeSecure] {
		cfg.Security.SecureBootstrap = *initializeSecure
	}
	// The option --initialize-insecure=true/false was used.
	// Store the inverted value of this to the secure bootstrap cfg item
	if actualFlags[nmInitializeInsecure] {
		cfg.Security.SecureBootstrap = !*initializeInsecure
	}
	if actualFlags[nmDisconnectOnExpiredPassword] {
		cfg.Security.DisconnectOnExpiredPassword = *disconnectOnExpiredPassword
	}
	// Secure bootstrap initializes with Socket authentication
	// which is not supported on windows. Only the insecure bootstrap
	// method is supported.
	if runtime.GOOS == "windows" && cfg.Security.SecureBootstrap {
		err = fmt.Errorf("the option -initialize-secure is not supported on Windows")
		terror.MustNil(err)
	}
	// Initialize SQL File is used to run a set of SQL statements after first bootstrap.
	// It is important in the use case that you want to set GLOBAL variables, which
	// are persisted to the cluster and not read from a config file.
	if actualFlags[nmInitializeSQLFile] {
		if _, err := os.Stat(*initializeSQLFile); err != nil {
			err = fmt.Errorf("can not access -initialize-sql-file %s", *initializeSQLFile)
			terror.MustNil(err)
		}
		cfg.InitializeSQLFile = *initializeSQLFile
	}

	if actualFlags[nmKeyspaceName] {
		cfg.KeyspaceName = *keyspaceName
	}
}

func setVersions() {
	cfg := config.GetGlobalConfig()
	if len(cfg.ServerVersion) > 0 {
		mysql.ServerVersion = cfg.ServerVersion
	}
	if len(cfg.TiDBEdition) > 0 {
		versioninfo.TiDBEdition = cfg.TiDBEdition
	}
	if len(cfg.TiDBReleaseVersion) > 0 {
		mysql.TiDBReleaseVersion = cfg.TiDBReleaseVersion
	}
}

func setGlobalVars() {
	cfg := config.GetGlobalConfig()

	// config.DeprecatedOptions records the config options that should be moved to [instance] section.
	for _, deprecatedOption := range config.DeprecatedOptions {
		for oldName := range deprecatedOption.NameMappings {
			switch deprecatedOption.SectionName {
			case "":
				switch oldName {
				case "check-mb4-value-in-utf8":
					cfg.Instance.CheckMb4ValueInUTF8.Store(cfg.CheckMb4ValueInUTF8.Load())
				case "enable-collect-execution-info":
					cfg.Instance.EnableCollectExecutionInfo.Store(cfg.EnableCollectExecutionInfo)
				case "max-server-connections":
					cfg.Instance.MaxConnections = cfg.MaxServerConnections
				case "run-ddl":
					cfg.Instance.TiDBEnableDDL.Store(cfg.RunDDL)
				}
			case "log":
				switch oldName {
				case "enable-slow-log":
					cfg.Instance.EnableSlowLog.Store(cfg.Log.EnableSlowLog.Load())
				case "slow-threshold":
					cfg.Instance.SlowThreshold = cfg.Log.SlowThreshold
				case "record-plan-in-slow-log":
					cfg.Instance.RecordPlanInSlowLog = cfg.Log.RecordPlanInSlowLog
				}
			case "performance":
				switch oldName {
				case "force-priority":
					cfg.Instance.ForcePriority = cfg.Performance.ForcePriority
				}
			case "plugin":
				switch oldName {
				case "load":
					cfg.Instance.PluginLoad = cfg.Plugin.Load
				case "dir":
					cfg.Instance.PluginDir = cfg.Plugin.Dir
				}
			default:
			}
		}
	}

	// Disable automaxprocs log
	nopLog := func(string, ...interface{}) {}
	_, err := maxprocs.Set(maxprocs.Logger(nopLog))
	terror.MustNil(err)
	// We should respect to user's settings in config file.
	// The default value of MaxProcs is 0, runtime.GOMAXPROCS(0) is no-op.
	runtime.GOMAXPROCS(int(cfg.Performance.MaxProcs))
	metrics.MaxProcs.Set(float64(runtime.GOMAXPROCS(0)))

	util.SetGOGC(cfg.Performance.GOGC)

	ddlLeaseDuration := parseDuration(cfg.Lease)
	session.SetSchemaLease(ddlLeaseDuration)
	statsLeaseDuration := parseDuration(cfg.Performance.StatsLease)
	session.SetStatsLease(statsLeaseDuration)
	indexUsageSyncLeaseDuration := parseDuration(cfg.Performance.IndexUsageSyncLease)
	session.SetIndexUsageSyncLease(indexUsageSyncLeaseDuration)
	planReplayerGCLease := parseDuration(cfg.Performance.PlanReplayerGCLease)
	session.SetPlanReplayerGCLease(planReplayerGCLease)
	bindinfo.Lease = parseDuration(cfg.Performance.BindInfoLease)
	statistics.RatioOfPseudoEstimate.Store(cfg.Performance.PseudoEstimateRatio)
	if cfg.SplitTable {
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	}
	plannercore.AllowCartesianProduct.Store(cfg.Performance.CrossJoin)
	privileges.SkipWithGrant = cfg.Security.SkipGrantTable
	if cfg.Performance.TxnTotalSizeLimit == config.DefTxnTotalSizeLimit {
		// practically deprecate the config, let the new session memory tracker take charge of it.
		kv.TxnTotalSizeLimit = config.SuperLargeTxnSize
	} else {
		kv.TxnTotalSizeLimit = cfg.Performance.TxnTotalSizeLimit
	}
	if cfg.Performance.TxnEntrySizeLimit > config.MaxTxnEntrySizeLimit {
		log.Fatal("cannot set txn entry size limit larger than 120M")
	}
	kv.TxnEntrySizeLimit = cfg.Performance.TxnEntrySizeLimit

	priority := mysql.Str2Priority(cfg.Instance.ForcePriority)
	variable.ForcePriority = int32(priority)

	variable.ProcessGeneralLog.Store(cfg.Instance.TiDBGeneralLog)
	variable.EnablePProfSQLCPU.Store(cfg.Instance.EnablePProfSQLCPU)
	variable.EnableRCReadCheckTS.Store(cfg.Instance.TiDBRCReadCheckTS)
	variable.IsSandBoxModeEnabled.Store(!cfg.Security.DisconnectOnExpiredPassword)
	atomic.StoreUint32(&variable.DDLSlowOprThreshold, cfg.Instance.DDLSlowOprThreshold)
	atomic.StoreUint64(&variable.ExpensiveQueryTimeThreshold, cfg.Instance.ExpensiveQueryTimeThreshold)

	if len(cfg.ServerVersion) > 0 {
		mysql.ServerVersion = cfg.ServerVersion
		variable.SetSysVar(variable.Version, cfg.ServerVersion)
	}

	if len(cfg.TiDBEdition) > 0 {
		versioninfo.TiDBEdition = cfg.TiDBEdition
		variable.SetSysVar(variable.VersionComment, "TiDB Server (Apache License 2.0) "+versioninfo.TiDBEdition+" Edition, MySQL 5.7 compatible")
	}
	if len(cfg.VersionComment) > 0 {
		variable.SetSysVar(variable.VersionComment, cfg.VersionComment)
	}
	if len(cfg.TiDBReleaseVersion) > 0 {
		mysql.TiDBReleaseVersion = cfg.TiDBReleaseVersion
	}

	variable.SetSysVar(variable.TiDBForcePriority, mysql.Priority2Str[priority])
	variable.SetSysVar(variable.TiDBOptDistinctAggPushDown, variable.BoolToOnOff(cfg.Performance.DistinctAggPushDown))
	variable.SetSysVar(variable.TiDBOptProjectionPushDown, variable.BoolToOnOff(cfg.Performance.ProjectionPushDown))
	variable.SetSysVar(variable.LogBin, variable.BoolToOnOff(cfg.Binlog.Enable))
	variable.SetSysVar(variable.Port, fmt.Sprintf("%d", cfg.Port))
	cfg.Socket = strings.Replace(cfg.Socket, "{Port}", fmt.Sprintf("%d", cfg.Port), 1)
	variable.SetSysVar(variable.Socket, cfg.Socket)
	variable.SetSysVar(variable.DataDir, cfg.Path)
	variable.SetSysVar(variable.TiDBSlowQueryFile, cfg.Log.SlowQueryFile)
	variable.SetSysVar(variable.TiDBIsolationReadEngines, strings.Join(cfg.IsolationRead.Engines, ","))
	variable.SetSysVar(variable.TiDBEnforceMPPExecution, variable.BoolToOnOff(config.GetGlobalConfig().Performance.EnforceMPP))
	variable.MemoryUsageAlarmRatio.Store(cfg.Instance.MemoryUsageAlarmRatio)
	variable.SetSysVar(variable.TiDBConstraintCheckInPlacePessimistic, variable.BoolToOnOff(cfg.PessimisticTxn.ConstraintCheckInPlacePessimistic))
	if hostname, err := os.Hostname(); err == nil {
		variable.SetSysVar(variable.Hostname, hostname)
	}
	variable.GlobalLogMaxDays.Store(int32(config.GetGlobalConfig().Log.File.MaxDays))

	if cfg.Security.EnableSEM {
		sem.Enable()
	}

	// For CI environment we default enable prepare-plan-cache.
	if config.CheckTableBeforeDrop { // only for test
		variable.SetSysVar(variable.TiDBEnablePrepPlanCache, variable.BoolToOnOff(true))
	}
	// use server-memory-quota as max-plan-cache-memory
	plannercore.PreparedPlanCacheMaxMemory.Store(cfg.Performance.ServerMemoryQuota)
	total, err := memory.MemTotal()
	terror.MustNil(err)
	// if server-memory-quota is larger than max-system-memory or not set, use max-system-memory as max-plan-cache-memory
	if plannercore.PreparedPlanCacheMaxMemory.Load() > total || plannercore.PreparedPlanCacheMaxMemory.Load() <= 0 {
		plannercore.PreparedPlanCacheMaxMemory.Store(total)
	}

	atomic.StoreUint64(&transaction.CommitMaxBackoff, uint64(parseDuration(cfg.TiKVClient.CommitTimeout).Seconds()*1000))
	tikv.SetRegionCacheTTLSec(int64(cfg.TiKVClient.RegionCacheTTL))
	domainutil.RepairInfo.SetRepairMode(cfg.RepairMode)
	domainutil.RepairInfo.SetRepairTableList(cfg.RepairTableList)
	executor.GlobalDiskUsageTracker.SetBytesLimit(cfg.TempStorageQuota)
	if cfg.Performance.ServerMemoryQuota < 1 {
		// If MaxMemory equals 0, it means unlimited
		executor.GlobalMemoryUsageTracker.SetBytesLimit(-1)
	} else {
		executor.GlobalMemoryUsageTracker.SetBytesLimit(int64(cfg.Performance.ServerMemoryQuota))
	}
	kvcache.GlobalLRUMemUsageTracker.AttachToGlobalTracker(executor.GlobalMemoryUsageTracker)

	t, err := time.ParseDuration(cfg.TiKVClient.StoreLivenessTimeout)
	if err != nil || t < 0 {
		logutil.BgLogger().Fatal("invalid duration value for store-liveness-timeout",
			zap.String("currentValue", cfg.TiKVClient.StoreLivenessTimeout))
	}
	tikv.SetStoreLivenessTimeout(t)
	parsertypes.TiDBStrictIntegerDisplayWidth = cfg.DeprecateIntegerDisplayWidth
	deadlockhistory.GlobalDeadlockHistory.Resize(cfg.PessimisticTxn.DeadlockHistoryCapacity)
	txninfo.Recorder.ResizeSummaries(cfg.TrxSummary.TransactionSummaryCapacity)
	txninfo.Recorder.SetMinDuration(time.Duration(cfg.TrxSummary.TransactionIDDigestMinDuration) * time.Millisecond)
	chunk.InitChunkAllocSize(cfg.TiDBMaxReuseChunk, cfg.TiDBMaxReuseColumn)
}

func setupLog() {
	cfg := config.GetGlobalConfig()
	err := logutil.InitLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)

	// trigger internal http(s) client init.
	util.InternalHTTPClient()
}

func setupExtensions() *extension.Extensions {
	err := extension.Setup()
	terror.MustNil(err)

	extensions, err := extension.GetExtensions()
	terror.MustNil(err)

	return extensions
}

func printInfo() {
	// Make sure the TiDB info is always printed.
	level := log.GetLevel()
	log.SetLevel(zap.InfoLevel)
	printer.PrintTiDBInfo()
	log.SetLevel(level)
}

func createServer(storage kv.Storage, dom *domain.Domain) *server.Server {
	cfg := config.GetGlobalConfig()
	driver := server.NewTiDBDriver(storage)
	svr, err := server.NewServer(cfg, driver)
	// Both domain and storage have started, so we have to clean them before exiting.
	if err != nil {
		closeDomainAndStorage(storage, dom)
		log.Fatal("failed to create the server", zap.Error(err), zap.Stack("stack"))
	}
	svr.SetDomain(dom)
	svr.InitGlobalConnID(dom.ServerID)
	go dom.ExpensiveQueryHandle().SetSessionManager(svr).Run()
	go dom.MemoryUsageAlarmHandle().SetSessionManager(svr).Run()
	go dom.ServerMemoryLimitHandle().SetSessionManager(svr).Run()
	dom.InfoSyncer().SetSessionManager(svr)
	return svr
}

func setupMetrics() {
	cfg := config.GetGlobalConfig()
	// Enable the mutex profile, 1/10 of mutex blocking event sampling.
	runtime.SetMutexProfileFraction(10)
	systimeErrHandler := func() {
		metrics.TimeJumpBackCounter.Inc()
	}
	go systimemon.StartMonitor(time.Now, systimeErrHandler)

	pushMetric(cfg.Status.MetricsAddr, time.Duration(cfg.Status.MetricsInterval)*time.Second)
}

func setupTracing() {
	cfg := config.GetGlobalConfig()
	tracingCfg := cfg.OpenTracing.ToTracingConfig()
	tracingCfg.ServiceName = "TiDB"
	tracer, _, err := tracingCfg.NewTracer()
	if err != nil {
		log.Fatal("setup jaeger tracer failed", zap.String("error message", err.Error()))
	}
	opentracing.SetGlobalTracer(tracer)
}

func closeDomainAndStorage(storage kv.Storage, dom *domain.Domain) {
	tikv.StoreShuttingDown(1)
	dom.Close()
	copr.GlobalMPPFailedStoreProber.Stop()
	err := storage.Close()
	terror.Log(errors.Trace(err))
}

func cleanup(svr *server.Server, storage kv.Storage, dom *domain.Domain, graceful bool) {
	if graceful {
		done := make(chan struct{})
		svr.GracefulDown(context.Background(), done)
		// Kill sys processes such as auto analyze. Otherwise, tidb-server cannot exit until auto analyze is finished.
		// See https://github.com/pingcap/tidb/issues/40038 for details.
		svr.KillSysProcesses()
	} else {
		svr.TryGracefulDown()
	}
	plugin.Shutdown(context.Background())
	closeDomainAndStorage(storage, dom)
	disk.CleanUp()
	topsql.Close()
}

func stringToList(repairString string) []string {
	if len(repairString) <= 0 {
		return []string{}
	}
	if repairString[0] == '[' && repairString[len(repairString)-1] == ']' {
		repairString = repairString[1 : len(repairString)-1]
	}
	return strings.FieldsFunc(repairString, func(r rune) bool {
		return r == ',' || r == ' ' || r == '"'
	})
}
