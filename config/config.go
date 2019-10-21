// Copyright 2017 PingCAP, Inc.
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

package config

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	zaplog "github.com/pingcap/log"
	"github.com/pingcap/tidb/util/logutil"
	tracing "github.com/uber/jaeger-client-go/config"
	"go.uber.org/atomic"
)

// Config number limitations
const (
	MaxLogFileSize    = 4096 // MB
	MinPessimisticTTL = time.Second * 15
	MaxPessimisticTTL = time.Second * 120
	// DefTxnTotalSizeLimit is the default value of TxnTxnTotalSizeLimit.
	DefTxnTotalSizeLimit = 100 * 1024 * 1024
)

// Valid config maps
var (
	ValidStorage = map[string]bool{
		"mocktikv": true,
		"tikv":     true,
	}
	// checkTableBeforeDrop enable to execute `admin check table` before `drop table`.
	CheckTableBeforeDrop = false
	// checkBeforeDropLDFlag is a go build flag.
	checkBeforeDropLDFlag = "None"
)

// Config contains configuration options.
type Config struct {
	Host             string          `toml:"host" json:"host"`
	AdvertiseAddress string          `toml:"advertise-address" json:"advertise-address"`
	Port             uint            `toml:"port" json:"port"`
	Cors             string          `toml:"cors" json:"cors"`
	Store            string          `toml:"store" json:"store"`
	Path             string          `toml:"path" json:"path"`
	Socket           string          `toml:"socket" json:"socket"`
	Lease            string          `toml:"lease" json:"lease"`
	RunDDL           bool            `toml:"run-ddl" json:"run-ddl"`
	SplitTable       bool            `toml:"split-table" json:"split-table"`
	TokenLimit       uint            `toml:"token-limit" json:"token-limit"`
	OOMUseTmpStorage bool            `toml:"oom-use-tmp-storage" json:"oom-use-tmp-storage"`
	OOMAction        string          `toml:"oom-action" json:"oom-action"`
	MemQuotaQuery    int64           `toml:"mem-quota-query" json:"mem-quota-query"`
	EnableStreaming  bool            `toml:"enable-streaming" json:"enable-streaming"`
	TxnLocalLatches  TxnLocalLatches `toml:"txn-local-latches" json:"txn-local-latches"`
	// Set sys variable lower-case-table-names, ref: https://dev.mysql.com/doc/refman/5.7/en/identifier-case-sensitivity.html.
	// TODO: We actually only support mode 2, which keeps the original case, but the comparison is case-insensitive.
	LowerCaseTableNames int `toml:"lower-case-table-names" json:"lower-case-table-names"`

	Log                 Log               `toml:"log" json:"log"`
	Security            Security          `toml:"security" json:"security"`
	Status              Status            `toml:"status" json:"status"`
	Performance         Performance       `toml:"performance" json:"performance"`
	PreparedPlanCache   PreparedPlanCache `toml:"prepared-plan-cache" json:"prepared-plan-cache"`
	OpenTracing         OpenTracing       `toml:"opentracing" json:"opentracing"`
	ProxyProtocol       ProxyProtocol     `toml:"proxy-protocol" json:"proxy-protocol"`
	TiKVClient          TiKVClient        `toml:"tikv-client" json:"tikv-client"`
	Binlog              Binlog            `toml:"binlog" json:"binlog"`
	CompatibleKillQuery bool              `toml:"compatible-kill-query" json:"compatible-kill-query"`
	Plugin              Plugin            `toml:"plugin" json:"plugin"`
	PessimisticTxn      PessimisticTxn    `toml:"pessimistic-txn" json:"pessimistic-txn"`
	CheckMb4ValueInUTF8 bool              `toml:"check-mb4-value-in-utf8" json:"check-mb4-value-in-utf8"`
	// TreatOldVersionUTF8AsUTF8MB4 is use to treat old version table/column UTF8 charset as UTF8MB4. This is for compatibility.
	// Currently not support dynamic modify, because this need to reload all old version schema.
	TreatOldVersionUTF8AsUTF8MB4 bool `toml:"treat-old-version-utf8-as-utf8mb4" json:"treat-old-version-utf8-as-utf8mb4"`
	// EnableTableLock indicate whether enable table lock.
	// TODO: remove this after table lock features stable.
	EnableTableLock     bool        `toml:"enable-table-lock" json:"enable-table-lock"`
	DelayCleanTableLock uint64      `toml:"delay-clean-table-lock" json:"delay-clean-table-lock"`
	SplitRegionMaxNum   uint64      `toml:"split-region-max-num" json:"split-region-max-num"`
	StmtSummary         StmtSummary `toml:"stmt-summary" json:"stmt-summary"`
}

// Log is the log section of config.
type Log struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// Log format. one of json, text, or console.
	Format string `toml:"format" json:"format"`
	// Disable automatic timestamps in output.
	DisableTimestamp bool `toml:"disable-timestamp" json:"disable-timestamp"`
	// DisableErrorStack stops annotating logs with the full stack error
	// message.
	DisableErrorStack bool `toml:"disable-error-stack" json:"disable-error-stack"`
	// File log config.
	File logutil.FileLogConfig `toml:"file" json:"file"`

	SlowQueryFile       string `toml:"slow-query-file" json:"slow-query-file"`
	SlowThreshold       uint64 `toml:"slow-threshold" json:"slow-threshold"`
	ExpensiveThreshold  uint   `toml:"expensive-threshold" json:"expensive-threshold"`
	QueryLogMaxLen      uint64 `toml:"query-log-max-len" json:"query-log-max-len"`
	RecordPlanInSlowLog uint32 `toml:"record-plan-in-slow-log" json:"record-plan-in-slow-log"`
}

// Security is the security section of the config.
type Security struct {
	SkipGrantTable bool   `toml:"skip-grant-table" json:"skip-grant-table"`
	SSLCA          string `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert        string `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey         string `toml:"ssl-key" json:"ssl-key"`
	ClusterSSLCA   string `toml:"cluster-ssl-ca" json:"cluster-ssl-ca"`
	ClusterSSLCert string `toml:"cluster-ssl-cert" json:"cluster-ssl-cert"`
	ClusterSSLKey  string `toml:"cluster-ssl-key" json:"cluster-ssl-key"`
}

// The ErrConfigValidationFailed error is used so that external callers can do a type assertion
// to defer handling of this specific error when someone does not want strict type checking.
// This is needed only because logging hasn't been set up at the time we parse the config file.
// This should all be ripped out once strict config checking is made the default behavior.
type ErrConfigValidationFailed struct {
	err string
}

func (e *ErrConfigValidationFailed) Error() string {
	return e.err
}

// ToTLSConfig generates tls's config based on security section of the config.
func (s *Security) ToTLSConfig() (*tls.Config, error) {
	var tlsConfig *tls.Config
	if len(s.ClusterSSLCA) != 0 {
		var certificates = make([]tls.Certificate, 0)
		if len(s.ClusterSSLCert) != 0 && len(s.ClusterSSLKey) != 0 {
			// Load the client certificates from disk
			certificate, err := tls.LoadX509KeyPair(s.ClusterSSLCert, s.ClusterSSLKey)
			if err != nil {
				return nil, errors.Errorf("could not load client key pair: %s", err)
			}
			certificates = append(certificates, certificate)
		}

		// Create a certificate pool from the certificate authority
		certPool := x509.NewCertPool()
		ca, err := ioutil.ReadFile(s.ClusterSSLCA)
		if err != nil {
			return nil, errors.Errorf("could not read ca certificate: %s", err)
		}

		// Append the certificates from the CA
		if !certPool.AppendCertsFromPEM(ca) {
			return nil, errors.New("failed to append ca certs")
		}

		tlsConfig = &tls.Config{
			Certificates: certificates,
			RootCAs:      certPool,
		}
	}

	return tlsConfig, nil
}

// Status is the status section of the config.
type Status struct {
	StatusHost      string `toml:"status-host" json:"status-host"`
	MetricsAddr     string `toml:"metrics-addr" json:"metrics-addr"`
	StatusPort      uint   `toml:"status-port" json:"status-port"`
	MetricsInterval uint   `toml:"metrics-interval" json:"metrics-interval"`
	ReportStatus    bool   `toml:"report-status" json:"report-status"`
	RecordQPSbyDB   bool   `toml:"record-db-qps" json:"record-db-qps"`
}

// Performance is the performance section of the config.
type Performance struct {
	MaxProcs            uint    `toml:"max-procs" json:"max-procs"`
	MaxMemory           uint64  `toml:"max-memory" json:"max-memory"`
	StatsLease          string  `toml:"stats-lease" json:"stats-lease"`
	StmtCountLimit      uint    `toml:"stmt-count-limit" json:"stmt-count-limit"`
	FeedbackProbability float64 `toml:"feedback-probability" json:"feedback-probability"`
	QueryFeedbackLimit  uint    `toml:"query-feedback-limit" json:"query-feedback-limit"`
	PseudoEstimateRatio float64 `toml:"pseudo-estimate-ratio" json:"pseudo-estimate-ratio"`
	ForcePriority       string  `toml:"force-priority" json:"force-priority"`
	BindInfoLease       string  `toml:"bind-info-lease" json:"bind-info-lease"`
	TxnTotalSizeLimit   uint64  `toml:"txn-total-size-limit" json:"txn-total-size-limit"`
	TCPKeepAlive        bool    `toml:"tcp-keep-alive" json:"tcp-keep-alive"`
	CrossJoin           bool    `toml:"cross-join" json:"cross-join"`
	RunAutoAnalyze      bool    `toml:"run-auto-analyze" json:"run-auto-analyze"`
}

// PlanCache is the PlanCache section of the config.
type PlanCache struct {
	Enabled  bool `toml:"enabled" json:"enabled"`
	Capacity uint `toml:"capacity" json:"capacity"`
	Shards   uint `toml:"shards" json:"shards"`
}

// TxnLocalLatches is the TxnLocalLatches section of the config.
type TxnLocalLatches struct {
	Enabled  bool `toml:"enabled" json:"enabled"`
	Capacity uint `toml:"capacity" json:"capacity"`
}

// PreparedPlanCache is the PreparedPlanCache section of the config.
type PreparedPlanCache struct {
	Enabled          bool    `toml:"enabled" json:"enabled"`
	Capacity         uint    `toml:"capacity" json:"capacity"`
	MemoryGuardRatio float64 `toml:"memory-guard-ratio" json:"memory-guard-ratio"`
}

// OpenTracing is the opentracing section of the config.
type OpenTracing struct {
	Enable     bool                `toml:"enable" json:"enable"`
	RPCMetrics bool                `toml:"rpc-metrics" json:"rpc-metrics"`
	Sampler    OpenTracingSampler  `toml:"sampler" json:"sampler"`
	Reporter   OpenTracingReporter `toml:"reporter" json:"reporter"`
}

// OpenTracingSampler is the config for opentracing sampler.
// See https://godoc.org/github.com/uber/jaeger-client-go/config#SamplerConfig
type OpenTracingSampler struct {
	Type                    string        `toml:"type" json:"type"`
	Param                   float64       `toml:"param" json:"param"`
	SamplingServerURL       string        `toml:"sampling-server-url" json:"sampling-server-url"`
	MaxOperations           int           `toml:"max-operations" json:"max-operations"`
	SamplingRefreshInterval time.Duration `toml:"sampling-refresh-interval" json:"sampling-refresh-interval"`
}

// OpenTracingReporter is the config for opentracing reporter.
// See https://godoc.org/github.com/uber/jaeger-client-go/config#ReporterConfig
type OpenTracingReporter struct {
	QueueSize           int           `toml:"queue-size" json:"queue-size"`
	BufferFlushInterval time.Duration `toml:"buffer-flush-interval" json:"buffer-flush-interval"`
	LogSpans            bool          `toml:"log-spans" json:"log-spans"`
	LocalAgentHostPort  string        `toml:"local-agent-host-port" json:"local-agent-host-port"`
}

// ProxyProtocol is the PROXY protocol section of the config.
type ProxyProtocol struct {
	// PROXY protocol acceptable client networks.
	// Empty string means disable PROXY protocol,
	// * means all networks.
	Networks string `toml:"networks" json:"networks"`
	// PROXY protocol header read timeout, Unit is second.
	HeaderTimeout uint `toml:"header-timeout" json:"header-timeout"`
}

// TiKVClient is the config for tikv client.
type TiKVClient struct {
	// GrpcConnectionCount is the max gRPC connections that will be established
	// with each tikv-server.
	GrpcConnectionCount uint `toml:"grpc-connection-count" json:"grpc-connection-count"`
	// After a duration of this time in seconds if the client doesn't see any activity it pings
	// the server to see if the transport is still alive.
	GrpcKeepAliveTime uint `toml:"grpc-keepalive-time" json:"grpc-keepalive-time"`
	// After having pinged for keepalive check, the client waits for a duration of Timeout in seconds
	// and if no activity is seen even after that the connection is closed.
	GrpcKeepAliveTimeout uint `toml:"grpc-keepalive-timeout" json:"grpc-keepalive-timeout"`
	// CommitTimeout is the max time which command 'commit' will wait.
	CommitTimeout string `toml:"commit-timeout" json:"commit-timeout"`

	// MaxBatchSize is the max batch size when calling batch commands API.
	MaxBatchSize uint `toml:"max-batch-size" json:"max-batch-size"`
	// If TiKV load is greater than this, TiDB will wait for a while to avoid little batch.
	OverloadThreshold uint `toml:"overload-threshold" json:"overload-threshold"`
	// MaxBatchWaitTime in nanosecond is the max wait time for batch.
	MaxBatchWaitTime time.Duration `toml:"max-batch-wait-time" json:"max-batch-wait-time"`
	// BatchWaitSize is the max wait size for batch.
	BatchWaitSize uint `toml:"batch-wait-size" json:"batch-wait-size"`
	// EnableArrow indicate the data encode in arrow format.
	EnableArrow bool `toml:"enable-arrow" json:"enable-arrow"`
	// If a Region has not been accessed for more than the given duration (in seconds), it
	// will be reloaded from the PD.
	RegionCacheTTL uint `toml:"region-cache-ttl" json:"region-cache-ttl"`
}

// Binlog is the config for binlog.
type Binlog struct {
	Enable bool `toml:"enable" json:"enable"`
	// If IgnoreError is true, when writing binlog meets error, TiDB would
	// ignore the error.
	IgnoreError  bool   `toml:"ignore-error" json:"ignore-error"`
	WriteTimeout string `toml:"write-timeout" json:"write-timeout"`
	// Use socket file to write binlog, for compatible with kafka version tidb-binlog.
	BinlogSocket string `toml:"binlog-socket" json:"binlog-socket"`
	// The strategy for sending binlog to pump, value can be "range" or "hash" now.
	Strategy string `toml:"strategy" json:"strategy"`
}

// Plugin is the config for plugin
type Plugin struct {
	Dir  string `toml:"dir" json:"dir"`
	Load string `toml:"load" json:"load"`
}

// PessimisticTxn is the config for pessimistic transaction.
type PessimisticTxn struct {
	// Enable must be true for 'begin lock' or session variable to start a pessimistic transaction.
	Enable bool `toml:"enable" json:"enable"`
	// The max count of retry for a single statement in a pessimistic transaction.
	MaxRetryCount uint `toml:"max-retry-count" json:"max-retry-count"`
	// The pessimistic lock ttl.
	TTL string `toml:"ttl" json:"ttl"`
}

// StmtSummary is the config for statement summary.
type StmtSummary struct {
	// The maximum number of statements kept in memory.
	MaxStmtCount uint `toml:"max-stmt-count" json:"max-stmt-count"`
	// The maximum length of displayed normalized SQL and sample SQL.
	MaxSQLLength uint `toml:"max-sql-length" json:"max-sql-length"`
}

var defaultConf = Config{
	Host:                         "0.0.0.0",
	AdvertiseAddress:             "",
	Port:                         4000,
	Cors:                         "",
	Store:                        "mocktikv",
	Path:                         "/tmp/tidb",
	RunDDL:                       true,
	SplitTable:                   true,
	Lease:                        "45s",
	TokenLimit:                   1000,
	OOMUseTmpStorage:             true,
	OOMAction:                    "log",
	MemQuotaQuery:                32 << 30,
	EnableStreaming:              false,
	CheckMb4ValueInUTF8:          true,
	TreatOldVersionUTF8AsUTF8MB4: true,
	EnableTableLock:              false,
	DelayCleanTableLock:          0,
	SplitRegionMaxNum:            1000,
	TxnLocalLatches: TxnLocalLatches{
		Enabled:  false,
		Capacity: 2048000,
	},
	LowerCaseTableNames: 2,
	Log: Log{
		Level:               "info",
		Format:              "text",
		File:                logutil.NewFileLogConfig(logutil.DefaultLogMaxSize),
		SlowQueryFile:       "tidb-slow.log",
		SlowThreshold:       logutil.DefaultSlowThreshold,
		ExpensiveThreshold:  10000,
		DisableErrorStack:   true,
		QueryLogMaxLen:      logutil.DefaultQueryLogMaxLen,
		RecordPlanInSlowLog: logutil.DefaultRecordPlanInSlowLog,
	},
	Status: Status{
		ReportStatus:    true,
		StatusHost:      "0.0.0.0",
		StatusPort:      10080,
		MetricsInterval: 15,
		RecordQPSbyDB:   false,
	},
	Performance: Performance{
		MaxMemory:           0,
		TCPKeepAlive:        true,
		CrossJoin:           true,
		StatsLease:          "3s",
		RunAutoAnalyze:      true,
		StmtCountLimit:      5000,
		FeedbackProbability: 0.05,
		QueryFeedbackLimit:  1024,
		PseudoEstimateRatio: 0.8,
		ForcePriority:       "NO_PRIORITY",
		BindInfoLease:       "3s",
		TxnTotalSizeLimit:   DefTxnTotalSizeLimit,
	},
	ProxyProtocol: ProxyProtocol{
		Networks:      "",
		HeaderTimeout: 5,
	},
	PreparedPlanCache: PreparedPlanCache{
		Enabled:          false,
		Capacity:         100,
		MemoryGuardRatio: 0.1,
	},
	OpenTracing: OpenTracing{
		Enable: false,
		Sampler: OpenTracingSampler{
			Type:  "const",
			Param: 1.0,
		},
		Reporter: OpenTracingReporter{},
	},
	TiKVClient: TiKVClient{
		GrpcConnectionCount:  16,
		GrpcKeepAliveTime:    10,
		GrpcKeepAliveTimeout: 3,
		CommitTimeout:        "41s",

		MaxBatchSize:      128,
		OverloadThreshold: 200,
		MaxBatchWaitTime:  0,
		BatchWaitSize:     8,

		EnableArrow: true,

		RegionCacheTTL: 600,
	},
	Binlog: Binlog{
		WriteTimeout: "15s",
		Strategy:     "range",
	},
	PessimisticTxn: PessimisticTxn{
		Enable:        true,
		MaxRetryCount: 256,
		TTL:           "40s",
	},
	StmtSummary: StmtSummary{
		MaxStmtCount: 100,
		MaxSQLLength: 4096,
	},
}

var (
	globalConf              = atomic.Value{}
	reloadConfPath          = ""
	confReloader            func(nc, c *Config)
	confReloadLock          sync.Mutex
	supportedReloadConfigs  = make(map[string]struct{}, 32)
	supportedReloadConfList = make([]string, 0, 32)
)

// NewConfig creates a new config instance with default value.
func NewConfig() *Config {
	conf := defaultConf
	return &conf
}

// SetConfReloader sets reload config path and a reloader.
// It should be called only once at start time.
func SetConfReloader(cpath string, reloader func(nc, c *Config), confItems ...string) {
	reloadConfPath = cpath
	confReloader = reloader
	for _, item := range confItems {
		supportedReloadConfigs[item] = struct{}{}
		supportedReloadConfList = append(supportedReloadConfList, item)
	}
}

// GetGlobalConfig returns the global configuration for this server.
// It should store configuration from command line and configuration file.
// Other parts of the system can read the global configuration use this function.
func GetGlobalConfig() *Config {
	return globalConf.Load().(*Config)
}

// StoreGlobalConfig stores a new config to the globalConf. It mostly uses in the test to avoid some data races.
func StoreGlobalConfig(config *Config) {
	globalConf.Store(config)
}

// ReloadGlobalConfig reloads global configuration for this server.
func ReloadGlobalConfig() error {
	confReloadLock.Lock()
	defer confReloadLock.Unlock()

	nc := NewConfig()
	if err := nc.Load(reloadConfPath); err != nil {
		return err
	}
	if err := nc.Valid(); err != nil {
		return err
	}
	c := GetGlobalConfig()

	diffs := collectsDiff(*nc, *c, "")
	if len(diffs) == 0 {
		return nil
	}
	var formattedDiff bytes.Buffer
	for k, vs := range diffs {
		formattedDiff.WriteString(fmt.Sprintf(", %v:%v->%v", k, vs[1], vs[0]))
	}
	unsupported := make([]string, 0, 2)
	for k := range diffs {
		if _, ok := supportedReloadConfigs[k]; !ok {
			unsupported = append(unsupported, k)
		}
	}
	if len(unsupported) > 0 {
		return fmt.Errorf("reloading config %v is not supported, only %v are supported now, "+
			"your changes%s", unsupported, supportedReloadConfList, formattedDiff.String())
	}

	confReloader(nc, c)
	globalConf.Store(nc)
	logutil.BgLogger().Info("reload config changes" + formattedDiff.String())
	return nil
}

// collectsDiff collects different config items.
// map[string][]string -> map[field path][]{new value, old value}
func collectsDiff(i1, i2 interface{}, fieldPath string) map[string][]interface{} {
	diff := make(map[string][]interface{})
	t := reflect.TypeOf(i1)
	if t.Kind() != reflect.Struct {
		if reflect.DeepEqual(i1, i2) {
			return diff
		}
		diff[fieldPath] = []interface{}{i1, i2}
		return diff
	}

	v1 := reflect.ValueOf(i1)
	v2 := reflect.ValueOf(i2)
	for i := 0; i < v1.NumField(); i++ {
		p := t.Field(i).Name
		if fieldPath != "" {
			p = fieldPath + "." + p
		}
		m := collectsDiff(v1.Field(i).Interface(), v2.Field(i).Interface(), p)
		for k, v := range m {
			diff[k] = v
		}
	}
	return diff
}

// Load loads config options from a toml file.
func (c *Config) Load(confFile string) error {
	metaData, err := toml.DecodeFile(confFile, c)
	if c.TokenLimit <= 0 {
		c.TokenLimit = 1000
	}

	// If any items in confFile file are not mapped into the Config struct, issue
	// an error and stop the server from starting.
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 && err == nil {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		err = &ErrConfigValidationFailed{fmt.Sprintf("config file %s contained unknown configuration options: %s", confFile, strings.Join(undecodedItems, ", "))}
	}

	return err
}

// Valid checks if this config is valid.
func (c *Config) Valid() error {
	if c.Security.SkipGrantTable && !hasRootPrivilege() {
		return fmt.Errorf("TiDB run with skip-grant-table need root privilege")
	}
	if _, ok := ValidStorage[c.Store]; !ok {
		nameList := make([]string, 0, len(ValidStorage))
		for k, v := range ValidStorage {
			if v {
				nameList = append(nameList, k)
			}
		}
		return fmt.Errorf("invalid store=%s, valid storages=%v", c.Store, nameList)
	}
	if c.Store == "mocktikv" && !c.RunDDL {
		return fmt.Errorf("can't disable DDL on mocktikv")
	}
	if c.Log.File.MaxSize > MaxLogFileSize {
		return fmt.Errorf("invalid max log file size=%v which is larger than max=%v", c.Log.File.MaxSize, MaxLogFileSize)
	}
	c.OOMAction = strings.ToLower(c.OOMAction)
	if c.OOMAction != OOMActionLog && c.OOMAction != OOMActionCancel {
		return fmt.Errorf("unsupported OOMAction %v, TiDB only supports [%v, %v]", c.OOMAction, OOMActionLog, OOMActionCancel)
	}

	// lower_case_table_names is allowed to be 0, 1, 2
	if c.LowerCaseTableNames < 0 || c.LowerCaseTableNames > 2 {
		return fmt.Errorf("lower-case-table-names should be 0 or 1 or 2")
	}

	if c.TxnLocalLatches.Enabled && c.TxnLocalLatches.Capacity == 0 {
		return fmt.Errorf("txn-local-latches.capacity can not be 0")
	}

	// For tikvclient.
	if c.TiKVClient.GrpcConnectionCount == 0 {
		return fmt.Errorf("grpc-connection-count should be greater than 0")
	}
	if c.PessimisticTxn.TTL != "" {
		dur, err := time.ParseDuration(c.PessimisticTxn.TTL)
		if err != nil {
			return err
		}
		if dur < MinPessimisticTTL || dur > MaxPessimisticTTL {
			return fmt.Errorf("pessimistic transaction ttl %s out of range [%s, %s]",
				dur, MinPessimisticTTL, MaxPessimisticTTL)
		}
	}
	return nil
}

func hasRootPrivilege() bool {
	return os.Geteuid() == 0
}

// TableLockEnabled uses to check whether enabled the table lock feature.
func TableLockEnabled() bool {
	return GetGlobalConfig().EnableTableLock
}

// TableLockDelayClean uses to get the time of delay clean table lock.
var TableLockDelayClean = func() uint64 {
	return GetGlobalConfig().DelayCleanTableLock
}

// ToLogConfig converts *Log to *logutil.LogConfig.
func (l *Log) ToLogConfig() *logutil.LogConfig {
	return logutil.NewLogConfig(l.Level, l.Format, l.SlowQueryFile, l.File, l.DisableTimestamp, func(config *zaplog.Config) { config.DisableErrorVerbose = l.DisableErrorStack })
}

// ToTracingConfig converts *OpenTracing to *tracing.Configuration.
func (t *OpenTracing) ToTracingConfig() *tracing.Configuration {
	ret := &tracing.Configuration{
		Disabled:   !t.Enable,
		RPCMetrics: t.RPCMetrics,
		Reporter:   &tracing.ReporterConfig{},
		Sampler:    &tracing.SamplerConfig{},
	}
	ret.Reporter.QueueSize = t.Reporter.QueueSize
	ret.Reporter.BufferFlushInterval = t.Reporter.BufferFlushInterval
	ret.Reporter.LogSpans = t.Reporter.LogSpans
	ret.Reporter.LocalAgentHostPort = t.Reporter.LocalAgentHostPort

	ret.Sampler.Type = t.Sampler.Type
	ret.Sampler.Param = t.Sampler.Param
	ret.Sampler.SamplingServerURL = t.Sampler.SamplingServerURL
	ret.Sampler.MaxOperations = t.Sampler.MaxOperations
	ret.Sampler.SamplingRefreshInterval = t.Sampler.SamplingRefreshInterval
	return ret
}

func init() {
	globalConf.Store(&defaultConf)
	if checkBeforeDropLDFlag == "1" {
		CheckTableBeforeDrop = true
	}
}

// The following constants represents the valid action configurations for OOMAction.
// NOTE: Although the values is case insensitive, we should use lower-case
// strings because the configuration value will be transformed to lower-case
// string and compared with these constants in the further usage.
const (
	OOMActionCancel = "cancel"
	OOMActionLog    = "log"
)
