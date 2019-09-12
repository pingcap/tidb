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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"strings"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
<<<<<<< HEAD
	"github.com/pingcap/parser/mysql"
=======
	zaplog "github.com/pingcap/log"
>>>>>>> cbf4ddc... *: improve the format of the error log (#12155)
	"github.com/pingcap/tidb/util/logutil"
	tracing "github.com/uber/jaeger-client-go/config"
)

// Config number limitations
const (
	MaxLogFileSize = 4096 // MB
	// DefTxnEntryCountLimit is the default value of TxnEntryCountLimit.
	DefTxnEntryCountLimit = 300 * 1000
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
	Store            string          `toml:"store" json:"store"`
	Path             string          `toml:"path" json:"path"`
	Socket           string          `toml:"socket" json:"socket"`
	Lease            string          `toml:"lease" json:"lease"`
	RunDDL           bool            `toml:"run-ddl" json:"run-ddl"`
	SplitTable       bool            `toml:"split-table" json:"split-table"`
	TokenLimit       uint            `toml:"token-limit" json:"token-limit"`
	OOMAction        string          `toml:"oom-action" json:"oom-action"`
	MemQuotaQuery    int64           `toml:"mem-quota-query" json:"mem-quota-query"`
	EnableStreaming  bool            `toml:"enable-streaming" json:"enable-streaming"`
	EnableBatchDML   bool            `toml:"enable-batch-dml" json:"enable-batch-dml"`
	TxnLocalLatches  TxnLocalLatches `toml:"txn-local-latches" json:"txn-local-latches"`
	// Set sys variable lower-case-table-names, ref: https://dev.mysql.com/doc/refman/5.7/en/identifier-case-sensitivity.html.
	// TODO: We actually only support mode 2, which keeps the original case, but the comparison is case-insensitive.
	LowerCaseTableNames int               `toml:"lower-case-table-names" json:"lower-case-table-names"`
	ServerVersion       string            `toml:"server-version" json:"server-version"`
	Log                 Log               `toml:"log" json:"log"`
	Security            Security          `toml:"security" json:"security"`
	Status              Status            `toml:"status" json:"status"`
	Performance         Performance       `toml:"performance" json:"performance"`
	XProtocol           XProtocol         `toml:"xprotocol" json:"xprotocol"`
	PreparedPlanCache   PreparedPlanCache `toml:"prepared-plan-cache" json:"prepared-plan-cache"`
	OpenTracing         OpenTracing       `toml:"opentracing" json:"opentracing"`
	ProxyProtocol       ProxyProtocol     `toml:"proxy-protocol" json:"proxy-protocol"`
	TiKVClient          TiKVClient        `toml:"tikv-client" json:"tikv-client"`
	Binlog              Binlog            `toml:"binlog" json:"binlog"`
	CompatibleKillQuery bool              `toml:"compatible-kill-query" json:"compatible-kill-query"`
	CheckMb4ValueInUTF8 bool              `toml:"check-mb4-value-in-utf8" json:"check-mb4-value-in-utf8"`
	// TreatOldVersionUTF8AsUTF8MB4 is use to treat old version table/column UTF8 charset as UTF8MB4. This is for compatibility.
	// Currently not support dynamic modify, because this need to reload all old version schema.
	TreatOldVersionUTF8AsUTF8MB4 bool   `toml:"treat-old-version-utf8-as-utf8mb4" json:"treat-old-version-utf8-as-utf8mb4"`
	Plugin                       Plugin `toml:"plugin" json:"plugin"`
	SplitRegionMaxNum            uint64 `toml:"split-region-max-num" json:"split-region-max-num"`
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

	SlowQueryFile      string `toml:"slow-query-file" json:"slow-query-file"`
	SlowThreshold      uint64 `toml:"slow-threshold" json:"slow-threshold"`
	ExpensiveThreshold uint   `toml:"expensive-threshold" json:"expensive-threshold"`
	QueryLogMaxLen     uint64 `toml:"query-log-max-len" json:"query-log-max-len"`
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
	ReportStatus    bool   `toml:"report-status" json:"report-status"`
	StatusPort      uint   `toml:"status-port" json:"status-port"`
	MetricsAddr     string `toml:"metrics-addr" json:"metrics-addr"`
	MetricsInterval uint   `toml:"metrics-interval" json:"metrics-interval"`
}

// Performance is the performance section of the config.
type Performance struct {
	MaxProcs            uint    `toml:"max-procs" json:"max-procs"`
	TCPKeepAlive        bool    `toml:"tcp-keep-alive" json:"tcp-keep-alive"`
	CrossJoin           bool    `toml:"cross-join" json:"cross-join"`
	StatsLease          string  `toml:"stats-lease" json:"stats-lease"`
	EnableUpdateStats   bool    `toml:"enable-update-stats" json:"enable-update-stats"`
	RunAutoAnalyze      bool    `toml:"run-auto-analyze" json:"run-auto-analyze"`
	StmtCountLimit      uint    `toml:"stmt-count-limit" json:"stmt-count-limit"`
	FeedbackProbability float64 `toml:"feedback-probability" json:"feedback-probability"`
	QueryFeedbackLimit  uint    `toml:"query-feedback-limit" json:"query-feedback-limit"`
	PseudoEstimateRatio float64 `toml:"pseudo-estimate-ratio" json:"pseudo-estimate-ratio"`
	ForcePriority       string  `toml:"force-priority" json:"force-priority"`
	TxnEntryCountLimit  uint64  `toml:"txn-entry-count-limit" json:"txn-entry-count-limit"`
	TxnTotalSizeLimit   uint64  `toml:"txn-total-size-limit" json:"txn-total-size-limit"`
}

// XProtocol is the XProtocol section of the config.
type XProtocol struct {
	XServer bool   `toml:"xserver" json:"xserver"`
	XHost   string `toml:"xhost" json:"xhost"`
	XPort   uint   `toml:"xport" json:"xport"`
	XSocket string `toml:"xsocket" json:"xsocket"`
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
	Enabled  bool `toml:"enabled" json:"enabled"`
	Capacity uint `toml:"capacity" json:"capacity"`
}

// OpenTracing is the opentracing section of the config.
type OpenTracing struct {
	Enable     bool                `toml:"enable" json:"enable"`
	Sampler    OpenTracingSampler  `toml:"sampler" json:"sampler"`
	Reporter   OpenTracingReporter `toml:"reporter" json:"reporter"`
	RPCMetrics bool                `toml:"rpc-metrics" json:"rpc-metrics"`
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
	// MaxTxnTimeUse is the max time a Txn may use (in seconds) from its startTS to commitTS.
	MaxTxnTimeUse uint `toml:"max-txn-time-use" json:"max-txn-time-use"`
}

// Plugin is the config for plugin
type Plugin struct {
	Dir  string `toml:"dir" json:"dir"`
	Load string `toml:"load" json:"load"`
}

// Binlog is the config for binlog.
type Binlog struct {
	Enable       bool   `toml:"enable" json:"enable"`
	WriteTimeout string `toml:"write-timeout" json:"write-timeout"`
	// If IgnoreError is true, when writting binlog meets error, TiDB would
	// ignore the error.
	IgnoreError bool `toml:"ignore-error" json:"ignore-error"`
	// Use socket file to write binlog, for compatible with kafka version tidb-binlog.
	BinlogSocket string `toml:"binlog-socket" json:"binlog-socket"`
}

var defaultConf = Config{
	Host:                         "0.0.0.0",
	AdvertiseAddress:             "",
	Port:                         4000,
	Store:                        "mocktikv",
	Path:                         "/tmp/tidb",
	RunDDL:                       true,
	SplitTable:                   true,
	Lease:                        "45s",
	TokenLimit:                   1000,
	OOMAction:                    "log",
	MemQuotaQuery:                32 << 30,
	EnableStreaming:              false,
	EnableBatchDML:               false,
	CheckMb4ValueInUTF8:          true,
	TreatOldVersionUTF8AsUTF8MB4: true,
	SplitRegionMaxNum:            1000,
	TxnLocalLatches: TxnLocalLatches{
		Enabled:  false,
		Capacity: 10240000,
	},
	LowerCaseTableNames: 2,
	ServerVersion:       "",
	Log: Log{
		Level:              "info",
		Format:             "text",
		File:               logutil.NewFileLogConfig(true, logutil.DefaultLogMaxSize),
		SlowQueryFile:      "tidb-slow.log",
		SlowThreshold:      logutil.DefaultSlowThreshold,
		ExpensiveThreshold: 10000,
		DisableErrorStack:  true,
		QueryLogMaxLen:     logutil.DefaultQueryLogMaxLen,
	},
	Status: Status{
		ReportStatus:    true,
		StatusPort:      10080,
		MetricsInterval: 15,
	},
	Performance: Performance{
		TCPKeepAlive:        true,
		CrossJoin:           true,
		StatsLease:          "3s",
		EnableUpdateStats:   true,
		RunAutoAnalyze:      true,
		StmtCountLimit:      5000,
		FeedbackProbability: 0.05,
		QueryFeedbackLimit:  1024,
		PseudoEstimateRatio: 0.8,
		ForcePriority:       "NO_PRIORITY",
		TxnEntryCountLimit:  DefTxnEntryCountLimit,
		TxnTotalSizeLimit:   DefTxnTotalSizeLimit,
	},
	XProtocol: XProtocol{
		XHost: "",
		XPort: 0,
	},
	ProxyProtocol: ProxyProtocol{
		Networks:      "",
		HeaderTimeout: 5,
	},
	PreparedPlanCache: PreparedPlanCache{
		Enabled:  false,
		Capacity: 100,
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
		MaxTxnTimeUse:        590,
	},
	Binlog: Binlog{
		WriteTimeout: "15s",
	},
}

var globalConf = atomic.Value{}

// NewConfig creates a new config instance with default value.
func NewConfig() *Config {
	conf := defaultConf
	return &conf
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

// Load loads config options from a toml file.
func (c *Config) Load(confFile string) error {
	metaData, err := toml.DecodeFile(confFile, c)
	if c.TokenLimit <= 0 {
		c.TokenLimit = 1000
	}
	if len(c.ServerVersion) > 0 {
		mysql.ServerVersion = c.ServerVersion
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
// NOTE: Althrough the values is case insensitiv, we should use lower-case
// strings because the configuration value will be transformed to lower-case
// string and compared with these constants in the further usage.
const (
	OOMActionCancel = "cancel"
	OOMActionLog    = "log"
)
