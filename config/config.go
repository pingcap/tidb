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
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/logutil"
	tracing "github.com/uber/jaeger-client-go/config"
)

// Config contains configuration options.
type Config struct {
	Host         string `toml:"host" json:"host"`
	Port         int    `toml:"port" json:"port"`
	Store        string `toml:"store" json:"store"`
	Path         string `toml:"path" json:"path"`
	Socket       string `toml:"socket" json:"socket"`
	BinlogSocket string `toml:"binlog-socket" json:"binlog-socket"`
	Lease        string `toml:"lease" json:"lease"`
	RunDDL       bool   `toml:"run-ddl" json:"run-ddl"`
	SplitTable   bool   `toml:"split-table" json:"split-table"`

	Log               Log               `toml:"log" json:"log"`
	Security          Security          `toml:"security" json:"security"`
	Status            Status            `toml:"status" json:"status"`
	Performance       Performance       `toml:"performance" json:"performance"`
	XProtocol         XProtocol         `toml:"xprotocol" json:"xprotocol"`
	PlanCache         PlanCache         `toml:"plan-cache" json:"plan-cache"`
	PreparedPlanCache PreparedPlanCache `toml:"prepared-plan-cache" json:"prepared-plan-cache"`
	OpenTracing       OpenTracing       `toml:"opentracing" json:"opentracing"`
}

// Log is the log section of config.
type Log struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// Log format. one of json, text, or console.
	Format string `toml:"format" json:"format"`
	// Disable automatic timestamps in output.
	DisableTimestamp bool `toml:"disable-timestamp" json:"disable-timestamp"`
	// File log config.
	File logutil.FileLogConfig `toml:"file" json:"file"`

	SlowQueryFile  string `toml:"slow-query-file" json:"slow-query-file"`
	SlowThreshold  int    `toml:"slow-threshold" json:"slow-threshold"`
	QueryLogMaxLen int    `toml:"query-log-max-len" json:"query-log-max-len"`
}

// Security is the security section of the config.
type Security struct {
	SkipGrantTable bool   `toml:"skip-grant-table" json:"skip-grant-table"`
	SSLCA          string `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert        string `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey         string `toml:"ssl-key" json:"ssl-key"`
}

// Status is the status section of the config.
type Status struct {
	ReportStatus    bool   `toml:"report-status" json:"report-status"`
	StatusPort      int    `toml:"status-port" json:"status-port"`
	MetricsAddr     string `toml:"metrics-addr" json:"metrics-addr"`
	MetricsInterval int    `toml:"metrics-interval" json:"metrics-interval"`
}

// Performance is the performance section of the config.
type Performance struct {
	TCPKeepAlive    bool   `toml:"tcp-keep-alive" json:"tcp-keep-alive"`
	RetryLimit      int    `toml:"retry-limit" json:"retry-limit"`
	JoinConcurrency int    `toml:"join-concurrency" json:"join-concurrency"`
	CrossJoin       bool   `toml:"cross-join" json:"cross-join"`
	StatsLease      string `toml:"stats-lease" json:"stats-lease"`
	RunAutoAnalyze  bool   `toml:"run-auto-analyze" json:"run-auto-analyze"`
}

// XProtocol is the XProtocol section of the config.
type XProtocol struct {
	XServer bool   `toml:"xserver" json:"xserver"`
	XHost   string `toml:"xhost" json:"xhost"`
	XPort   int    `toml:"xport" json:"xport"`
	XSocket string `toml:"xsocket" json:"xsocket"`
}

// PlanCache is the PlanCache section of the config.
type PlanCache struct {
	Enabled  bool  `toml:"enabled" json:"enabled"`
	Capacity int64 `toml:"capacity" json:"capacity"`
	Shards   int64 `toml:"shards" json:"shards"`
}

// PreparedPlanCache is the PreparedPlanCache section of the config.
type PreparedPlanCache struct {
	Enabled  bool  `toml:"enabled" json:"enabled"`
	Capacity int64 `toml:"capacity" json:"capacity"`
}

// OpenTracing is the opentracing section of the config.
type OpenTracing struct {
	Enable     bool                `toml:"enable" json:"enbale"`
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

var defaultConf = Config{
	Host:   "0.0.0.0",
	Port:   4000,
	Store:  "mocktikv",
	Path:   "/tmp/tidb",
	RunDDL: true,
	Lease:  "10s",
	Log: Log{
		Level:  "info",
		Format: "text",
		File: logutil.FileLogConfig{
			LogRotate: true,
		},
		SlowThreshold:  300,
		QueryLogMaxLen: 2048,
	},
	Status: Status{
		ReportStatus:    true,
		StatusPort:      10080,
		MetricsInterval: 15,
	},
	Performance: Performance{
		TCPKeepAlive:    true,
		RetryLimit:      10,
		JoinConcurrency: 5,
		CrossJoin:       true,
		StatsLease:      "3s",
		RunAutoAnalyze:  true,
	},
	XProtocol: XProtocol{
		XHost: "0.0.0.0",
		XPort: 14000,
	},
	PlanCache: PlanCache{
		Enabled:  false,
		Capacity: 2560,
		Shards:   256,
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
}

var globalConf = defaultConf

// NewConfig creates a new config instance with default value.
func NewConfig() *Config {
	conf := defaultConf
	return &conf
}

// GetGlobalConfig returns the global configuration for this server.
// It should store configuration from command line and configuration file.
// Other parts of the system can read the global configuration use this function.
func GetGlobalConfig() *Config {
	return &globalConf
}

// Load loads config options from a toml file.
func (c *Config) Load(confFile string) error {
	_, err := toml.DecodeFile(confFile, c)
	return errors.Trace(err)
}

// ToLogConfig converts *Log to *logutil.LogConfig.
func (l *Log) ToLogConfig() *logutil.LogConfig {
	return &logutil.LogConfig{
		Level:            l.Level,
		Format:           l.Format,
		DisableTimestamp: l.DisableTimestamp,
		File:             l.File,
		SlowQueryFile:    l.SlowQueryFile,
	}
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
