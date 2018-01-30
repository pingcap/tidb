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
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/logutil"
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
	TokenLimit   int    `toml:"token-limit" json:"token-limit"`

	Log         Log         `toml:"log" json:"log"`
	Security    Security    `toml:"security" json:"security"`
	Status      Status      `toml:"status" json:"status"`
	Performance Performance `toml:"performance" json:"performance"`
	XProtocol   XProtocol   `toml:"xprotocol" json:"xprotocol"`
	PlanCache   PlanCache   `toml:"plan-cache" json:"plan-cache"`
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
	StmtCountLimit  int    `toml:"stmt-count-limit" json:"stmt-count-limit"`
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
	Enabled  bool  `toml:"plan-cache-enabled" json:"plan-cache-enabled"`
	Capacity int64 `toml:"plan-cache-capacity" json:"plan-cache-capacity"`
	Shards   int64 `toml:"plan-cache-shards" json:"plan-cache-shards"`
}

var defaultConf = Config{
	Host:       "0.0.0.0",
	Port:       4000,
	Store:      "mocktikv",
	Path:       "/tmp/tidb",
	RunDDL:     true,
	Lease:      "10s",
	TokenLimit: 1000,
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
		StmtCountLimit:  5000,
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
	if c.TokenLimit <= 0 {
		c.TokenLimit = 1000
	}
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
