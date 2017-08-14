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

import "sync"

// Config contains configuration options.
type Config struct {
	Addr           string `json:"addr" toml:"addr"`
	LogLevel       string `json:"log_level" toml:"log_level"`
	SkipAuth       bool   `json:"skip_auth" toml:"skip_auth"`
	StatusAddr     string `json:"status_addr" toml:"status_addr"`
	Socket         string `json:"socket" toml:"socket"`
	ReportStatus   bool   `json:"report_status" toml:"report_status"`
	StorePath      string `json:"store_path" toml:"store_path"`
	Store          string `json:"store" toml:"store"`
	SlowThreshold  int    `json:"slow_threshold" toml:"slow_threshold"`
	QueryLogMaxlen int    `json:"query_log_max_len" toml:"query_log_max_len"`
	TCPKeepAlive   bool   `json:"tcp_keep_alive" toml:"tcp_keep_alive"`
}

var cfg *Config
var once sync.Once

// GetGlobalConfig returns the global configuration for this server.
// It should store configuration from command line and configuration file.
// Other parts of the system can read the global configuration use this function.
func GetGlobalConfig() *Config {
	once.Do(func() {
		cfg = &Config{
			SlowThreshold:  300,
			QueryLogMaxlen: 2048,
		}
	})
	return cfg
}
