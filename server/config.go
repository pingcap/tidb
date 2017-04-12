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

package server

// Config contains configuration options.
type Config struct {
	Addr         string `json:"addr" toml:"addr"`
	LogLevel     string `json:"log_level" toml:"log_level"`
	SkipAuth     bool   `json:"skip_auth" toml:"skip_auth"`
	StatusAddr   string `json:"status_addr" toml:"status_addr"`
	Socket       string `json:"socket" toml:"socket"`
	ReportStatus bool   `json:"report_status" toml:"report_status"`
	StorePath    string `json:"store_path" toml:"store_path"`
	Store        string `json:"store" toml:"store"`
}
