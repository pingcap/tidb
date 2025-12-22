// Copyright 2025 PingCAP, Inc.
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

package importsdk

import (
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// SDKOption customizes the SDK configuration
type SDKOption func(*SDKConfig)

// SDKConfig is the configuration for the SDK
type SDKConfig struct {
	// Loader options
	concurrency    int
	sqlMode        mysql.SQLMode
	fileRouteRules []*config.FileRouteRule
	filter         []string
	charset        string

	// General options
	logger log.Logger
}

func defaultSDKConfig() *SDKConfig {
	return &SDKConfig{
		concurrency: 4,
		filter:      config.GetDefaultFilter(),
		logger:      log.L(),
		charset:     "auto",
	}
}

// WithConcurrency sets the number of concurrent DB/Table creation workers.
func WithConcurrency(n int) SDKOption {
	return func(cfg *SDKConfig) {
		if n > 0 {
			cfg.concurrency = n
		}
	}
}

// WithLogger specifies a custom logger
func WithLogger(logger log.Logger) SDKOption {
	return func(cfg *SDKConfig) {
		cfg.logger = logger
	}
}

// WithSQLMode specifies the SQL mode for schema parsing
func WithSQLMode(mode mysql.SQLMode) SDKOption {
	return func(cfg *SDKConfig) {
		cfg.sqlMode = mode
	}
}

// WithFilter specifies a filter for the loader
func WithFilter(filter []string) SDKOption {
	return func(cfg *SDKConfig) {
		cfg.filter = filter
	}
}

// WithFileRouters specifies custom file routing rules
func WithFileRouters(routers []*config.FileRouteRule) SDKOption {
	return func(cfg *SDKConfig) {
		cfg.fileRouteRules = routers
	}
}

// WithCharset specifies the character set for import (default "auto").
func WithCharset(cs string) SDKOption {
	return func(cfg *SDKConfig) {
		if cs != "" {
			cfg.charset = cs
		}
	}
}
