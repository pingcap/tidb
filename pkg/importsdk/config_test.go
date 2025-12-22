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
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestDefaultSDKConfig(t *testing.T) {
	cfg := defaultSDKConfig()
	require.Equal(t, 4, cfg.concurrency)
	require.Equal(t, config.GetDefaultFilter(), cfg.filter)
	require.Equal(t, log.L(), cfg.logger)
	require.Equal(t, "auto", cfg.charset)
}

func TestSDKOptions(t *testing.T) {
	cfg := defaultSDKConfig()

	// Test WithConcurrency
	WithConcurrency(10)(cfg)
	require.Equal(t, 10, cfg.concurrency)
	WithConcurrency(-1)(cfg) // Should ignore invalid value
	require.Equal(t, 10, cfg.concurrency)

	// Test WithLogger
	logger := log.L()
	WithLogger(logger)(cfg)
	require.Equal(t, logger, cfg.logger)

	// Test WithSQLMode
	mode := mysql.ModeStrictTransTables
	WithSQLMode(mode)(cfg)
	require.Equal(t, mode, cfg.sqlMode)

	// Test WithFilter
	filter := []string{"*.*"}
	WithFilter(filter)(cfg)
	require.Equal(t, filter, cfg.filter)

	// Test WithFileRouters
	routers := []*config.FileRouteRule{{Schema: "test"}}
	WithFileRouters(routers)(cfg)
	require.Equal(t, routers, cfg.fileRouteRules)

	// Test WithCharset
	WithCharset("utf8mb4")(cfg)
	require.Equal(t, "utf8mb4", cfg.charset)
	WithCharset("")(cfg) // Should ignore empty value
	require.Equal(t, "utf8mb4", cfg.charset)
}
