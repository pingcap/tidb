// Copyright 2026 PingCAP, Inc.
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

package errmsg

import (
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestExtendByRegex(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.ErrorMessageExtensions = []config.ErrorMessageExtension{
		{Pattern: `^Access denied for user '.+'@'.+' \(using password: (YES|NO)\)$`, Suffix: "see https://docs.pingcap.com/tidbcloud/select-cluster-tier#user-name-prefix for more details"},
		{Pattern: `^require_secure_transport can not be set to ON with SEM\(security enhanced mode\) enabled$`, Suffix: "see https://docs.pingcap.com/tidbcloud/secure-connections-to-serverless-tier-clusters for more details"},
		{Pattern: `^sleep\(\) argument is greater than [0-9]+$`, Suffix: "see https://docs.pingcap.com/tidbcloud/serverless-tier-limitations#sql for more details"},
		{Pattern: `^[A-Z ]+ command denied to user '[^']+'@'[^']+' for table '[^']+'$`, Suffix: "see https://docs.pingcap.com/tidbcloud/limited-sql-features#system-tables for more details"},
		{Pattern: `^Access denied; you need \(at least one of\) the RESTRICTED_VARIABLES_ADMIN privilege\(s\) for this operation$`, Suffix: "see https://docs.pingcap.com/tidbcloud/limited-sql-features#system-variables for more details"},
		{Pattern: `^Feature '.+' is not supported when security enhanced mode is enabled$`, Suffix: "see https://docs.pingcap.com/tidbcloud/limited-sql-features#statements for more details"},
		{Pattern: `^Error message\.$`, Suffix: "suffix."},
		{Pattern: `^Error message without period$`, Suffix: "suffix"},
		{Pattern: `^Error message with multiple periods\.\.\.$`, Suffix: "suffix..."},
		{Pattern: `^Error message with empty suffix$`, Suffix: ""},
	}
	config.StoreGlobalConfig(&newCfg)
	t.Cleanup(func() {
		config.StoreGlobalConfig(originCfg)
	})

	tests := []struct {
		name     string
		message  string
		expected string
	}{
		{
			name:     "user prefix",
			message:  "Access denied for user 'root.foo'@'127.0.0.1' (using password: YES)",
			expected: "Access denied for user 'root.foo'@'127.0.0.1' (using password: YES), see https://docs.pingcap.com/tidbcloud/select-cluster-tier#user-name-prefix for more details.",
		},
		{
			name:     "max sleep seconds",
			message:  "sleep() argument is greater than 31536000",
			expected: "sleep() argument is greater than 31536000, see https://docs.pingcap.com/tidbcloud/serverless-tier-limitations#sql for more details.",
		},
		{
			name:     "require secure transport",
			message:  "require_secure_transport can not be set to ON with SEM(security enhanced mode) enabled",
			expected: "require_secure_transport can not be set to ON with SEM(security enhanced mode) enabled, see https://docs.pingcap.com/tidbcloud/secure-connections-to-serverless-tier-clusters for more details.",
		},
		{
			name:     "resource unit",
			message:  "Exceeded resource group quota limitation",
			expected: "Exceeded resource group quota limitation",
		},
		{
			name:     "serverless not support",
			message:  "Feature 'SELECT INTO' is not supported when security enhanced mode is enabled",
			expected: "Feature 'SELECT INTO' is not supported when security enhanced mode is enabled, see https://docs.pingcap.com/tidbcloud/limited-sql-features#statements for more details.",
		},
		{
			name:     "invisible table",
			message:  "SELECT command denied to user 'u'@'%' for table 'tidb'",
			expected: "SELECT command denied to user 'u'@'%' for table 'tidb', see https://docs.pingcap.com/tidbcloud/limited-sql-features#system-tables for more details.",
		},
		{
			name:     "invisible sysvar",
			message:  "Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation",
			expected: "Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation, see https://docs.pingcap.com/tidbcloud/limited-sql-features#system-variables for more details.",
		},
		{
			name:     "unmatched",
			message:  "Table 'test.t' doesn't exist",
			expected: "Table 'test.t' doesn't exist",
		},
		{
			name:     "message ending with period",
			message:  "Error message.",
			expected: "Error message, suffix.",
		},
		{
			name:     "message and suffix without period",
			message:  "Error message without period",
			expected: "Error message without period, suffix.",
		},
		{
			name:     "message and suffix ending with multiple periods",
			message:  "Error message with multiple periods...",
			expected: "Error message with multiple periods, suffix.",
		},
		{
			name:     "empty suffix",
			message:  "Error message with empty suffix",
			expected: "Error message with empty suffix",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlErr := mysql.NewErrf(mysql.ErrUnknown, "%s", nil, tt.message)
			Extend(sqlErr)
			require.Equal(t, tt.expected, sqlErr.Message)
		})
	}
}

func TestExtendWithoutConfig(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.ErrorMessageExtensions = nil
	config.StoreGlobalConfig(&newCfg)
	t.Cleanup(func() {
		config.StoreGlobalConfig(originCfg)
	})

	sqlErr := mysql.NewErrf(mysql.ErrUnknown, "%s", nil, "Exceeded resource group quota limitation")
	Extend(sqlErr)
	require.Equal(t, "Exceeded resource group quota limitation", sqlErr.Message)
}

func TestExtendSkipsInvalidRegex(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.ErrorMessageExtensions = []config.ErrorMessageExtension{
		{Pattern: `[`, Suffix: "invalid regex"},
		{Pattern: `^sleep\(\) argument is greater than [0-9]+$`, Suffix: "see https://docs.pingcap.com/tidbcloud/serverless-tier-limitations#sql for more details"},
	}
	config.StoreGlobalConfig(&newCfg)
	t.Cleanup(func() {
		config.StoreGlobalConfig(originCfg)
	})

	sqlErr := mysql.NewErrf(mysql.ErrUnknown, "%s", nil, "sleep() argument is greater than 31536000")
	Extend(sqlErr)
	require.Equal(t, "sleep() argument is greater than 31536000, see https://docs.pingcap.com/tidbcloud/serverless-tier-limitations#sql for more details.", sqlErr.Message)
}

func TestExtendPrefersLongestPattern(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.ErrorMessageExtensions = []config.ErrorMessageExtension{
		{Pattern: `^Access denied`, Suffix: "generic access denied message"},
		{Pattern: `^Access denied for user '.+'@'.+' \(using password: (YES|NO)\)$`, Suffix: "specific user prefix message"},
	}
	config.StoreGlobalConfig(&newCfg)
	t.Cleanup(func() {
		config.StoreGlobalConfig(originCfg)
	})

	sqlErr := mysql.NewErrf(mysql.ErrUnknown, "%s", nil, "Access denied for user 'root.foo'@'127.0.0.1' (using password: YES)")
	Extend(sqlErr)
	require.Equal(t, "Access denied for user 'root.foo'@'127.0.0.1' (using password: YES), specific user prefix message.", sqlErr.Message)
}

func TestExtendConcurrentWithStoreGlobalConfig(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.ErrorMessageExtensions = []config.ErrorMessageExtension{
		{Pattern: `^Access denied`, Suffix: "generic access denied message"},
		{Pattern: `^Access denied for user '.+'@'.+' \(using password: (YES|NO)\)$`, Suffix: "specific user prefix message"},
	}
	config.StoreGlobalConfig(&newCfg)
	t.Cleanup(func() {
		config.StoreGlobalConfig(originCfg)
	})

	publishedCfg := config.GetGlobalConfig()
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		<-start
		for range 1000 {
			config.StoreGlobalConfig(publishedCfg)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for range 1000 {
			sqlErr := mysql.NewErrf(mysql.ErrUnknown, "%s", nil, "Access denied for user 'root.foo'@'127.0.0.1' (using password: YES)")
			Extend(sqlErr)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := range 1000 {
			config.GetGlobalConfig().Instance.EnableSlowLog.Store(i%2 == 0)
		}
	}()

	close(start)
	wg.Wait()
}
