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
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestExtendErrorMessageByRegex(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.ExtendedErrorMsgs = map[string]string{
		`^Access denied for user '.+'@'.+' \(using password: (YES|NO)\)$`:                                                "see https://docs.pingcap.com/tidbcloud/select-cluster-tier#user-name-prefix for more details",
		`^require_secure_transport can not be set to ON with SEM\(security enhanced mode\) enabled$`:                     "see https://docs.pingcap.com/tidbcloud/secure-connections-to-serverless-tier-clusters for more details",
		`^sleep\(\) argument is greater than [0-9]+$`:                                                                    "see https://docs.pingcap.com/tidbcloud/serverless-tier-limitations#sql for more details",
		`^[A-Z ]+ command denied to user '[^']+'@'[^']+' for table '[^']+'$`:                                             "see https://docs.pingcap.com/tidbcloud/limited-sql-features#system-tables for more details",
		`^Access denied; you need \(at least one of\) the RESTRICTED_VARIABLES_ADMIN privilege\(s\) for this operation$`: "see https://docs.pingcap.com/tidbcloud/limited-sql-features#system-variables for more details",
		`^Feature '.+' is not supported when security enhanced mode is enabled$`:                                         "see https://docs.pingcap.com/tidbcloud/limited-sql-features#statements for more details",
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlErr := mysql.NewErrf(mysql.ErrUnknown, "%s", nil, tt.message)
			ExtendErrorMessage(sqlErr)
			require.Equal(t, tt.expected, sqlErr.Message)
		})
	}
}

func TestExtendErrorMessageWithoutConfig(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.ExtendedErrorMsgs = nil
	config.StoreGlobalConfig(&newCfg)
	t.Cleanup(func() {
		config.StoreGlobalConfig(originCfg)
	})

	sqlErr := mysql.NewErrf(mysql.ErrUnknown, "%s", nil, "Exceeded resource group quota limitation")
	ExtendErrorMessage(sqlErr)
	require.Equal(t, "Exceeded resource group quota limitation", sqlErr.Message)
}

func TestExtendErrorMessageSkipsInvalidRegex(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.ExtendedErrorMsgs = map[string]string{
		`[`: "invalid regex",
		`^sleep\(\) argument is greater than [0-9]+$`: "see https://docs.pingcap.com/tidbcloud/serverless-tier-limitations#sql for more details",
	}
	config.StoreGlobalConfig(&newCfg)
	t.Cleanup(func() {
		config.StoreGlobalConfig(originCfg)
	})

	sqlErr := mysql.NewErrf(mysql.ErrUnknown, "%s", nil, "sleep() argument is greater than 31536000")
	ExtendErrorMessage(sqlErr)
	require.Equal(t, "sleep() argument is greater than 31536000, see https://docs.pingcap.com/tidbcloud/serverless-tier-limitations#sql for more details.", sqlErr.Message)
}

func TestExtendErrorMessagePrefersLongestPattern(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.ExtendedErrorMsgs = map[string]string{
		`^Access denied`: "generic access denied message",
		`^Access denied for user '.+'@'.+' \(using password: (YES|NO)\)$`: "specific user prefix message",
	}
	config.StoreGlobalConfig(&newCfg)
	t.Cleanup(func() {
		config.StoreGlobalConfig(originCfg)
	})

	sqlErr := mysql.NewErrf(mysql.ErrUnknown, "%s", nil, "Access denied for user 'root.foo'@'127.0.0.1' (using password: YES)")
	ExtendErrorMessage(sqlErr)
	require.Equal(t, "Access denied for user 'root.foo'@'127.0.0.1' (using password: YES), specific user prefix message.", sqlErr.Message)
}
