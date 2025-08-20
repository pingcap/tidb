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

package sem

import (
	"os"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func parseSEMConfig(t *testing.T, configStr string) (*Config, error) {
	// write config to tempdir
	tempFile, err := os.CreateTemp(t.TempDir(), "sem_config_*.json")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	_, err = tempFile.WriteString(configStr)
	require.NoError(t, err)
	tempFile.Close()
	require.NoError(t, err)
	return parseSEMConfigFromFile(tempFile.Name())
}

func TestParseConfigWithDifferentFormat(t *testing.T) {
	cases := []struct {
		name    string
		config  string
		wantErr bool
	}{
		{
			name: "valid config",
			config: `{
				"version": "1.0",
				"tidb_version": "v6.0.0",
				"restricted_databases": ["mysql", "test"],
				"restricted_tables": [
					{
						"schema": "test",
						"name": "t1",
						"hidden": false,
						"columns": [
							{"name": "c1", "hidden": true, "value": "default"}
						]
					}
				],
				"restricted_variables": [
					{"name": "autocommit", "hidden": false, "readonly": true, "value": "1"}
				],
				"restricted_privileges": ["SUPER"],
				"restricted_sql": {
					"sql": ["DROP DATABASE"],
					"rule": ["no_drop"]
				}
			}`,
			wantErr: false,
		},
		{
			name:    "invalid JSON",
			config:  `{"version": "1.0", "tidb_version": "v6.0.0",`,
			wantErr: true,
		},
		{
			name:    "empty JSON",
			config:  `{}`,
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSEMConfig(t, tc.config)
			if tc.wantErr {
				require.Error(t, err, "expected error but got none")
			} else {
				require.NoError(t, err, "unexpected error: %v", err)
			}
		})
	}
}

func TestValidateConfig(t *testing.T) {
	mysql.TiDBReleaseVersion = "v9.0.0"

	cases := []struct {
		name   string
		config string
		errMsg string
	}{
		{
			name: "valid config",
			config: `{
				"version": "1.0",
				"tidb_version": "v6.0.0",
				"restricted_variables": [
					{"name": "autocommit", "hidden": false, "readonly": true, "value": ""}
				]
			}`,
			errMsg: "",
		},
		{
			name: "invalid TiDB version",
			config: `{
				"version": "1.0",
				"tidb_version": "v99.0.0"
			}`,
			errMsg: "current TiDB version",
		},
		{
			name: "unknown variable",
			config: `{
				"version": "1.0",
				"tidb_version": "v6.0.0",
				"restricted_variables": [
					{"name": "invalid_var", "hidden": false, "readonly": true, "value": "1"}
				]
			}`,
			errMsg: "restricted variable invalid_var is not a valid system variable",
		},
		{
			name: "invalid value for variable",
			config: `{
				"version": "1.0",
				"tidb_version": "v6.0.0",
				"restricted_variables": [
					{"name": "autocommit", "hidden": false, "readonly": true, "value": "1"}
				]
			}`,
			errMsg: "restricted variable autocommit has a value set, but it is not a readonly variable",
		},
		{
			name: "invalid restricted SQL rule",
			config: `{
				"version": "1.0",
				"tidb_version": "v6.0.0",
				"restricted_sql": {
					"sql": ["DROP DATABASE"],
					"rule": ["unknown_rule"]
				}
			}`,
			errMsg: "unknown SQL rule: unknown_rule",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			semConfig, err := parseSEMConfig(t, tc.config)
			require.NoError(t, err, "failed to parse SEM config: %v", err)

			err = validateSEMConfig(semConfig)
			if err != nil && tc.errMsg == "" {
				t.Fatalf("expected no error, but got: %v", err)
			}
			if err == nil && tc.errMsg != "" {
				t.Fatalf("expected error %q, but got none", tc.errMsg)
			}
			if err != nil && !strings.Contains(err.Error(), tc.errMsg) {
				t.Fatalf("expected error to contain %q, but got: %v", tc.errMsg, err)
			}
		})
	}
}
