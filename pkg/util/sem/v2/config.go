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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

// Config defines the configuration for SEM
type Config struct {
	// Version is the version of this config
	// The version is not used for now because we only have one version of SEM config.
	Version string `json:"version"`

	// TiDBVersion represents the minimum requirement of TiDB versions
	TiDBVersion string `json:"tidb_version"`

	// RestrictedDatabases is the list of restricted databases
	RestrictedDatabases []string `json:"restricted_databases"`

	// RestrictedTables is the list of restricted tables
	RestrictedTables []TableRestriction `json:"restricted_tables"`

	// RestrictedVariables is the list of restricted variables
	RestrictedVariables []VariableRestriction `json:"restricted_variables"`

	// RestrictedStatusVariables is the list of restricted status variables
	RestrictedStatusVar []string `json:"restricted_status_variables"`

	// RestrictedPrivileges is the list of restricted privileges
	RestrictedPrivileges []string `json:"restricted_privileges"`

	// RestrictedSQL contains restricted SQL statements and rules
	RestrictedSQL SQLRestriction `json:"restricted_sql"`
}

// TableRestriction defines the configuration for a restricted table
type TableRestriction struct {
	// Schema is the schema name
	Schema string `json:"schema"`

	// Name is the table name
	Name string `json:"name"`

	// Hidden indicates whether the table is hidden
	Hidden bool `json:"hidden"`

	// Columns represents the special configuration for columns in the table
	Columns []ColumnRestriction `json:"columns"`
}

// ColumnRestriction defines the configuration for a restricted column
type ColumnRestriction struct {
	// Name is the column name
	Name string `json:"name"`
	// Hidden indicates whether the column is hidden
	Hidden bool `json:"hidden"`
	// Value is the value to be set for the column
	Value string `json:"value"`
}

// VariableRestriction defines the configuration for a restricted variable
type VariableRestriction struct {
	// Name is the variable name
	Name string `json:"name"`

	// Hidden indicates whether the variable is hidden
	Hidden bool `json:"hidden"`

	// Readonly indicates whether the variable is read-only
	Readonly bool `json:"readonly"`

	// Value is the value to be set for the variable
	Value string `json:"value"`
}

// SQLRestriction defines the configuration for restricted SQL statements and rules
type SQLRestriction struct {
	// SQL is the list of restricted SQL statements
	SQL []string `json:"sql"`

	// Rule is the list of restricted SQL rules
	Rule []string `json:"rule"`
}

// parseSEMConfigFromFile reads a SEM configuration from a file and returns a SEMConfig instance.
func parseSEMConfigFromFile(filePath string) (*Config, error) {
	// Open the file
	file, err := os.Open(filepath.Clean(filePath))
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// Decode the JSON content
	var config Config
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode JSON from file %s: %w", filePath, err)
	}

	return &config, nil
}

func validateSEMConfig(cfg *Config) error {
	// validate the TiDBVersion
	currentVersion, err := semver.NewVersion(strings.TrimPrefix(mysql.TiDBReleaseVersion, "v"))
	if err != nil {
		return fmt.Errorf("failed to parse current TiDB version: %w", err)
	}
	minRequiredVersion, err := semver.NewVersion(strings.TrimPrefix(cfg.TiDBVersion, "v"))
	if err != nil {
		return fmt.Errorf("failed to parse minimum required TiDB version %s: %w", cfg.TiDBVersion, err)
	}
	if currentVersion.LessThan(*minRequiredVersion) {
		return fmt.Errorf("current TiDB version %s is less than the required version %s", currentVersion, minRequiredVersion)
	}

	// validate the variable configuration
	for _, varDef := range cfg.RestrictedVariables {
		sysVar := variable.GetSysVar(varDef.Name)
		if sysVar == nil {
			return fmt.Errorf("restricted variable %s is not a valid system variable", varDef.Name)
		}
		if varDef.Value != "" && sysVar.Scope != vardef.ScopeNone {
			return fmt.Errorf("restricted variable %s has a value set, but it is not a readonly variable", varDef.Name)
		}
	}

	// validate the SQL rules exist
	for _, ruleName := range cfg.RestrictedSQL.Rule {
		if _, ok := sqlRuleNameMap[ruleName]; !ok {
			return fmt.Errorf("unknown SQL rule: %s", ruleName)
		}
	}

	return nil
}
