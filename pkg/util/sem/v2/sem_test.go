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
	"os"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

var testConfig = &Config{
	Version:              "1.0",
	TiDBVersion:          "v9.0.0",
	RestrictedPrivileges: []string{"SUPER", "process"},
	RestrictedDatabases:  []string{"mysql", "test"},
	RestrictedTables: []TableRestriction{
		{
			Schema: "mysql",
			Name:   "user",
			Hidden: true,
		},
		{
			Schema: "test",
			Name:   "tbl2",
			Hidden: false,
		},
	},
	RestrictedVariables: []VariableRestriction{
		{
			Name:   vardef.SuperReadOnly,
			Hidden: true,
		},
		{
			Name:     vardef.TiDBEnableEnhancedSecurity,
			Hidden:   false,
			Readonly: true,
			Value:    vardef.On,
		},
	},
}

func semTestBackup(sem *Config) func() {
	variableDefValue := make(map[string]string)
	for _, v := range sem.RestrictedVariables {
		if v.Value != "" {
			sysVar := variable.GetSysVar(v.Name)
			if sysVar == nil {
				continue
			}

			variableDefValue[v.Name] = sysVar.Value
		}
	}

	return func() {
		sem = nil

		for name, value := range variableDefValue {
			variable.SetSysVar(name, value)
		}
	}
}

func TestSEMMethods(t *testing.T) {
	defer semTestBackup(testConfig)()
	sem := buildSEMFromConfig(testConfig)

	// Test restricted privileges
	require.True(t, sem.isRestrictedPrivilege("SUPER"))
	require.True(t, sem.isRestrictedPrivilege("PROCESS"))
	require.False(t, sem.isRestrictedPrivilege("RELOAD"))

	// Test restricted databases
	require.True(t, sem.isInvisibleSchema("mysql"))
	require.True(t, sem.isInvisibleSchema("test"))
	require.False(t, sem.isInvisibleSchema("information_schema"))

	// Test restricted tables
	require.True(t, sem.isInvisibleTable("mysql", "user"))
	require.True(t, sem.isInvisibleTable("mysql", "db"))
	require.False(t, sem.isInvisibleTable("test1", "tbl2"))

	// Test restricted variables
	require.True(t, sem.isInvisibleSysVar(vardef.SuperReadOnly))
	require.False(t, sem.isInvisibleSysVar(vardef.TiDBEnableEnhancedSecurity))

	// Test overrideRestrictedVariable
	sysVar := variable.GetSysVar(vardef.TiDBEnableEnhancedSecurity)
	require.Equal(t, sysVar.Value, vardef.Off)
	sem.overrideRestrictedVariable()
	sysVar = variable.GetSysVar(vardef.TiDBEnableEnhancedSecurity)
	require.Equal(t, sysVar.Value, vardef.On)
}

func TestEnableSEM(t *testing.T) {
	defer semTestBackup(testConfig)()
	mysql.TiDBReleaseVersion = "v9.0.0"
	require.False(t, IsEnabled())
	sysVar := variable.GetSysVar(vardef.TiDBEnableEnhancedSecurity)
	require.Equal(t, sysVar.Value, vardef.Off)

	configFile, err := os.CreateTemp(t.TempDir(), "sem_config_*.json")
	require.NoError(t, err)
	defer os.Remove(configFile.Name())
	configStr, err := json.Marshal(testConfig)
	require.NoError(t, err)
	_, err = configFile.Write(configStr)
	require.NoError(t, err)

	err = Enable(configFile.Name())
	require.NoError(t, err)

	// Test restricted privileges
	require.True(t, IsRestrictedPrivilege("SUPER"))
	require.True(t, IsRestrictedPrivilege("PROCESS"))
	require.False(t, IsRestrictedPrivilege("RELOAD"))

	// Test restricted databases
	require.True(t, IsInvisibleSchema("mysql"))
	require.True(t, IsInvisibleSchema("test"))
	require.False(t, IsInvisibleSchema("information_schema"))

	// Test restricted tables
	require.True(t, IsInvisibleTable("mysql", "user"))
	require.True(t, globalSem.Load().isInvisibleTable("mysql", "db"))
	require.False(t, globalSem.Load().isInvisibleTable("test1", "tbl2"))

	// Test restricted variables
	require.True(t, IsInvisibleSysVar(vardef.SuperReadOnly))
	require.False(t, IsInvisibleSysVar(vardef.TiDBEnableEnhancedSecurity))

	// Test overrideRestrictedVariable
	sysVar = variable.GetSysVar(vardef.TiDBEnableEnhancedSecurity)
	require.Equal(t, sysVar.Value, "CONFIG")
}
