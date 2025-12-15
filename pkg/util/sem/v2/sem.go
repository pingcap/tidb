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
	"strings"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	// in normal code path, sem is not changed after initialization, but during
	// UT, we have to change it multiple times, so we use atomic.Pointer here.
	globalSem atomic.Pointer[semImpl]
)

// IsInvisibleSchema checks if a database is hidden under SEM rules.
func IsInvisibleSchema(dbName string) bool {
	sem := globalSem.Load()
	if sem == nil {
		return false
	}
	return sem.isInvisibleSchema(dbName)
}

// IsInvisibleTable checks if a table is hidden in a specific database under SEM rules.
func IsInvisibleTable(dbLowerName, tblLowerName string) bool {
	sem := globalSem.Load()
	if sem == nil {
		return false
	}
	return sem.isInvisibleTable(dbLowerName, tblLowerName)
}

// IsRestrictedPrivilege checks if a privilege is restricted under SEM rules.
func IsRestrictedPrivilege(privilege string) bool {
	sem := globalSem.Load()
	if sem == nil {
		return false
	}
	return sem.isRestrictedPrivilege(privilege)
}

// IsInvisibleSysVar checks if a system variable is hidden under SEM rules.
func IsInvisibleSysVar(varName string) bool {
	sem := globalSem.Load()
	if sem == nil {
		return false
	}
	return sem.isInvisibleSysVar(varName)
}

// IsReadOnlyVariable checks if a system variable is read-only under SEM rules.
func IsReadOnlyVariable(varName string) bool {
	sem := globalSem.Load()
	if sem == nil {
		return false
	}
	return sem.isReadOnlyVariable(varName)
}

// IsInvisibleStatusVar checks if a status variable is restricted under SEM rules.
func IsInvisibleStatusVar(varName string) bool {
	sem := globalSem.Load()
	if sem == nil {
		return false
	}
	return sem.isInvisibleStatusVar(varName)
}

// IsRestrictedSQL checks if a SQL statement is restricted under SEM rules.
func IsRestrictedSQL(stmt ast.StmtNode) bool {
	sem := globalSem.Load()
	if sem == nil {
		return false
	}
	return sem.isRestrictedSQL(stmt)
}

// Enable enables SEM.
func Enable(configPath string) error {
	sem := globalSem.Load()
	intest.Assert(sem == nil, "SEM is already enabled")

	semConfig, err := parseSEMConfigFromFile(configPath)
	if err != nil {
		return err
	}
	return EnableBy(semConfig)
}

// EnableBy enables SEM by the given configuration.
// we add this to simplify testing.
func EnableBy(semConfig *Config) error {
	sem := globalSem.Load()
	intest.Assert(sem == nil, "SEM is already enabled")
	err := validateSEMConfig(semConfig)
	if err != nil {
		return err
	}

	sem = buildSEMFromConfig(semConfig)
	sem.overrideRestrictedVariable()
	globalSem.Store(sem)

	// set the system variable to indicate SEM is configured by the config file.
	variable.SetSysVar(vardef.TiDBEnableEnhancedSecurity, "CONFIG")

	// write to log so users understand why some operations are weird.
	logutil.BgLogger().Info("tidb-server is operating with security enhanced mode (SEM) v2 enabled")

	return nil
}

// IsEnabled checks if Security Enhanced Mode (SEM) is enabled
func IsEnabled() bool {
	return globalSem.Load() != nil
}

// Disable disables SEM.
func Disable() {
	globalSem.Store(nil)
	variable.SetSysVar(vardef.TiDBEnableEnhancedSecurity, vardef.Off)
}

type semImpl struct {
	restrictedDatabases       map[string]struct{}
	restrictedTables          map[string]map[string]restrictedTableAttr
	restrictedVariables       map[string]restrictedVariableAttr
	restrictedPrivileges      map[string]struct{}
	restrictedStatusVariables map[string]struct{}
	restrictedSQL             func(ast.StmtNode) bool
}

type restrictedVariableAttr struct {
	hidden   bool
	readonly bool
	value    string
}

type restrictedTableAttr struct {
	hidden bool
}

func (s *semImpl) isInvisibleSchema(dbName string) bool {
	_, ok := s.restrictedDatabases[strings.ToLower(dbName)]
	return ok
}

func (s *semImpl) isInvisibleTable(dbLowerName, tblLowerName string) bool {
	// to be compatible with SEM v1, we need to check the invisible schema.
	if s.isInvisibleSchema(dbLowerName) {
		return true
	}

	if tbls, ok := s.restrictedTables[dbLowerName]; ok {
		tbl, ok := tbls[tblLowerName]
		if !ok {
			return false
		}
		return tbl.hidden
	}
	return false
}

func (s *semImpl) isRestrictedPrivilege(privilege string) bool {
	// All privileges starting with "RESTRICTED_" are considered restricted.
	if strings.HasPrefix(privilege, "RESTRICTED_") {
		return true
	}
	_, ok := s.restrictedPrivileges[privilege]
	return ok
}

func (s *semImpl) isInvisibleSysVar(varName string) bool {
	attr, ok := s.restrictedVariables[varName]
	if !ok {
		return false
	}

	return attr.hidden
}

func (s *semImpl) isInvisibleStatusVar(varName string) bool {
	// SEM v2 does not support restricted status variables.
	// This function is kept for compatibility with SEM v1.
	_, ok := s.restrictedStatusVariables[varName]
	return ok
}

func (s *semImpl) isReadOnlyVariable(varName string) bool {
	attr, ok := s.restrictedVariables[varName]
	if !ok {
		return false
	}

	return attr.readonly
}

func (s *semImpl) isRestrictedSQL(stmt ast.StmtNode) bool {
	if s.restrictedSQL == nil {
		return false
	}

	return s.restrictedSQL(stmt)
}

func (s *semImpl) overrideRestrictedVariable() {
	for restrictedVar, attr := range s.restrictedVariables {
		if attr.value != "" {
			variable.SetSysVar(restrictedVar, attr.value)
		}
	}
}

func buildSEMSqlValidateFunction(sqlRestriction *SQLRestriction) func(ast.StmtNode) bool {
	if sqlRestriction == nil {
		return nil
	}

	sqlRules := make(map[string]SQLRule, len(sqlRestriction.Rule))
	for _, ruleName := range sqlRestriction.Rule {
		if rule, ok := sqlRuleNameMap[ruleName]; ok {
			sqlRules[ruleName] = rule
		} else {
			// should never happen
			logutil.BgLogger().Warn("unknown SQL rule", zap.String("rule", ruleName))
			intest.Assert(false, "unknown SQL rule: %s", ruleName)
		}
	}

	sqlCommands := make(map[string]struct{}, len(sqlRestriction.SQL))
	for _, sql := range sqlRestriction.SQL {
		sql = strings.TrimSpace(strings.ToUpper(sql))
		if sql == "" {
			continue
		}
		sqlCommands[sql] = struct{}{}
	}

	return func(stmt ast.StmtNode) bool {
		// check SQL commands
		if _, ok := sqlCommands[stmt.SEMCommand()]; ok {
			return true
		}

		// check SQL rules
		for _, rule := range sqlRules {
			if rule(stmt) {
				return true
			}
		}

		return false
	}
}

func buildSEMFromConfig(cfg *Config) *semImpl {
	sem := &semImpl{
		restrictedDatabases:       make(map[string]struct{}, len(cfg.RestrictedDatabases)),
		restrictedTables:          make(map[string]map[string]restrictedTableAttr),
		restrictedVariables:       make(map[string]restrictedVariableAttr, len(cfg.RestrictedVariables)),
		restrictedStatusVariables: make(map[string]struct{}, len(cfg.RestrictedStatusVar)),
		restrictedPrivileges:      make(map[string]struct{}, len(cfg.RestrictedPrivileges)),
	}

	for _, db := range cfg.RestrictedDatabases {
		sem.restrictedDatabases[db] = struct{}{}
	}

	for _, tbl := range cfg.RestrictedTables {
		if sem.restrictedTables[tbl.Schema] == nil {
			sem.restrictedTables[tbl.Schema] = make(map[string]restrictedTableAttr)
		}
		sem.restrictedTables[tbl.Schema][tbl.Name] = restrictedTableAttr{hidden: tbl.Hidden}
	}

	for _, varDef := range cfg.RestrictedVariables {
		sem.restrictedVariables[varDef.Name] = restrictedVariableAttr{
			hidden:   varDef.Hidden,
			readonly: varDef.Readonly,
			value:    varDef.Value,
		}
	}

	for _, statusVar := range cfg.RestrictedStatusVar {
		sem.restrictedStatusVariables[statusVar] = struct{}{}
	}

	for _, priv := range cfg.RestrictedPrivileges {
		priv = strings.ToUpper(priv)
		sem.restrictedPrivileges[priv] = struct{}{}
	}

	sem.restrictedSQL = buildSEMSqlValidateFunction(&cfg.RestrictedSQL)

	return sem
}
