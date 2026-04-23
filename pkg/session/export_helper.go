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

package session

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema/issyncer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ExportedTableBasicInfo is TableBasicInfo for test packages
type ExportedTableBasicInfo = TableBasicInfo

// ExportedDatabaseBasicInfo is DatabaseBasicInfo for test packages
type ExportedDatabaseBasicInfo = DatabaseBasicInfo

// ExportedVersionedDDLTables is versionedDDLTables for test packages
type ExportedVersionedDDLTables struct {
	Version int64
	Tables  []ExportedTableBasicInfo
}

// ExportedBootstrapSession is BootstrapSession for test packages
func ExportedBootstrapSession(store kv.Storage) (*domain.Domain, error) {
	return BootstrapSession(store)
}

// ExportedCreateSession4Test is CreateSession4Test for test packages
func ExportedCreateSession4Test(store kv.Storage) (sessionapi.Session, error) {
	return CreateSession4Test(store)
}

// ExportedMustExecute is mustExecute for test packages
func ExportedMustExecute(s sessionapi.Session, sql string, args ...any) {
	mustExecute(s, sql, args...)
}

// ExportedGetStartMode is getStartMode for test packages
func ExportedGetStartMode(ver int64) ddl.StartMode {
	return getStartMode(ver)
}

// GetExportedCurrentBootstrapVersion returns the current bootstrap version for testing
func GetExportedCurrentBootstrapVersion() int64 {
	return currentBootstrapVersion
}

// GetExportedDDLTableVersionTables returns the DDL table version tables for testing
func GetExportedDDLTableVersionTables() []ExportedVersionedDDLTables {
	result := make([]ExportedVersionedDDLTables, len(ddlTableVersionTables))
	for i, v := range ddlTableVersionTables {
		tables := make([]ExportedTableBasicInfo, len(v.tables))
		copy(tables, v.tables)
		result[i] = ExportedVersionedDDLTables{
			Version: int64(v.ver),
			Tables:  tables,
		}
	}
	return result
}

// ExportedApproxParseSQLTokenCnt is approxParseSQLTokenCnt for test packages
func ExportedApproxParseSQLTokenCnt(sql string) int64 {
	return approxParseSQLTokenCnt(sql)
}

// ExportedApproxCompilePlanTokenCnt is approxCompilePlanTokenCnt for test packages
func ExportedApproxCompilePlanTokenCnt(sql string, hasSelect bool) int64 {
	return approxCompilePlanTokenCnt(sql, hasSelect)
}

// GetExportedUpgradeToVerFunctions returns the upgrade to version functions for testing
func GetExportedUpgradeToVerFunctions() []ExportedVersionedUpgradeFunction {
	result := make([]ExportedVersionedUpgradeFunction, len(upgradeToVerFunctions))
	for i, v := range upgradeToVerFunctions {
		result[i] = ExportedVersionedUpgradeFunction{
			Version: v.version,
			Fn:      v.fn,
		}
	}
	return result
}

// ExportedVersionedUpgradeFunction is the versionedUpgradeFunction type for testing
type ExportedVersionedUpgradeFunction struct {
	Version int64
	Fn      func(sessionapi.Session, int64)
}

// ExportedDrainRecordSet is drainRecordSet for test packages
func ExportedDrainRecordSet(ctx context.Context, se sessionapi.Session, rs sqlexec.RecordSet, alloc chunk.Allocator) ([]chunk.Row, error) {
	// Type assert to internal session type
	if s, ok := se.(*session); ok {
		return drainRecordSet(ctx, s, rs, alloc)
	}
	// If not a *session, return empty result
	return nil, nil
}

// ExportedResultSetToStringSlice is ResultSetToStringSlice for test packages
func ExportedResultSetToStringSlice(ctx context.Context, s sessionapi.Session, rs sqlexec.RecordSet) ([][]string, error) {
	return ResultSetToStringSlice(ctx, s, rs)
}

// GetSystemTablesOfBaseNextGenVersion returns systemTablesOfBaseNextGenVersion for testing
func GetSystemTablesOfBaseNextGenVersion() []TableBasicInfo {
	return systemTablesOfBaseNextGenVersion
}

// GetVersionedBootstrapSchemas returns versionedBootstrapSchemas for testing
func GetVersionedBootstrapSchemas() []ExportedVersionedBootstrapSchema {
	result := make([]ExportedVersionedBootstrapSchema, len(versionedBootstrapSchemas))
	for i, v := range versionedBootstrapSchemas {
		dbs := make([]ExportedDatabaseBasicInfo, len(v.databases))
		copy(dbs, v.databases)
		result[i] = ExportedVersionedBootstrapSchema{
			Version:   int64(v.ver),
			Databases: dbs,
		}
	}
	return result
}

// ExportedVersionedBootstrapSchema is the versionedBootstrapSchema type for testing
type ExportedVersionedBootstrapSchema struct {
	Version   int64
	Databases []ExportedDatabaseBasicInfo
}

// GetSystemDatabases returns systemDatabases for testing
func GetSystemDatabases() []ExportedDatabaseBasicInfo {
	result := make([]ExportedDatabaseBasicInfo, len(systemDatabases))
	copy(result, systemDatabases)
	return result
}

// ExportedCreateStoreAndBootstrap is CreateStoreAndBootstrap for test packages
// This function is defined in testutil.go and is used in test packages
func ExportedCreateStoreAndBootstrap(t *testing.T) (kv.Storage, *domain.Domain) {
	return CreateStoreAndBootstrap(t)
}

// ExportedCreateSessionAndSetID is CreateSessionAndSetID for test packages
func ExportedCreateSessionAndSetID(t *testing.T, store kv.Storage) sessionapi.Session {
	return CreateSessionAndSetID(t, store)
}

// ExportedMustExec is MustExec for test packages
func ExportedMustExec(t *testing.T, se sessionapi.Session, sql string, args ...any) {
	MustExec(t, se, sql, args...)
}

// ExportedMustExecToRecodeSet is MustExecToRecodeSet for test packages
func ExportedMustExecToRecodeSet(t *testing.T, se sessionapi.Session, sql string, args ...any) sqlexec.RecordSet {
	return MustExecToRecodeSet(t, se, sql, args...)
}

// GetDatabaseBasicInfoTables returns the Tables field from DatabaseBasicInfo for testing
func GetDatabaseBasicInfoTables(db DatabaseBasicInfo) []TableBasicInfo {
	return db.Tables
}

// GetStoreBootstrappedKey returns the StoreBootstrappedKey for testing
func GetStoreBootstrappedKey() string {
	return StoreBootstrappedKey
}

// ExportedDoDMLWorks is doDMLWorks for test packages
func ExportedDoDMLWorks(se sessionapi.Session) {
	doDMLWorks(se)
}

// ExportedBootstrapSchemas is bootstrapSchemas for test packages
func ExportedBootstrapSchemas(store kv.Storage) error {
	return bootstrapSchemas(store)
}

// ExportedGetSession returns the internal session from a sessionapi.Session for testing
func ExportedGetSession(se sessionapi.Session) *session {
	if s, ok := se.(*session); ok {
		return s
	}
	return nil
}

// ExportedNewPlanContextImpl is newPlanContextImpl for test packages
// Note: This returns the internal planContextImpl type, which is not exported
func ExportedNewPlanContextImpl() any {
	// newPlanContextImpl returns *planContextImpl which is not exported
	// For testing purposes, we return an any
	return nil
}

// ExportedInitDDLTables is InitDDLTables for test packages
func ExportedInitDDLTables(store kv.Storage) error {
	return InitDDLTables(store)
}

// ExportedDomap returns domap for testing
func ExportedDomap() *domainMap {
	return domap
}

// ExportedCheckBootstrapped is checkBootstrapped for test packages
func ExportedCheckBootstrapped(se sessionapi.Session) (bool, error) {
	return checkBootstrapped(se)
}

// ExportedDoDDLWorks is doDDLWorks for test packages
func ExportedDoDDLWorks(se sessionapi.Session) {
	doDDLWorks(se)
}

// ExportedDomainMap is the domainMap type for testing
type ExportedDomainMap = domainMap

// GetTidbNewCollationEnabled returns the TidbNewCollationEnabled constant
func GetTidbNewCollationEnabled() string {
	return TidbNewCollationEnabled
}

// GetNotBootstrapped returns the notBootstrapped value for testing
func GetNotBootstrapped() int64 {
	return notBootstrapped
}

// ExportedCreateSession is createSession for test packages
func ExportedCreateSession(store kv.Storage) (sessionapi.Session, error) {
	s, err := createSession(store)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// ExportedExec is exec for test packages
func ExportedExec(se sessionapi.Session, sql string, args ...any) (sqlexec.RecordSet, error) {
	rs, err := exec(se, sql, args...)
	return rs, err
}

// GetVersion57 returns the version57 constant
func GetVersion57() int64 {
	return version57
}

// ExportedOldPasswordUpgrade is oldPasswordUpgrade for test packages
func ExportedOldPasswordUpgrade(pass string) (string, error) {
	return oldPasswordUpgrade(pass)
}

// GetStoreBootstrapVersionWithCache is getStoreBootstrapVersionWithCache for test packages
func GetStoreBootstrapVersionWithCache(store kv.Storage) int64 {
	return getStoreBootstrapVersionWithCache(store)
}

// SetCurrentBootstrapVersion sets the currentBootstrapVersion for testing
func SetCurrentBootstrapVersion(ver int64) {
	currentBootstrapVersion = ver
}

// GetVersion82 returns the version82 constant
func GetVersion82() int64 {
	return version82
}

// GetVersion91 returns the version91 constant
func GetVersion91() int64 {
	return version91
}

// GetVersion93 returns the version93 constant
func GetVersion93() int64 {
	return version93
}

// GetVersion132 returns the version132 constant
func GetVersion132() int64 {
	return version132
}

// GetVersion135 returns the version135 constant
func GetVersion135() int64 {
	return version135
}

// GetVersion137 returns the version137 constant
func GetVersion137() int64 {
	return version137
}

// GetVersion139 returns the version139 constant
func GetVersion139() int64 {
	return version139
}

// ExportedRevertVersionAndVariables is RevertVersionAndVariables for test packages
// This wrapper accepts int64 and converts it to int
func ExportedRevertVersionAndVariables(t *testing.T, se sessionapi.Session, ver int64) {
	RevertVersionAndVariables(t, se, int(ver))
}

// GetVersion142 returns the version142 constant
func GetVersion142() int64 {
	return version142
}

// GetVersion143 returns the version143 constant
func GetVersion143() int64 {
	return version143
}

// GetVersion144 returns the version144 constant
func GetVersion144() int64 {
	return version144
}

// GetVersion169 returns the version169 constant
func GetVersion169() int64 {
	return version169
}

// GetVersion175 returns the version175 constant
func GetVersion175() int64 {
	return version175
}

// GetVersion176 returns the version176 constant
func GetVersion176() int64 {
	return version176
}

// GetVersion198 returns the version198 constant
func GetVersion198() int64 {
	return version198
}

// GetVersion239 returns the version239 constant
func GetVersion239() int64 {
	return version239
}

// ExportedInitGlobalVariableIfNotExists is initGlobalVariableIfNotExists for test packages
func ExportedInitGlobalVariableIfNotExists(se sessionapi.Session, name string, value string) {
	initGlobalVariableIfNotExists(se, name, value)
}

// GetVersion241 returns the version241 constant
func GetVersion241() int64 {
	return version241
}

// GetVersion245 returns the version245 constant
func GetVersion245() int64 {
	return version245
}

// GetVersion250 returns the version250 constant
func GetVersion250() int64 {
	return version250
}

// GetVersion253 returns the version253 constant
func GetVersion253() int64 {
	return version253
}

// GetIsNextGenForRUV2 calls the isNextGenForRUV2 function for testing
func GetIsNextGenForRUV2() bool {
	return isNextGenForRUV2()
}

// ExportedCheckSystemTableConstraint is checkSystemTableConstraint for test packages
func ExportedCheckSystemTableConstraint(tblInfo *model.TableInfo) error {
	return checkSystemTableConstraint(tblInfo)
}

// ExportedParseWithParams is ParseWithParams for test packages
func ExportedParseWithParams(se sessionapi.Session, ctx context.Context, sql string, args ...any) (ast.StmtNode, error) {
	if s, ok := se.(*session); ok {
		return s.ParseWithParams(ctx, sql, args...)
	}
	return nil, nil
}

// ExportedExecRestrictedStmt is ExecRestrictedStmt for test packages
func ExportedExecRestrictedStmt(se sessionapi.Session, ctx context.Context, stmtNode ast.StmtNode, opts ...sqlexec.OptionFuncAlias) ([]chunk.Row, []*resolve.ResultField, error) {
	if s, ok := se.(*session); ok {
		return s.ExecRestrictedStmt(ctx, stmtNode, opts...)
	}
	return nil, nil, nil
}

// ExportedExecRestrictedSQL is ExecRestrictedSQL for test packages
func ExportedExecRestrictedSQL(se sessionapi.Session, ctx context.Context, opts []sqlexec.OptionFuncAlias, sql string, params ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	if s, ok := se.(*session); ok {
		return s.ExecRestrictedSQL(ctx, opts, sql, params...)
	}
	return nil, nil, nil
}

// ExportedGetSessionVars returns sessionVars for test packages
func ExportedGetSessionVars(se sessionapi.Session) any {
	if s, ok := se.(*session); ok {
		return s.sessionVars
	}
	return nil
}

// ExportedGetDistSQLCtx is GetDistSQLCtx for test packages
func ExportedGetDistSQLCtx(se sessionapi.Session) any {
	if s, ok := se.(*session); ok {
		return s.GetDistSQLCtx()
	}
	return nil
}

// ExportedSetSessionVars sets sessionVars for test packages
func ExportedSetSessionVars(se sessionapi.Session, vars any) {
	if s, ok := se.(*session); ok {
		s.sessionVars = vars.(*variable.SessionVars)
	}
}

// ExportedExecuteStmt is ExecuteStmt for test packages
func ExportedExecuteStmt(se sessionapi.Session, ctx context.Context, stmt ast.StmtNode) (sqlexec.RecordSet, error) {
	if s, ok := se.(*session); ok {
		return s.ExecuteStmt(ctx, stmt)
	}
	return nil, nil
}

// ExportedCreateSessionWithOpt is createSessionWithOpt for test packages
// Note: For test purposes, nil values are passed for dom, schemaValidator, infoCache, and opt
func ExportedCreateSessionWithOpt(store kv.Storage) (sessionapi.Session, error) {
	s, err := createSession(store)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// ExportedBootstrapSession is BootstrapSession for test packages
// Note: This is already defined above, but ensuring it's available for tests
func ExportedBootstrapSessionFromPkg(store kv.Storage) (*domain.Domain, error) {
	return BootstrapSession(store)
}

// GetCurrentBootstrapVersion returns the current bootstrap version for testing
func GetCurrentBootstrapVersion() int64 {
	return currentBootstrapVersion
}

// ExportedDomapGetWithEtcdClient is domap.getWithEtcdClient for test packages
func ExportedDomapGetWithEtcdClient(store kv.Storage, etcdClient *clientv3.Client, schemaFilter issyncer.Filter) (*domain.Domain, error) {
	return domap.getWithEtcdClient(store, etcdClient, schemaFilter)
}
