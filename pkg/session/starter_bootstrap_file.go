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

package session

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

const (
	starterBootstrapVersionVar          = "starter_bootstrap_version"
	starterBootstrapKeyspacePlaceholder = "<keyspace>"
	starterBootstrapVersionComment      = "Starter bootstrap file version. Do not delete."
)

// These values are part of the existing PD keyspace metadata contract.
const (
	starterBranchResetCompleteKey  = "serverless_is_branch_bootstrapped"
	starterRestoreResetCompleteKey = "serverless_is_bootstrapped_for_restore"
)

var (
	starterBootstrapPlaceholderRe = regexp.MustCompile(`<[A-Za-z0-9_-]+>`)
	starterPrivilegeResetSQL      = []string{
		"DELETE FROM mysql.db",
		"DELETE FROM mysql.default_roles",
		"DELETE FROM mysql.global_grants",
		"DELETE FROM mysql.global_priv",
		"DELETE FROM mysql.role_edges",
		"DELETE FROM mysql.user",
	}
)

type starterBootstrapFileSpec struct {
	Version            int64                         `json:"version"`
	BootstrapSQLBlocks []string                      `json:"bootstrap,omitempty"`
	Upgrades           []starterBootstrapUpgradeSpec `json:"upgrades,omitempty"`
}

type starterBootstrapUpgradeSpec struct {
	Version   int64    `json:"version"`
	SQLBlocks []string `json:"sql,omitempty"`
}

type starterPrivilegeResetState struct {
	keyspaceName   string
	pendingMarkers map[string]string
}

func runStarterBootstrapLocked(s sessionapi.Session, bootstrapFile *starterBootstrapFileSpec) error {
	stmts, err := prepareStarterBootstrapStatements(s, bootstrapFile.BootstrapSQLBlocks)
	if err != nil {
		return err
	}
	return runStarterBootstrapTransaction(s, bootstrapFile, stmts, false)
}

func runStarterPrivilegeResetLocked(s sessionapi.Session, bootstrapFile *starterBootstrapFileSpec) error {
	stmts, err := prepareStarterBootstrapStatements(s, bootstrapFile.BootstrapSQLBlocks)
	if err != nil {
		return err
	}
	return runStarterBootstrapTransaction(s, bootstrapFile, stmts, true)
}

func runStarterBootstrapTransaction(
	s sessionapi.Session,
	bootstrapFile *starterBootstrapFileSpec,
	bootstrapStmts []ast.StmtNode,
	resetPrivileges bool,
) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	if _, err := s.ExecuteInternal(ctx, "BEGIN"); err != nil {
		return errors.Annotate(err, "begin starter bootstrap file")
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		if _, err := s.ExecuteInternal(ctx, "ROLLBACK"); err != nil {
			logutil.BgLogger().Warn("rollback starter bootstrap file failed", zap.Error(err))
		}
	}()
	if resetPrivileges {
		if err := executeStarterBootstrapSQLBlocks(s, starterPrivilegeResetSQL); err != nil {
			return errors.Annotate(err, "reset starter privilege tables")
		}
	}
	if err := executeStarterBootstrapStatements(s, bootstrapStmts); err != nil {
		return err
	}
	if resetPrivileges {
		if err := verifyStarterRootUser(s); err != nil {
			return err
		}
	}
	if err := updateStarterBootstrapVersion(s, bootstrapFile.Version); err != nil {
		return err
	}
	if _, err := s.ExecuteInternal(ctx, "COMMIT"); err != nil {
		return errors.Annotate(err, "commit starter bootstrap file")
	}
	committed = true
	return nil
}

// upgradeStarterBootstrap reconciles starter SQL independently of TiDB's core bootstrap lifecycle.
func upgradeStarterBootstrap(store kv.Storage) error {
	bootstrapFile, err := loadStarterBootstrapFile()
	if err != nil {
		return err
	}
	if bootstrapFile == nil {
		_, pending, err := readStarterPrivilegeResetState(store, false)
		if err != nil {
			return err
		}
		if pending {
			return errors.New("starter bootstrap file is required for pending privilege reset")
		}
		return nil
	}
	return upgradeStarterBootstrapWithFile(store, bootstrapFile)
}

func upgradeStarterBootstrapWithFile(store kv.Storage, bootstrapFile *starterBootstrapFileSpec) error {
	resetState, privilegeResetPending, err := readStarterPrivilegeResetState(store, false)
	if err != nil {
		return err
	}
	completedVersion, err := getStoreStarterBootstrapVersion(store)
	if err != nil {
		return err
	}
	if !privilegeResetPending && !needStarterBootstrapUpgrade(completedVersion, bootstrapFile) {
		return nil
	}

	startTime := time.Now()
	releaseFn, err := acquireLock(store)
	if err != nil {
		return errors.Annotate(err, "acquire starter bootstrap file upgrade lock")
	}
	defer releaseFn()

	if privilegeResetPending {
		resetState, privilegeResetPending, err = readStarterPrivilegeResetState(store, true)
		if err != nil {
			return err
		}
	}
	completedVersion, err = getStoreStarterBootstrapVersion(store)
	if err != nil {
		return err
	}
	if !privilegeResetPending && !needStarterBootstrapUpgrade(completedVersion, bootstrapFile) {
		return nil
	}

	s, err := createSession(store)
	if err != nil {
		return errors.Trace(err)
	}
	dom := domain.GetDomain(s)
	defer func() {
		dom.Close()
		if intest.InTest {
			infosync.MockGlobalServerInfoManagerEntry.Close()
		}
		domap.Delete(store)
	}()

	// Starter bootstrap SQL may access regular schemas and needs a fully initialized domain.
	if err = dom.Start(ddl.Normal); err != nil {
		return errors.Trace(err)
	}
	s.sessionVars.EnableClusteredIndex = vardef.ClusteredIndexDefModeIntOnly
	s.SetValue(sessionctx.Initing, true)
	defer s.ClearValue(sessionctx.Initing)

	storedVersion, err := getStarterBootstrapVersion(s)
	if err != nil {
		return err
	}
	if privilegeResetPending {
		copiedVersion := max(completedVersion, storedVersion)
		if copiedVersion > bootstrapFile.Version {
			return errors.Errorf("starter bootstrap file version %d is older than copied version %d", bootstrapFile.Version, copiedVersion)
		}
		if err = runStarterPrivilegeResetLocked(s, bootstrapFile); err != nil {
			return err
		}
		if err = finishStarterBootstrap(store, bootstrapFile.Version); err != nil {
			return err
		}
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
		if err = markStarterPrivilegeResetComplete(ctx, resetState); err != nil {
			return errors.Annotate(err, "complete starter privilege reset")
		}
		logutil.BgLogger().Info("starter privilege reset finished",
			zap.String("keyspace", resetState.keyspaceName),
			zap.Int64("version", bootstrapFile.Version),
			zap.Duration("cost", time.Since(startTime)))
		return nil
	}
	if !needStarterBootstrapUpgrade(storedVersion, bootstrapFile) {
		// The SQL version can be ahead after a crash before the completion key is written.
		return finishStarterBootstrap(store, storedVersion)
	}
	if storedVersion == 0 {
		if err = runStarterBootstrapLocked(s, bootstrapFile); err != nil {
			return err
		}
		if err = finishStarterBootstrap(store, bootstrapFile.Version); err != nil {
			return err
		}
		logutil.BgLogger().Info("starter bootstrap file initialization finished",
			zap.Int64("version", bootstrapFile.Version),
			zap.Duration("cost", time.Since(startTime)))
		return nil
	}

	if err = upgradeStarterBootstrapFromVersion(s, bootstrapFile, storedVersion); err != nil {
		return err
	}
	if err = finishStarterBootstrap(store, bootstrapFile.Version); err != nil {
		return err
	}
	logutil.BgLogger().Info("starter bootstrap file upgrade finished",
		zap.Int64("version", bootstrapFile.Version),
		zap.Duration("cost", time.Since(startTime)))
	return nil
}

func pendingStarterPrivilegeReset(keyspaceConfig map[string]string) (starterPrivilegeResetState, bool) {
	state := starterPrivilegeResetState{}
	for _, key := range []string{starterBranchResetCompleteKey, starterRestoreResetCompleteKey} {
		value, ok := keyspaceConfig[key]
		if !ok || value == "" {
			continue
		}
		complete, _ := strconv.ParseBool(value)
		if complete {
			continue
		}
		if state.pendingMarkers == nil {
			state.pendingMarkers = make(map[string]string)
		}
		state.pendingMarkers[key] = value
	}
	return state, len(state.pendingMarkers) > 0
}

func readStarterPrivilegeResetState(store kv.Storage, refreshFromPD bool) (starterPrivilegeResetState, bool, error) {
	keyspaceMeta := store.GetCodec().GetKeyspaceMeta()
	if keyspaceMeta == nil {
		return starterPrivilegeResetState{}, false, nil
	}
	if refreshFromPD {
		if storeWithPD, ok := store.(kv.StorageWithPD); ok && storeWithPD.GetPDClient() != nil {
			latestMeta, err := storeWithPD.GetPDClient().LoadKeyspace(context.Background(), keyspaceMeta.GetName())
			if err != nil {
				return starterPrivilegeResetState{}, false, errors.Annotate(err, "refresh starter privilege reset metadata")
			}
			if latestMeta != nil {
				keyspaceMeta = latestMeta
			}
		}
	}
	state, pending := pendingStarterPrivilegeReset(keyspaceMeta.GetConfig())
	state.keyspaceName = keyspaceMeta.GetName()
	return state, pending, nil
}

func markStarterPrivilegeResetComplete(ctx context.Context, state starterPrivilegeResetState) error {
	completeValue := "True"
	config := make(map[string]*string, len(state.pendingMarkers))
	preconditions := make(map[string]*string, len(state.pendingMarkers))
	for key, value := range state.pendingMarkers {
		observedValue := value
		config[key] = &completeValue
		preconditions[key] = &observedValue
	}
	return infosync.SetKeyspaceConfig(ctx, state.keyspaceName, pdhttp.UpdateKeyspaceConfigParams{
		Config:        config,
		Preconditions: preconditions,
	})
}

func getStoreStarterBootstrapVersion(store kv.Storage) (int64, error) {
	var version int64
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	err := kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
		var err error
		version, err = meta.NewReader(txn).GetStarterBootstrapVersion()
		return err
	})
	return version, errors.Annotate(err, "get starter bootstrap version from store")
}

func finishStarterBootstrap(store kv.Storage, version int64) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	err := kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		return meta.NewMutator(txn).FinishStarterBootstrap(version)
	})
	return errors.Annotate(err, "finish starter bootstrap in store")
}

func loadStarterBootstrapFile() (*starterBootstrapFileSpec, error) {
	if !deploymode.IsStarter() {
		return nil, nil
	}
	bootstrapFilePath := config.GetGlobalConfig().StarterParams.BootstrapFile
	if bootstrapFilePath == "" {
		return nil, nil
	}
	data, err := os.ReadFile(bootstrapFilePath) //nolint:gosec
	if err != nil {
		return nil, errors.Annotatef(err, "read starter bootstrap file %s", bootstrapFilePath)
	}
	bootstrapFile, err := parseStarterBootstrapFile(data)
	if err != nil {
		return nil, errors.Annotatef(err, "parse starter bootstrap file %s", bootstrapFilePath)
	}
	logutil.BgLogger().Info("loaded starter bootstrap file",
		zap.String("file", bootstrapFilePath),
		zap.Int64("version", bootstrapFile.Version),
		zap.Int("bootstrapBlocks", len(bootstrapFile.BootstrapSQLBlocks)),
		zap.Int("upgradeEntries", len(bootstrapFile.Upgrades)))
	return bootstrapFile, nil
}

func parseStarterBootstrapFile(data []byte) (*starterBootstrapFileSpec, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var bootstrapFile starterBootstrapFileSpec
	if err := decoder.Decode(&bootstrapFile); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("bootstrap file must contain a single JSON object")
		}
		return nil, err
	}
	if err := bootstrapFile.validate(); err != nil {
		return nil, err
	}
	return &bootstrapFile, nil
}

func (m *starterBootstrapFileSpec) validate() error {
	if m.Version <= 0 {
		return errors.New("bootstrap file version must be greater than 0")
	}
	if err := validateStarterBootstrapSQLBlocks("bootstrap", m.BootstrapSQLBlocks); err != nil {
		return err
	}
	seenUpgradeVersions := make(map[int64]struct{}, len(m.Upgrades))
	for i := range m.Upgrades {
		upgrade := &m.Upgrades[i]
		if upgrade.Version <= 0 {
			return errors.Errorf("upgrades[%d].version must be greater than 0", i)
		}
		if upgrade.Version > m.Version {
			return errors.Errorf("upgrades[%d].version %d is greater than bootstrap file version %d", i, upgrade.Version, m.Version)
		}
		if _, ok := seenUpgradeVersions[upgrade.Version]; ok {
			return errors.Errorf("duplicated upgrade version %d", upgrade.Version)
		}
		seenUpgradeVersions[upgrade.Version] = struct{}{}
		if err := validateStarterBootstrapSQLBlocks(fmt.Sprintf("upgrades[%d].sql", i), upgrade.SQLBlocks); err != nil {
			return err
		}
	}
	sort.Slice(m.Upgrades, func(i, j int) bool {
		return m.Upgrades[i].Version < m.Upgrades[j].Version
	})
	return nil
}

func validateStarterBootstrapSQLBlocks(field string, blocks []string) error {
	for i, block := range blocks {
		if strings.TrimSpace(block) == "" {
			return errors.Errorf("%s[%d] must not be empty", field, i)
		}
		placeholders := starterBootstrapPlaceholderRe.FindAllString(block, -1)
		for _, placeholder := range placeholders {
			if placeholder != starterBootstrapKeyspacePlaceholder {
				return errors.Errorf("%s[%d] uses unsupported placeholder %q", field, i, placeholder)
			}
		}
	}
	return nil
}

func needStarterBootstrapUpgrade(storedVersion int64, bootstrapFile *starterBootstrapFileSpec) bool {
	if storedVersion > bootstrapFile.Version {
		logutil.BgLogger().Warn("starter bootstrap file is older than cluster state",
			zap.Int64("storedVersion", storedVersion),
			zap.Int64("bootstrapFileVersion", bootstrapFile.Version))
		return false
	}
	return storedVersion < bootstrapFile.Version
}

func upgradeStarterBootstrapFromVersion(s sessionapi.Session, bootstrapFile *starterBootstrapFileSpec, storedVersion int64) error {
	if !needStarterBootstrapUpgrade(storedVersion, bootstrapFile) {
		return nil
	}

	// Upgrade SQL is committed statement by statement and must be idempotent for startup retries.
	for _, upgrade := range bootstrapFile.pendingUpgrades(storedVersion) {
		logutil.BgLogger().Info("running starter bootstrap file upgrade",
			zap.Int64("storedVersion", storedVersion),
			zap.Int64("upgradeVersion", upgrade.Version),
			zap.Int64("targetVersion", bootstrapFile.Version))
		if err := executeStarterBootstrapSQLBlocks(s, upgrade.SQLBlocks); err != nil {
			return errors.Annotatef(err, "upgrade starter bootstrap file to version %d", upgrade.Version)
		}
	}
	return updateStarterBootstrapVersion(s, bootstrapFile.Version)
}

func (m *starterBootstrapFileSpec) pendingUpgrades(storedVersion int64) []starterBootstrapUpgradeSpec {
	idx := sort.Search(len(m.Upgrades), func(i int) bool {
		return m.Upgrades[i].Version > storedVersion
	})
	return m.Upgrades[idx:]
}

func getStarterBootstrapVersion(s sessionapi.Session) (int64, error) {
	sVal, isNull, err := getTiDBVar(s, starterBootstrapVersionVar)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if isNull {
		return 0, nil
	}
	version, err := strconv.ParseInt(sVal, 10, 64)
	if err != nil {
		return 0, errors.Annotatef(err, "invalid starter bootstrap version %q", sVal)
	}
	return version, nil
}

func updateStarterBootstrapVersion(s sessionapi.Session, version int64) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	rs, err := s.ExecuteInternal(ctx,
		`INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?`,
		mysql.SystemDB, mysql.TiDBTable, starterBootstrapVersionVar, version, starterBootstrapVersionComment, version)
	if err != nil {
		return errors.Trace(err)
	}
	if rs != nil {
		return errors.Trace(rs.Close())
	}
	return nil
}

func executeStarterBootstrapSQLBlocks(s sessionapi.Session, blocks []string) error {
	stmts, err := parseStarterBootstrapSQLBlocks(s, blocks)
	if err != nil {
		return err
	}
	return executeStarterBootstrapStatements(s, stmts)
}

func prepareStarterBootstrapStatements(s sessionapi.Session, blocks []string) ([]ast.StmtNode, error) {
	if len(blocks) == 0 {
		return nil, errors.New("starter bootstrap file must contain bootstrap SQL")
	}
	stmts, err := parseStarterBootstrapSQLBlocks(s, blocks)
	if err != nil {
		return nil, err
	}
	if err := validateStarterBootstrapStatements(stmts); err != nil {
		return nil, err
	}
	return stmts, nil
}

func parseStarterBootstrapSQLBlocks(s sessionapi.Session, blocks []string) ([]ast.StmtNode, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	sessionVars := s.GetSessionVars()
	originalInRestrictedSQL := sessionVars.InRestrictedSQL
	sessionVars.InRestrictedSQL = true
	defer func() {
		sessionVars.InRestrictedSQL = originalInRestrictedSQL
	}()

	stmts := make([]ast.StmtNode, 0, len(blocks))
	for blockIdx, block := range blocks {
		rendered := renderStarterBootstrapSQL(block)
		parsed, err := s.Parse(ctx, rendered)
		if err != nil {
			return nil, errors.Annotatef(err, "parse SQL block %d", blockIdx)
		}
		if len(parsed) != 1 {
			return nil, errors.Errorf("SQL block %d must contain exactly one statement", blockIdx)
		}
		stmts = append(stmts, parsed[0])
	}
	return stmts, nil
}

func validateStarterBootstrapStatements(stmts []ast.StmtNode) error {
	for i, stmt := range stmts {
		switch stmt.(type) {
		case *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt:
		default:
			return errors.Errorf("bootstrap SQL block %d must be INSERT, REPLACE, UPDATE, or DELETE", i)
		}
	}
	return nil
}

func executeStarterBootstrapStatements(s sessionapi.Session, stmts []ast.StmtNode) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	sessionVars := s.GetSessionVars()
	originalInRestrictedSQL := sessionVars.InRestrictedSQL
	sessionVars.InRestrictedSQL = true
	defer func() {
		sessionVars.InRestrictedSQL = originalInRestrictedSQL
	}()

	for i, stmt := range stmts {
		rs, err := s.ExecuteStmt(ctx, stmt)
		if err != nil {
			return errors.Annotatef(err, "execute SQL block %d", i)
		}
		if rs != nil {
			if err := rs.Close(); err != nil {
				return errors.Annotate(err, "close SQL result")
			}
		}
	}
	return nil
}

func verifyStarterRootUser(s sessionapi.Session) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	rootUser := config.GetGlobalKeyspaceName() + ".root"
	rs, err := s.ExecuteInternal(ctx,
		"SELECT 1 FROM mysql.user WHERE Host = '%' AND User = %? LIMIT 1", rootUser)
	if err != nil {
		return errors.Annotate(err, "verify starter root user")
	}
	if rs == nil {
		return errors.New("verify starter root user returned no result")
	}
	req := rs.NewChunk(nil)
	nextErr := rs.Next(ctx, req)
	closeErr := rs.Close()
	if nextErr != nil {
		return errors.Annotate(nextErr, "verify starter root user")
	}
	if closeErr != nil {
		return errors.Annotate(closeErr, "close starter root verification result")
	}
	if req.NumRows() == 0 {
		return errors.Errorf("starter bootstrap file must create '%s'@'%%' for privilege reset", rootUser)
	}
	return nil
}

func renderStarterBootstrapSQL(sql string) string {
	keyspaceName := sqlescape.EscapeString(config.GetGlobalKeyspaceName())
	return strings.ReplaceAll(sql, starterBootstrapKeyspacePlaceholder, keyspaceName)
}
