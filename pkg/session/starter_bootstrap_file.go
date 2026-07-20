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
	starterBranchConfigKey              = "serverless_is_branch"
	starterBranchResetCompleteKey       = "serverless_is_branch_bootstrapped"
	starterRestoreResetCompleteKey      = "serverless_is_bootstrapped_for_restore"
	starterPrivilegeResetCompleteValue  = "True"
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
	keyspaceName  string
	completionKey string
	observedValue string
}

func runStarterBootstrapLocked(s sessionapi.Session, bootstrapFile *starterBootstrapFileSpec) error {
	return runStarterBootstrapTransaction(s, bootstrapFile, false)
}

func runStarterPrivilegeResetLocked(s sessionapi.Session, bootstrapFile *starterBootstrapFileSpec) error {
	if len(bootstrapFile.BootstrapSQLBlocks) == 0 {
		return errors.New("starter bootstrap file must contain bootstrap SQL for privilege reset")
	}
	return runStarterBootstrapTransaction(s, bootstrapFile, true)
}

func runStarterBootstrapTransaction(s sessionapi.Session, bootstrapFile *starterBootstrapFileSpec, resetPrivileges bool) error {
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
	if err := executeStarterBootstrapSQLBlocks(s, bootstrapFile.BootstrapSQLBlocks); err != nil {
		return err
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
	return upgradeStarterBootstrapWithFileAndReset(
		store,
		bootstrapFile,
		readStarterPrivilegeResetState,
		markStarterPrivilegeResetComplete,
	)
}

func upgradeStarterBootstrapWithFileAndReset(
	store kv.Storage,
	bootstrapFile *starterBootstrapFileSpec,
	readResetState func(kv.Storage, bool) (starterPrivilegeResetState, bool, error),
	markResetComplete func(context.Context, starterPrivilegeResetState) error,
) error {
	resetState, privilegeResetPending, err := readResetState(store, false)
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
		resetState, privilegeResetPending, err = readResetState(store, true)
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
		if err = runStarterPrivilegeResetLocked(s, bootstrapFile); err != nil {
			return err
		}
		if err = finishStarterBootstrap(store, bootstrapFile.Version); err != nil {
			return err
		}
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
		if err = markResetComplete(ctx, resetState); err != nil {
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
	isBranch, _ := strconv.ParseBool(keyspaceConfig[starterBranchConfigKey])
	if isBranch {
		value, ok := keyspaceConfig[starterBranchResetCompleteKey]
		if !ok || value == "" || value == starterPrivilegeResetCompleteValue {
			return starterPrivilegeResetState{}, false
		}
		return starterPrivilegeResetState{
			completionKey: starterBranchResetCompleteKey,
			observedValue: value,
		}, true
	}

	value, ok := keyspaceConfig[starterRestoreResetCompleteKey]
	if !ok || value == "" {
		return starterPrivilegeResetState{}, false
	}
	complete, _ := strconv.ParseBool(value)
	if complete {
		return starterPrivilegeResetState{}, false
	}
	return starterPrivilegeResetState{
		completionKey: starterRestoreResetCompleteKey,
		observedValue: value,
	}, true
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
	completeValue := starterPrivilegeResetCompleteValue
	observedValue := state.observedValue
	return infosync.SetKeyspaceConfig(ctx, state.keyspaceName, pdhttp.UpdateKeyspaceConfigParams{
		Config: map[string]*string{
			state.completionKey: &completeValue,
		},
		Preconditions: map[string]*string{
			state.completionKey: &observedValue,
		},
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
	if len(blocks) == 0 {
		return nil
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	sessionVars := s.GetSessionVars()
	originalInRestrictedSQL := sessionVars.InRestrictedSQL
	sessionVars.InRestrictedSQL = true
	defer func() {
		sessionVars.InRestrictedSQL = originalInRestrictedSQL
	}()

	for blockIdx, block := range blocks {
		rendered := renderStarterBootstrapSQL(block)
		stmts, err := s.Parse(ctx, rendered)
		if err != nil {
			return errors.Annotatef(err, "parse SQL block %d", blockIdx)
		}
		if len(stmts) != 1 {
			return errors.Errorf("SQL block %d must contain exactly one statement", blockIdx)
		}
		rs, err := s.ExecuteStmt(ctx, stmts[0])
		if err != nil {
			return errors.Annotatef(err, "execute SQL block %d", blockIdx)
		}
		if rs != nil {
			if err := rs.Close(); err != nil {
				return errors.Annotate(err, "close SQL result")
			}
		}
	}
	return nil
}

func renderStarterBootstrapSQL(sql string) string {
	keyspaceName := sqlescape.EscapeString(config.GetGlobalKeyspaceName())
	return strings.ReplaceAll(sql, starterBootstrapKeyspacePlaceholder, keyspaceName)
}
