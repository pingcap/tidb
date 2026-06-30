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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"go.uber.org/zap"
)

const (
	starterBootstrapVersionVar          = "starter_version"
	starterBootstrapKeyspacePlaceholder = "<keyspace>"
	starterBootstrapVersionComment      = "Starter bootstrap file version. Do not delete."
)

var starterBootstrapPlaceholderRe = regexp.MustCompile(`<[A-Za-z0-9_-]+>`)

type starterBootstrapFile struct {
	Version   int64                         `json:"version"`
	Bootstrap []string                      `json:"bootstrap,omitempty"`
	Upgrades  []starterBootstrapFileUpgrade `json:"upgrades,omitempty"`
}

type starterBootstrapFileUpgrade struct {
	Version int64    `json:"version"`
	SQL     []string `json:"sql,omitempty"`
}

func runStarterBootstrap(s sessionapi.Session) error {
	bootstrapFile, err := loadStarterBootstrapFile()
	if err != nil {
		return err
	}
	if bootstrapFile == nil {
		return nil
	}
	if err := executeStarterBootstrapSQLBlocks(s, bootstrapFile.Bootstrap); err != nil {
		return err
	}
	return updateStarterBootstrapVersion(s, bootstrapFile.Version)
}

func upgradeStarterBootstrap(store kv.Storage) error {
	bootstrapFile, err := loadStarterBootstrapFile()
	if err != nil {
		return err
	}
	if bootstrapFile == nil {
		return nil
	}

	startTime := time.Now()
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
	if !needStarterBootstrapUpgrade(storedVersion, bootstrapFile) {
		return nil
	}

	releaseFn, err := acquireLock(store)
	if err != nil {
		return errors.Annotate(err, "acquire starter bootstrap file upgrade lock")
	}
	defer releaseFn()
	storedVersion, err = getStarterBootstrapVersion(s)
	if err != nil {
		return err
	}
	if !needStarterBootstrapUpgrade(storedVersion, bootstrapFile) {
		return nil
	}

	if err = upgradeStarterBootstrapFromVersion(s, bootstrapFile, storedVersion); err != nil {
		return err
	}
	logutil.BgLogger().Info("starter bootstrap file upgrade finished",
		zap.Int64("version", bootstrapFile.Version),
		zap.Duration("cost", time.Since(startTime)))
	return nil
}

func loadStarterBootstrapFile() (*starterBootstrapFile, error) {
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
		zap.Int("bootstrapBlocks", len(bootstrapFile.Bootstrap)),
		zap.Int("upgradeEntries", len(bootstrapFile.Upgrades)))
	return bootstrapFile, nil
}

func parseStarterBootstrapFile(data []byte) (*starterBootstrapFile, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var bootstrapFile starterBootstrapFile
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

func (m *starterBootstrapFile) validate() error {
	if m.Version <= 0 {
		return errors.New("bootstrap file version must be greater than 0")
	}
	if err := validateStarterBootstrapSQLBlocks("bootstrap", m.Bootstrap); err != nil {
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
		if err := validateStarterBootstrapSQLBlocks(fmt.Sprintf("upgrades[%d].sql", i), upgrade.SQL); err != nil {
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

func needStarterBootstrapUpgrade(storedVersion int64, bootstrapFile *starterBootstrapFile) bool {
	if storedVersion > bootstrapFile.Version {
		logutil.BgLogger().Warn("starter bootstrap file is older than cluster state",
			zap.Int64("storedVersion", storedVersion),
			zap.Int64("bootstrapFileVersion", bootstrapFile.Version))
		return false
	}
	return storedVersion < bootstrapFile.Version
}

func upgradeStarterBootstrapFromVersion(s sessionapi.Session, bootstrapFile *starterBootstrapFile, storedVersion int64) error {
	if !needStarterBootstrapUpgrade(storedVersion, bootstrapFile) {
		return nil
	}

	for _, upgrade := range bootstrapFile.pendingUpgrades(storedVersion) {
		logutil.BgLogger().Info("running starter bootstrap file upgrade",
			zap.Int64("storedVersion", storedVersion),
			zap.Int64("upgradeVersion", upgrade.Version),
			zap.Int64("targetVersion", bootstrapFile.Version))
		if err := executeStarterBootstrapSQLBlocks(s, upgrade.SQL); err != nil {
			return errors.Annotatef(err, "upgrade starter bootstrap file to version %d", upgrade.Version)
		}
	}
	if err := updateStarterBootstrapVersion(s, bootstrapFile.Version); err != nil {
		return err
	}

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	_, err := s.ExecuteInternal(ctx, "COMMIT")
	if err == nil {
		return nil
	}
	sleepTime := time.Second
	logutil.BgLogger().Info("update starter bootstrap file version failed",
		zap.Error(err), zap.Duration("sleeping time", sleepTime))
	time.Sleep(sleepTime)
	latestVersion, err1 := getStarterBootstrapVersion(s)
	if err1 != nil {
		return errors.Annotate(err1, "check starter bootstrap file version after commit failure")
	}
	if latestVersion >= bootstrapFile.Version {
		return nil
	}
	return errors.Annotatef(err, "upgrade starter bootstrap file from version %d to %d", storedVersion, bootstrapFile.Version)
}

func (m *starterBootstrapFile) pendingUpgrades(storedVersion int64) []starterBootstrapFileUpgrade {
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
		for stmtIdx, stmt := range stmts {
			rs, err := s.ExecuteStmt(ctx, stmt)
			if err != nil {
				return errors.Annotatef(err, "execute SQL block %d statement %d", blockIdx, stmtIdx)
			}
			if rs != nil {
				if err := rs.Close(); err != nil {
					return errors.Annotate(err, "close SQL result")
				}
			}
		}
	}
	return nil
}

func renderStarterBootstrapSQL(sql string) string {
	keyspaceName := sqlescape.EscapeString(config.GetGlobalKeyspaceName())
	return strings.ReplaceAll(sql, starterBootstrapKeyspacePlaceholder, keyspaceName)
}
