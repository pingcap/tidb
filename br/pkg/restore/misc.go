// Copyright 2024 PingCAP, Inc.
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

package restore

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// deprecated parameter
type Granularity string

const (
	FineGrained   Granularity = "fine-grained"
	CoarseGrained Granularity = "coarse-grained"
)

type UniqueTableName struct {
	DB    string
	Table string
}

func TransferBoolToValue(enable bool) string {
	if enable {
		return "ON"
	}
	return "OFF"
}

// GetTableSchema returns the schema of a table from TiDB.
func GetTableSchema(
	dom *domain.Domain,
	dbName pmodel.CIStr,
	tableName pmodel.CIStr,
) (*model.TableInfo, error) {
	info := dom.InfoSchema()
	table, err := info.TableByName(context.Background(), dbName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return table.Meta(), nil
}

const maxUserTablesNum = 10

// AssertUserDBsEmpty check whether user dbs exist in the cluster
func AssertUserDBsEmpty(dom *domain.Domain) error {
	databases := dom.InfoSchema().AllSchemas()
	m := meta.NewReader(dom.Store().GetSnapshot(kv.MaxVersion))
	userTables := make([]string, 0, maxUserTablesNum+1)
	appendTables := func(dbName, tableName string) bool {
		if len(userTables) >= maxUserTablesNum {
			userTables = append(userTables, "...")
			return true
		}
		userTables = append(userTables, fmt.Sprintf("%s.%s", dbName, tableName))
		return false
	}
LISTDBS:
	for _, db := range databases {
		dbName := db.Name.L
		if tidbutil.IsMemOrSysDB(dbName) {
			continue
		}
		tables, err := m.ListSimpleTables(db.ID)
		if err != nil {
			return errors.Annotatef(err, "failed to iterator tables of database[id=%d]", db.ID)
		}
		if len(tables) == 0 {
			// tidb create test db on fresh cluster
			// if it's empty we don't take it as user db
			if dbName != "test" {
				if appendTables(db.Name.O, "") {
					break LISTDBS
				}
			}
			continue
		}
		for _, table := range tables {
			if appendTables(db.Name.O, table.Name.O) {
				break LISTDBS
			}
		}
	}
	if len(userTables) > 0 {
		return errors.Annotate(berrors.ErrRestoreNotFreshCluster,
			"user db/tables: "+strings.Join(userTables, ", "))
	}
	return nil
}

// GetTS gets a new timestamp from PD.
func GetTS(ctx context.Context, pdClient pd.Client) (uint64, error) {
	p, l, err := pdClient.GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	restoreTS := oracle.ComposeTS(p, l)
	return restoreTS, nil
}

// GetTSWithRetry gets a new timestamp with retry from PD.
func GetTSWithRetry(ctx context.Context, pdClient pd.Client) (uint64, error) {
	var (
		startTS  uint64
		getTSErr error
		retry    uint
	)

	err := utils.WithRetry(ctx, func() error {
		startTS, getTSErr = GetTS(ctx, pdClient)
		failpoint.Inject("get-ts-error", func(val failpoint.Value) {
			if val.(bool) && retry < 3 {
				getTSErr = errors.Errorf("rpc error: code = Unknown desc = [PD:tso:ErrGenerateTimestamp]generate timestamp failed, requested pd is not leader of cluster")
			}
		})

		retry++
		if getTSErr != nil {
			log.Warn("failed to get TS, retry it", zap.Uint("retry time", retry), logutil.ShortError(getTSErr))
		}
		return getTSErr
	}, utils.NewPDReqBackoffer())

	if err != nil {
		log.Error("failed to get TS", zap.Error(err))
	}
	return startTS, errors.Trace(err)
}

// HasRestoreIDColumn checks if the tidb_pitr_id_map table has restore_id column
func HasRestoreIDColumn(dom *domain.Domain) bool {
	table, err := GetTableSchema(dom, pmodel.NewCIStr("mysql"), pmodel.NewCIStr("tidb_pitr_id_map"))
	if err != nil {
		return false
	}

	for _, col := range table.Columns {
		if col.Name.L == "restore_id" {
			return true
		}
	}
	return false
}

type regionScanner struct {
	regionClient split.SplitClient
	regionCache  []*split.RegionInfo
	cacheSize    int
}

func NewRegionScanner(regionClient split.SplitClient, cacheSize int) *regionScanner {
	return &regionScanner{
		regionClient: regionClient,
		cacheSize:    cacheSize,
	}
}

func (scanner *regionScanner) locateRegionFromRemote(ctx context.Context, key []byte) (*split.RegionInfo, error) {
	regionInfos, err := split.ScanRegionsWithRetry(ctx, scanner.regionClient, key, []byte(""), scanner.cacheSize)
	if err != nil {
		return nil, errors.Trace(err)
	}
	scanner.regionCache = regionInfos
	return scanner.regionCache[0], nil
}

func (scanner *regionScanner) locateRegionFromCache(ctx context.Context, key []byte) (*split.RegionInfo, error) {
	if len(scanner.regionCache) == 0 {
		return scanner.locateRegionFromRemote(ctx, key)
	}
	if bytes.Compare(key, scanner.regionCache[len(scanner.regionCache)-1].Region.EndKey) >= 0 {
		return scanner.locateRegionFromRemote(ctx, key)
	}
	i, ok := slices.BinarySearchFunc(scanner.regionCache, key, func(regionInfo *split.RegionInfo, k []byte) int {
		startCmpRet := bytes.Compare(regionInfo.Region.StartKey, k)
		if startCmpRet <= 0 && (len(regionInfo.Region.EndKey) == 0 || bytes.Compare(regionInfo.Region.EndKey, k) > 0) {
			return 0
		}
		return startCmpRet
	})
	if !ok {
		return scanner.locateRegionFromRemote(ctx, key)
	}
	scanner.regionCache = scanner.regionCache[i:]
	return scanner.regionCache[0], nil
}

func (scanner *regionScanner) IsKeyRangeInOneRegion(ctx context.Context, startKey, endKey []byte) (bool, error) {
	regionInfo, err := scanner.locateRegionFromCache(ctx, startKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	return len(regionInfo.Region.EndKey) == 0 || bytes.Compare(endKey, regionInfo.Region.EndKey) < 0, nil
}

type BackupFileSetWithKeyRange struct {
	backupFileSet BackupFileSet
	startKey      []byte
	endKey        []byte
}

func GroupOverlappedBackupFileSetsIter(ctx context.Context, regionClient split.SplitClient, backupFileSets []BackupFileSet, fn func(BatchBackupFileSet)) error {
	backupFileSetWithKeyRanges := make([]*BackupFileSetWithKeyRange, 0, len(backupFileSets))
	for _, backupFileSet := range backupFileSets {
		startKey, endKey, err := getKeyRangeForBackupFileSet(backupFileSet)
		if err != nil {
			return errors.Trace(err)
		}
		backupFileSetWithKeyRanges = append(backupFileSetWithKeyRanges, &BackupFileSetWithKeyRange{
			backupFileSet: backupFileSet,
			startKey:      startKey,
			endKey:        endKey,
		})
	}
	slices.SortFunc(backupFileSetWithKeyRanges, func(a, b *BackupFileSetWithKeyRange) int {
		startKeyCmp := bytes.Compare(a.startKey, b.startKey)
		if startKeyCmp == 0 {
			return bytes.Compare(a.endKey, b.endKey)
		}
		return startKeyCmp
	})
	regionScanner := NewRegionScanner(regionClient, 64)
	var thisBackupFileSet *BackupFileSet = nil
	thisBatchBackupFileSet := make([]BackupFileSet, 0)
	lastEndKey := []byte{}
	for _, file := range backupFileSetWithKeyRanges {
		if bytes.Compare(lastEndKey, file.startKey) < 0 {
			// the next file is not overlapped with this backup file set anymore, so add the set
			// into the batch set.
			if thisBackupFileSet != nil {
				thisBatchBackupFileSet = append(thisBatchBackupFileSet, *thisBackupFileSet)
				thisBackupFileSet = nil
			}
			// create new this backup file set
			thisBackupFileSet = &BackupFileSet{
				TableID:      file.backupFileSet.TableID,
				SSTFiles:     make([]*backuppb.File, 0),
				RewriteRules: file.backupFileSet.RewriteRules,
			}
			thisBackupFileSet.SSTFiles = append(thisBackupFileSet.SSTFiles, file.backupFileSet.SSTFiles...)
			// check whether [lastEndKey, file.startKey] is in the one region
			inOneRegion, err := regionScanner.IsKeyRangeInOneRegion(ctx, lastEndKey, file.startKey)
			if err != nil {
				return errors.Trace(err)
			}
			if !inOneRegion && len(thisBatchBackupFileSet) > 0 {
				// not in the same region, so this batch backup file set can be output
				log.Info("generating one batch.", zap.Int("size", len(thisBatchBackupFileSet)),
					zap.Binary("from", lastEndKey),
					zap.Binary("to", file.startKey))
				fn(thisBatchBackupFileSet)
				thisBatchBackupFileSet = make([]BackupFileSet, 0)
			}
			lastEndKey = file.endKey
		} else {
			// the next file is overlapped with this backup file set, so add the file
			// into the set.
			thisBackupFileSet.SSTFiles = append(thisBackupFileSet.SSTFiles, file.backupFileSet.SSTFiles...)
			if thisBackupFileSet.TableID != file.backupFileSet.TableID || !thisBackupFileSet.RewriteRules.Equal(file.backupFileSet.RewriteRules) {
				log.Error("the overlapped SST must have the same table id and rewrite rules",
					zap.Int64("set table id", thisBackupFileSet.TableID),
					zap.Int64("file table id", file.backupFileSet.TableID),
					zap.Reflect("set rewrite rule", thisBackupFileSet.RewriteRules),
					zap.Reflect("file rewrite rule", file.backupFileSet.RewriteRules),
				)
				return errors.Errorf("the overlapped SST must have the same table id(%d<>%d) and rewrite rules",
					thisBackupFileSet.TableID, file.backupFileSet.TableID)
			}
			// update lastEndKey if file.endKey is larger
			if bytes.Compare(lastEndKey, file.endKey) < 0 {
				lastEndKey = file.endKey
			}
		}
	}
	// add the set into the batch set.
	if thisBackupFileSet != nil {
		thisBatchBackupFileSet = append(thisBatchBackupFileSet, *thisBackupFileSet)
	}
	// output the last batch backup file set
	if len(thisBatchBackupFileSet) > 0 {
		log.Info("generating one batch.", zap.Int("size", len(thisBatchBackupFileSet)),
			zap.Binary("from", lastEndKey),
			zap.Binary("to", []byte{}))
		fn(thisBatchBackupFileSet)
	}
	return nil
}

func getKeyRangeForBackupFileSet(backupFileSet BackupFileSet) ([]byte, []byte, error) {
	var startKey, endKey []byte
	for _, f := range backupFileSet.SSTFiles {
		start, end, err := restoreutils.GetRewriteRawKeys(f, backupFileSet.RewriteRules)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if len(startKey) == 0 || bytes.Compare(start, startKey) < 0 {
			startKey = start
		}
		if len(endKey) == 0 || bytes.Compare(endKey, end) < 0 {
			endKey = end
		}
	}
	return startKey, endKey, nil
}
