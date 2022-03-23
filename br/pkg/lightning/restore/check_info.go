// Copyright 2020 PingCAP, Inc.
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
	"database/sql"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"modernc.org/mathutil"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/store/pdtypes"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
)

const (
	pdStores       = "/pd/api/v1/stores"
	pdReplicate    = "/pd/api/v1/config/replicate"
	pdEmptyRegions = "/pd/api/v1/regions/check/empty-region"

	defaultCSVSize    = 10 * units.GiB
	maxSampleDataSize = 10 * 1024 * 1024
	maxSampleRowCount = 10 * 1024

	warnEmptyRegionCntPerStore  = 500
	errorEmptyRegionCntPerStore = 1000
	warnRegionCntMinMaxRatio    = 0.75
	errorRegionCntMinMaxRatio   = 0.5

	// We only check RegionCntMaxMinRatio when the maximum region count of all stores is larger than this threshold.
	checkRegionCntRatioThreshold = 1000
)

func (rc *Controller) isSourceInLocal() bool {
	return strings.HasPrefix(rc.store.URI(), storage.LocalURIPrefix)
}

func (rc *Controller) getReplicaCount(ctx context.Context) (uint64, error) {
	result := &pdtypes.ReplicationConfig{}
	err := rc.tls.WithHost(rc.cfg.TiDB.PdAddr).GetJSON(ctx, pdReplicate, &result)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return result.MaxReplicas, nil
}

func (rc *Controller) getClusterAvail(ctx context.Context) (uint64, error) {
	result := &pdtypes.StoresInfo{}
	if err := rc.tls.WithHost(rc.cfg.TiDB.PdAddr).GetJSON(ctx, pdStores, result); err != nil {
		return 0, errors.Trace(err)
	}
	clusterAvail := uint64(0)
	for _, store := range result.Stores {
		clusterAvail += uint64(store.Status.Available)
	}
	return clusterAvail, nil
}

// clusterResource check cluster has enough resource to import data. this test can by skipped.
func (rc *Controller) clusterResource(ctx context.Context, localSource int64) error {
	passed := true
	message := "Cluster resources are rich for this import task"
	defer func() {
		rc.checkTemplate.Collect(Critical, passed, message)
	}()

	var (
		clusterAvail  uint64
		clusterSource uint64
	)
	if rc.taskMgr == nil {
		var err error
		clusterAvail, err = rc.getClusterAvail(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		clusterSource = uint64(localSource)
	} else {
		if err := rc.taskMgr.CheckTasksExclusively(ctx, func(tasks []taskMeta) ([]taskMeta, error) {
			clusterAvail = 0
			clusterSource = 0
			restoreStarted := false
			for _, task := range tasks {
				if task.status > taskMetaStatusInitial {
					restoreStarted = true
				}
				clusterSource += task.sourceBytes
				if task.clusterAvail > 0 {
					clusterAvail = task.clusterAvail
				}
			}
			if restoreStarted || clusterAvail > 0 {
				return nil, nil
			}

			var err error
			clusterAvail, err = rc.getClusterAvail(ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			newTasks := append([]taskMeta(nil), tasks...)
			for i := 0; i < len(newTasks); i++ {
				newTasks[i].clusterAvail = clusterAvail
			}
			return newTasks, nil
		}); err != nil {
			return errors.Trace(err)
		}
	}

	replicaCount, err := rc.getReplicaCount(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	estimateSize := clusterSource * replicaCount
	if estimateSize > clusterAvail {
		passed = false
		message = fmt.Sprintf("Cluster doesn't have enough space, available is %s, but we need %s",
			units.BytesSize(float64(clusterAvail)), units.BytesSize(float64(estimateSize)))
	} else {
		message = fmt.Sprintf("Cluster available is rich, available is %s, we need %s",
			units.BytesSize(float64(clusterAvail)), units.BytesSize(float64(estimateSize)))
	}
	return nil
}

// ClusterIsAvailable check cluster is available to import data. this test can be skipped.
func (rc *Controller) ClusterIsAvailable(ctx context.Context) {
	passed := true
	message := "Cluster is available"
	defer func() {
		rc.checkTemplate.Collect(Critical, passed, message)
	}()
	checkCtx := &backend.CheckCtx{
		DBMetas: rc.dbMetas,
	}
	if err := rc.backend.CheckRequirements(ctx, checkCtx); err != nil {
		err = common.NormalizeError(err)
		passed = false
		message = fmt.Sprintf("cluster available check failed: %s", err.Error())
	}
}

func isTiFlash(store *pdtypes.MetaStore) bool {
	for _, label := range store.Labels {
		if label.Key == "engine" && label.Value == "tiflash" {
			return true
		}
	}
	return false
}

func (rc *Controller) checkEmptyRegion(ctx context.Context) error {
	passed := true
	message := "Cluster doesn't have too many empty regions"
	defer func() {
		rc.checkTemplate.Collect(Critical, passed, message)
	}()
	storeInfo := &pdtypes.StoresInfo{}
	err := rc.tls.WithHost(rc.cfg.TiDB.PdAddr).GetJSON(ctx, pdStores, storeInfo)
	if err != nil {
		return errors.Trace(err)
	}
	if len(storeInfo.Stores) <= 1 {
		return nil
	}

	var result pdtypes.RegionsInfo
	if err := rc.tls.WithHost(rc.cfg.TiDB.PdAddr).GetJSON(ctx, pdEmptyRegions, &result); err != nil {
		return errors.Trace(err)
	}
	regions := make(map[uint64]int)
	stores := make(map[uint64]*pdtypes.StoreInfo)
	for _, region := range result.Regions {
		for _, peer := range region.Peers {
			regions[peer.StoreId]++
		}
	}
	for _, store := range storeInfo.Stores {
		stores[store.Store.GetId()] = store
	}
	tableCount := 0
	for _, db := range rc.dbMetas {
		info, ok := rc.dbInfos[db.Name]
		if !ok {
			continue
		}
		tableCount += len(info.Tables)
	}
	errorThrehold := mathutil.Max(errorEmptyRegionCntPerStore, tableCount*3)
	warnThrehold := mathutil.Max(warnEmptyRegionCntPerStore, tableCount)
	var (
		errStores  []string
		warnStores []string
	)
	for storeID, regionCnt := range regions {
		if store, ok := stores[storeID]; ok {
			if metapb.StoreState(metapb.StoreState_value[store.Store.StateName]) != metapb.StoreState_Up {
				continue
			}
			if isTiFlash(store.Store) {
				continue
			}
			if regionCnt > errorThrehold {
				errStores = append(errStores, strconv.Itoa(int(storeID)))
			} else if regionCnt > warnThrehold {
				warnStores = append(warnStores, strconv.Itoa(int(storeID)))
			}
		}
	}

	var messages []string
	if len(errStores) > 0 {
		passed = false
		messages = append(messages, fmt.Sprintf("TiKV stores (%s) contains more than %v empty regions respectively, "+
			"which will greatly affect the import speed and success rate", strings.Join(errStores, ", "), errorEmptyRegionCntPerStore))
	}
	if len(warnStores) > 0 {
		messages = append(messages, fmt.Sprintf("TiKV stores (%s) contains more than %v empty regions respectively, "+
			"which will affect the import speed and success rate", strings.Join(warnStores, ", "), warnEmptyRegionCntPerStore))
	}
	if len(messages) > 0 {
		message = strings.Join(messages, "\n")
	}
	return nil
}

// checkRegionDistribution checks if regions distribution is unbalanced.
func (rc *Controller) checkRegionDistribution(ctx context.Context) error {
	passed := true
	message := "Cluster region distribution is balanced"
	defer func() {
		rc.checkTemplate.Collect(Critical, passed, message)
	}()

	result := &pdtypes.StoresInfo{}
	err := rc.tls.WithHost(rc.cfg.TiDB.PdAddr).GetJSON(ctx, pdStores, result)
	if err != nil {
		return errors.Trace(err)
	}
	stores := make([]*pdtypes.StoreInfo, 0, len(result.Stores))
	for _, store := range result.Stores {
		if metapb.StoreState(metapb.StoreState_value[store.Store.StateName]) != metapb.StoreState_Up {
			continue
		}
		if isTiFlash(store.Store) {
			continue
		}
		stores = append(stores, store)
	}
	if len(stores) <= 1 {
		return nil
	}
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].Status.RegionCount < stores[j].Status.RegionCount
	})
	minStore := stores[0]
	maxStore := stores[len(stores)-1]
	tableCount := 0
	for _, db := range rc.dbMetas {
		info, ok := rc.dbInfos[db.Name]
		if !ok {
			continue
		}
		tableCount += len(info.Tables)
	}
	threhold := mathutil.Max(checkRegionCntRatioThreshold, tableCount)
	if maxStore.Status.RegionCount <= threhold {
		return nil
	}
	ratio := float64(minStore.Status.RegionCount) / float64(maxStore.Status.RegionCount)
	if ratio < errorRegionCntMinMaxRatio {
		passed = false
		message = fmt.Sprintf("Region distribution is unbalanced, the ratio of the regions count of the store(%v) "+
			"with least regions(%v) to the store(%v) with most regions(%v) is %v, but we expect it must not be less than %v",
			minStore.Store.GetId(), minStore.Status.RegionCount, maxStore.Store.GetId(), maxStore.Status.RegionCount, ratio, errorRegionCntMinMaxRatio)
	} else if ratio < warnRegionCntMinMaxRatio {
		message = fmt.Sprintf("Region distribution is unbalanced, the ratio of the regions count of the store(%v) "+
			"with least regions(%v) to the store(%v) with most regions(%v) is %v, but we expect it should not be less than %v",
			minStore.Store.GetId(), minStore.Status.RegionCount, maxStore.Store.GetId(), maxStore.Status.RegionCount, ratio, warnRegionCntMinMaxRatio)
	}
	return nil
}

// checkClusterRegion checks cluster if there are too many empty regions or region distribution is unbalanced.
func (rc *Controller) checkClusterRegion(ctx context.Context) error {
	err := rc.taskMgr.CheckTasksExclusively(ctx, func(tasks []taskMeta) ([]taskMeta, error) {
		restoreStarted := false
		for _, task := range tasks {
			if task.status > taskMetaStatusInitial {
				restoreStarted = true
				break
			}
		}
		if restoreStarted {
			return nil, nil
		}
		if err := rc.checkEmptyRegion(ctx); err != nil {
			return nil, errors.Trace(err)
		}
		if err := rc.checkRegionDistribution(ctx); err != nil {
			return nil, errors.Trace(err)
		}
		return nil, nil
	})
	return errors.Trace(err)
}

// StoragePermission checks whether Lightning has enough permission to storage.
// this test cannot be skipped.
func (rc *Controller) StoragePermission(ctx context.Context) error {
	passed := true
	message := "Lightning has the correct storage permission"
	defer func() {
		rc.checkTemplate.Collect(Critical, passed, message)
	}()

	u, err := storage.ParseBackend(rc.cfg.Mydumper.SourceDir, nil)
	if err != nil {
		return common.NormalizeError(err)
	}
	_, err = storage.New(ctx, u, &storage.ExternalStorageOptions{
		CheckPermissions: []storage.Permission{
			storage.ListObjects,
			storage.GetObject,
		},
	})
	if err != nil {
		passed = false
		message = err.Error()
	}
	return nil
}

// HasLargeCSV checks whether input csvs is fit for Lightning import.
// If strictFormat is false, and csv file is large. Lightning will have performance issue.
// this test cannot be skipped.
func (rc *Controller) HasLargeCSV(dbMetas []*mydump.MDDatabaseMeta) {
	passed := true
	message := "Source csv files size is proper"
	defer func() {
		rc.checkTemplate.Collect(Warn, passed, message)
	}()
	if !rc.cfg.Mydumper.StrictFormat {
		for _, db := range dbMetas {
			for _, t := range db.Tables {
				for _, f := range t.DataFiles {
					if f.FileMeta.FileSize > defaultCSVSize {
						message = fmt.Sprintf("large csv: %s file exists and it will slow down import performance", f.FileMeta.Path)
						passed = false
					}
				}
			}
		}
	} else {
		message = "Skip the csv size check, because config.StrictFormat is true"
	}
}

func (rc *Controller) estimateSourceData(ctx context.Context) (int64, error) {
	sourceSize := int64(0)
	originSource := int64(0)
	bigTableCount := 0
	tableCount := 0
	unSortedTableCount := 0
	errMgr := errormanager.New(nil, rc.cfg)
	for _, db := range rc.dbMetas {
		info, ok := rc.dbInfos[db.Name]
		if !ok {
			continue
		}
		for _, tbl := range db.Tables {
			originSource += tbl.TotalSize
			tableInfo, ok := info.Tables[tbl.Name]
			if ok {
				// Do not sample small table because there may a large number of small table and it will take a long
				// time to sample data for all of them.
				if rc.cfg.TikvImporter.Backend == config.BackendTiDB || tbl.TotalSize < int64(config.SplitRegionSize) {
					sourceSize += tbl.TotalSize
					tbl.IndexRatio = 1.0
					tbl.IsRowOrdered = false
				} else {
					if err := rc.sampleDataFromTable(ctx, db.Name, tbl, tableInfo.Core, errMgr); err != nil {
						return sourceSize, errors.Trace(err)
					}

					if tbl.IndexRatio > 0 {
						sourceSize += int64(float64(tbl.TotalSize) * tbl.IndexRatio)
					} else {
						// if sample data failed due to max-error, fallback to use source size
						sourceSize += tbl.TotalSize
					}

					if tbl.TotalSize > int64(config.DefaultBatchSize)*2 {
						bigTableCount += 1
						if !tbl.IsRowOrdered {
							unSortedTableCount += 1
						}
					}
				}
				tableCount += 1
			}
		}
	}

	if rc.status != nil {
		rc.status.TotalFileSize.Store(originSource)
	}
	// Do not import with too large concurrency because these data may be all unsorted.
	if bigTableCount > 0 && unSortedTableCount > 0 {
		if rc.cfg.App.TableConcurrency > rc.cfg.App.IndexConcurrency {
			rc.cfg.App.TableConcurrency = rc.cfg.App.IndexConcurrency
		}
	}
	return sourceSize, nil
}

// localResource checks the local node has enough resources for this import when local backend enabled;
func (rc *Controller) localResource(sourceSize int64) error {
	if rc.isSourceInLocal() {
		sourceDir := strings.TrimPrefix(rc.cfg.Mydumper.SourceDir, storage.LocalURIPrefix)
		same, err := common.SameDisk(sourceDir, rc.cfg.TikvImporter.SortedKVDir)
		if err != nil {
			return errors.Trace(err)
		}
		if same {
			rc.checkTemplate.Collect(Warn, false,
				fmt.Sprintf("sorted-kv-dir:%s and data-source-dir:%s are in the same disk, may slow down performance",
					rc.cfg.TikvImporter.SortedKVDir, sourceDir))
		}
	}

	storageSize, err := common.GetStorageSize(rc.cfg.TikvImporter.SortedKVDir)
	if err != nil {
		return errors.Trace(err)
	}
	localAvailable := int64(storageSize.Available)

	var message string
	var passed bool
	switch {
	case localAvailable > sourceSize:
		message = fmt.Sprintf("local disk resources are rich, estimate sorted data size %s, local available is %s",
			units.BytesSize(float64(sourceSize)), units.BytesSize(float64(localAvailable)))
		passed = true
	case int64(rc.cfg.TikvImporter.DiskQuota) > localAvailable:
		message = fmt.Sprintf("local disk space may not enough to finish import, estimate sorted data size is %s,"+
			" but local available is %s, please set `tikv-importer.disk-quota` to a smaller value than %s"+
			" or change `mydumper.sorted-kv-dir` to another disk with enough space to finish imports",
			units.BytesSize(float64(sourceSize)),
			units.BytesSize(float64(localAvailable)), units.BytesSize(float64(localAvailable)))
		passed = false
		log.L().Error(message)
	default:
		message = fmt.Sprintf("local disk space may not enough to finish import, "+
			"estimate sorted data size is %s, but local available is %s,"+
			"we will use disk-quota (size: %s) to finish imports, which may slow down import",
			units.BytesSize(float64(sourceSize)),
			units.BytesSize(float64(localAvailable)), units.BytesSize(float64(rc.cfg.TikvImporter.DiskQuota)))
		passed = true
		log.L().Warn(message)
	}
	rc.checkTemplate.Collect(Critical, passed, message)
	return nil
}

// CheckpointIsValid checks whether we can start this import with this checkpoint.
func (rc *Controller) CheckpointIsValid(ctx context.Context, tableInfo *mydump.MDTableMeta) ([]string, bool) {
	msgs := make([]string, 0)
	uniqueName := common.UniqueTable(tableInfo.DB, tableInfo.Name)
	tableCheckPoint, err := rc.checkpointsDB.Get(ctx, uniqueName)
	if err != nil {
		// there is no checkpoint
		log.L().Debug("no checkpoint detected", zap.String("table", uniqueName))
		return nil, true
	}
	// if checkpoint enable and not missing, we skip the check table empty progress.
	if tableCheckPoint.Status <= checkpoints.CheckpointStatusMissing {
		return nil, false
	}

	if tableCheckPoint.Status <= checkpoints.CheckpointStatusMaxInvalid {
		failedStep := tableCheckPoint.Status * 10
		var action strings.Builder
		action.WriteString("./tidb-lightning-ctl --checkpoint-error-")
		switch failedStep {
		case checkpoints.CheckpointStatusAlteredAutoInc, checkpoints.CheckpointStatusAnalyzed:
			action.WriteString("ignore")
		default:
			action.WriteString("destroy")
		}
		action.WriteString("='")
		action.WriteString(uniqueName)
		action.WriteString("' --config=...")

		msgs = append(msgs, fmt.Sprintf("TiDB Lightning has failed last time. To prevent data loss, this run will stop now, "+
			"%s failed in step(%s), please run command %s,"+
			"You may also run `./tidb-lightning-ctl --checkpoint-error-destroy=all --config=...` to start from scratch,"+
			"For details of this failure, read the log file from the PREVIOUS run",
			uniqueName, failedStep.MetricName(), action.String()))
		return msgs, false
	}

	dbInfo, ok := rc.dbInfos[tableInfo.DB]
	if ok {
		t, ok := dbInfo.Tables[tableInfo.Name]
		if ok {
			if tableCheckPoint.TableID > 0 && tableCheckPoint.TableID != t.ID {
				msgs = append(msgs, fmt.Sprintf("TiDB Lightning has detected tables with illegal checkpoints. To prevent data loss, this run will stop now,"+
					"please run command \"./tidb-lightning-ctl --checkpoint-remove='%s' --config=...\""+
					"You may also run `./tidb-lightning-ctl --checkpoint-error-destroy=all --config=...` to start from scratch,"+
					"For details of this failure, read the log file from the PREVIOUS run",
					uniqueName))
				return msgs, false
			}
		}
	}

	var permFromCheckpoint []int
	var columns []string
	for _, eng := range tableCheckPoint.Engines {
		if len(eng.Chunks) > 0 {
			chunk := eng.Chunks[0]
			permFromCheckpoint = chunk.ColumnPermutation
			columns = chunk.Chunk.Columns
			if filepath.Dir(chunk.FileMeta.Path) != rc.cfg.Mydumper.SourceDir {
				message := fmt.Sprintf("chunk checkpoints path is not equal to config"+
					"checkpoint is %s, config source dir is %s", chunk.FileMeta.Path, rc.cfg.Mydumper.SourceDir)
				msgs = append(msgs, message)
			}
		}
	}
	if len(columns) == 0 {
		log.L().Debug("no valid checkpoint detected", zap.String("table", uniqueName))
		return nil, false
	}
	info := rc.dbInfos[tableInfo.DB].Tables[tableInfo.Name]
	if info != nil {
		permFromTiDB, err := parseColumnPermutations(info.Core, columns, nil)
		if err != nil {
			msgs = append(msgs, fmt.Sprintf("failed to calculate columns %s, table %s's info has changed,"+
				"consider remove this checkpoint, and start import again.", err.Error(), uniqueName))
		}
		if !reflect.DeepEqual(permFromCheckpoint, permFromTiDB) {
			msgs = append(msgs, fmt.Sprintf("compare columns perm failed. table %s's info has changed,"+
				"consider remove this checkpoint, and start import again.", uniqueName))
		}
	}
	return msgs, false
}

// hasDefault represents col has default value.
func hasDefault(col *model.ColumnInfo) bool {
	return col.DefaultIsExpr || col.DefaultValue != nil || !mysql.HasNotNullFlag(col.Flag) ||
		col.IsGenerated() || mysql.HasAutoIncrementFlag(col.Flag)
}

func (rc *Controller) readFirstRow(ctx context.Context, dataFileMeta mydump.SourceFileMeta) (cols []string, row []types.Datum, err error) {
	var reader storage.ReadSeekCloser
	if dataFileMeta.Type == mydump.SourceTypeParquet {
		reader, err = mydump.OpenParquetReader(ctx, rc.store, dataFileMeta.Path, dataFileMeta.FileSize)
	} else {
		reader, err = rc.store.Open(ctx, dataFileMeta.Path)
	}
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var parser mydump.Parser
	blockBufSize := int64(rc.cfg.Mydumper.ReadBlockSize)
	switch dataFileMeta.Type {
	case mydump.SourceTypeCSV:
		hasHeader := rc.cfg.Mydumper.CSV.Header
		// Create a utf8mb4 convertor to encode and decode data with the charset of CSV files.
		charsetConvertor, err := mydump.NewCharsetConvertor(rc.cfg.Mydumper.DataCharacterSet, rc.cfg.Mydumper.DataInvalidCharReplace)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		parser, err = mydump.NewCSVParser(&rc.cfg.Mydumper.CSV, reader, blockBufSize, rc.ioWorkers, hasHeader, charsetConvertor)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	case mydump.SourceTypeSQL:
		parser = mydump.NewChunkParser(rc.cfg.TiDB.SQLMode, reader, blockBufSize, rc.ioWorkers)
	case mydump.SourceTypeParquet:
		parser, err = mydump.NewParquetParser(ctx, rc.store, reader, dataFileMeta.Path)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	default:
		panic(fmt.Sprintf("unknown file type '%s'", dataFileMeta.Type))
	}
	defer parser.Close()

	err = parser.ReadRow()
	if err != nil && errors.Cause(err) != io.EOF {
		return nil, nil, errors.Trace(err)
	}
	return parser.Columns(), parser.LastRow().Row, nil
}

// SchemaIsValid checks the import file and cluster schema is match.
func (rc *Controller) SchemaIsValid(ctx context.Context, tableInfo *mydump.MDTableMeta) ([]string, error) {
	if len(tableInfo.DataFiles) == 0 {
		log.L().Info("no data files detected", zap.String("db", tableInfo.DB), zap.String("table", tableInfo.Name))
		return nil, nil
	}

	msgs := make([]string, 0)
	info, ok := rc.dbInfos[tableInfo.DB].Tables[tableInfo.Name]
	if !ok {
		msgs = append(msgs, fmt.Sprintf("TiDB schema `%s`.`%s` doesn't exists,"+
			"please give a schema file in source dir or create table manually", tableInfo.DB, tableInfo.Name))
		return msgs, nil
	}

	igCol, err := rc.cfg.Mydumper.IgnoreColumns.GetIgnoreColumns(tableInfo.DB, tableInfo.Name, rc.cfg.Mydumper.CaseSensitive)
	if err != nil {
		return nil, errors.Trace(err)
	}
	igCols := igCol.ColumnsMap()

	colCountFromTiDB := len(info.Core.Columns)
	core := info.Core
	defaultCols := make(map[string]struct{})
	for _, col := range core.Columns {
		if hasDefault(col) || (info.Core.ContainsAutoRandomBits() && mysql.HasPriKeyFlag(col.Flag)) {
			// this column has default value or it's auto random id, so we can ignore it
			defaultCols[col.Name.L] = struct{}{}
		}
	}
	// tidb_rowid have a default value.
	defaultCols[model.ExtraHandleName.String()] = struct{}{}

	// only check the first file of this table.
	dataFile := tableInfo.DataFiles[0]
	log.L().Info("datafile to check", zap.String("db", tableInfo.DB),
		zap.String("table", tableInfo.Name), zap.String("path", dataFile.FileMeta.Path))
	// get columns name from data file.
	dataFileMeta := dataFile.FileMeta

	if tp := dataFileMeta.Type; tp != mydump.SourceTypeCSV && tp != mydump.SourceTypeSQL && tp != mydump.SourceTypeParquet {
		msgs = append(msgs, fmt.Sprintf("file '%s' with unknown source type '%s'", dataFileMeta.Path, dataFileMeta.Type.String()))
		return msgs, nil
	}
	colsFromDataFile, row, err := rc.readFirstRow(ctx, dataFileMeta)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if colsFromDataFile == nil && len(row) == 0 {
		log.L().Info("file contains no data, skip checking against schema validity", zap.String("path", dataFileMeta.Path))
		return msgs, nil
	}

	if colsFromDataFile == nil {
		// when there is no columns name in data file. we must insert data in order.
		// so the last several columns either can be ignored or has a default value.
		for i := len(row); i < colCountFromTiDB; i++ {
			if _, ok := defaultCols[core.Columns[i].Name.L]; !ok {
				msgs = append(msgs, fmt.Sprintf("TiDB schema `%s`.`%s` has %d columns,"+
					"and data file has %d columns, but column %s are missing the default value,"+
					"please give column a default value to skip this check",
					tableInfo.DB, tableInfo.Name, colCountFromTiDB, len(row), core.Columns[i].Name.L))
			}
		}
		return msgs, nil
	}

	// compare column names and make sure
	// 1. TiDB table info has data file's all columns(besides ignore columns)
	// 2. Those columns not introduced in data file always have a default value.
	colMap := make(map[string]struct{})
	for col := range igCols {
		colMap[col] = struct{}{}
	}
	for _, col := range core.Columns {
		if _, ok := colMap[col.Name.L]; ok {
			// tidb's column is ignored
			// we need ensure this column has the default value.
			if _, hasDefault := defaultCols[col.Name.L]; !hasDefault {
				msgs = append(msgs, fmt.Sprintf("TiDB schema `%s`.`%s`'s column %s cannot be ignored,"+
					"because it doesn't have a default value, please set tables.ignoreColumns properly",
					tableInfo.DB, tableInfo.Name, col.Name.L))
			}
		} else {
			colMap[col.Name.L] = struct{}{}
		}
	}
	// tidb_rowid can be ignored in check
	colMap[model.ExtraHandleName.String()] = struct{}{}
	for _, col := range colsFromDataFile {
		if _, ok := colMap[col]; !ok {
			checkMsg := "please check table schema"
			if dataFileMeta.Type == mydump.SourceTypeCSV && rc.cfg.Mydumper.CSV.Header {
				checkMsg += " and csv file header"
			}
			msgs = append(msgs, fmt.Sprintf("TiDB schema `%s`.`%s` doesn't have column %s, "+
				"%s or use tables.ignoreColumns to ignore %s",
				tableInfo.DB, tableInfo.Name, col, checkMsg, col))
		} else {
			// remove column for next iteration
			delete(colMap, col)
		}
	}
	// if theses rest columns don't have a default value.
	for col := range colMap {
		if _, ok := defaultCols[col]; ok {
			continue
		}
		msgs = append(msgs, fmt.Sprintf("TiDB schema `%s`.`%s` doesn't have the default value for %s. "+
			"Please add default value for column '%s' or choose another column to ignore or add this column in data file",
			tableInfo.DB, tableInfo.Name, col, col))
	}
	return msgs, nil
}

// checkCSVHeader try to check whether the csv header config is consistent with the source csv files by:
// 1. pick one table with two CSV files and a unique/primary key
// 2. read the first row of those two CSV files
// 3. checks if the content of those first rows are compatible with the table schema, and whether the
//    two rows are identical, to determine if the first rows are a header rows.
func (rc *Controller) checkCSVHeader(ctx context.Context, dbMetas []*mydump.MDDatabaseMeta) error {
	// if cfg set header = ture but source files actually contain not header, former SchemaCheck should
	// return error in this situation, so we need do it again.
	if rc.cfg.Mydumper.CSV.Header {
		return nil
	}
	var (
		tableMeta    *mydump.MDTableMeta
		csvCount     int
		hasUniqueIdx bool
	)
	// only check one table source files for better performance. The checked table is chosen based on following two factor:
	// 1. contains at least 1 csv source file, 2 is preferable
	// 2. table schema contains primary key or unique key
	// if the two factors can't be both satisfied, the first one has a higher priority
outer:
	for _, dbMeta := range dbMetas {
		for _, tblMeta := range dbMeta.Tables {
			if len(tblMeta.DataFiles) == 0 {
				continue
			}
			tableHasUniqueIdx := false
			tableCSVCount := 0
			for _, f := range tblMeta.DataFiles {
				if f.FileMeta.Type == mydump.SourceTypeCSV {
					tableCSVCount++
					if tableCSVCount >= 2 {
						break
					}
				}
			}
			if tableCSVCount == 0 {
				continue
			}

			info := rc.dbInfos[tblMeta.DB].Tables[tblMeta.Name]
			for _, idx := range info.Core.Indices {
				if idx.Primary || idx.Unique {
					tableHasUniqueIdx = true
				}
			}

			if tableCSVCount >= 2 && hasUniqueIdx {
				tableMeta = tblMeta
				// if a perfect table source is found, we can stop check more tables
				break outer
			}
			if tableCSVCount > csvCount || (tableCSVCount == csvCount && !hasUniqueIdx && tableHasUniqueIdx) {
				tableMeta = tblMeta
				csvCount = tableCSVCount
				hasUniqueIdx = tableHasUniqueIdx
			}
		}
	}

	if tableMeta == nil {
		return nil
	}

	var rows [][]types.Datum
	for _, f := range tableMeta.DataFiles {
		if f.FileMeta.Type != mydump.SourceTypeCSV {
			continue
		}
		_, row, err := rc.readFirstRow(ctx, f.FileMeta)
		if err != nil {
			return errors.Trace(err)
		}
		if len(row) > 0 {
			rows = append(rows, row)
		}
		// only check at most two of all the files
		if len(rows) >= 2 {
			break
		}
	}
	if len(rows) == 0 {
		return nil
	} else if len(rows) >= 2 {
		// if the first row in two source files are not the same, they should not be the header line
		// NOTE: though lightning's logic allows different source files contains different columns or the
		// order is difference, here we only check if they are exactly the same because this is the common case.
		if len(rows[0]) != len(rows[1]) {
			return nil
		}

		for i := 0; i < len(rows[0]); i++ {
			if rows[0][i].GetString() != rows[1][i].GetString() {
				return nil
			}
		}
	}

	// check if some fields are unique and not ignored
	// if at least one field appears in a unique key, we can sure there is something wrong,
	// they should be either the header line or the data is duplicated.
	tableInfo := rc.dbInfos[tableMeta.DB].Tables[tableMeta.Name]
	tableFields := make(map[string]struct{})
	uniqueIdxFields := make(map[string]struct{})
	ignoreColumns, err := rc.cfg.Mydumper.IgnoreColumns.GetIgnoreColumns(tableMeta.DB, tableMeta.Name, rc.cfg.Mydumper.CaseSensitive)
	if err != nil {
		return errors.Trace(err)
	}
	ignoreColsSet := make(map[string]struct{})
	for _, col := range ignoreColumns.Columns {
		ignoreColsSet[col] = struct{}{}
	}
	for _, idx := range tableInfo.Core.Indices {
		if !idx.Unique && !idx.Primary {
			continue
		}
		for _, col := range idx.Columns {
			if _, ok := ignoreColsSet[col.Name.L]; !ok {
				uniqueIdxFields[col.Name.L] = struct{}{}
			}
		}
	}
	for _, f := range tableInfo.Core.Columns {
		tableFields[f.Name.L] = struct{}{}
	}
	if common.TableHasAutoRowID(tableInfo.Core) {
		tableFields[model.ExtraHandleName.L] = struct{}{}
	}
	hasUniqueField := false
	for _, d := range rows[0] {
		val := strings.ToLower(d.GetString())
		if _, ok := tableFields[val]; !ok {
			return nil
		}
		if _, ok := uniqueIdxFields[val]; ok {
			hasUniqueField = true
			break
		}
	}

	msg := fmt.Sprintf("source csv files contains header row but `mydumper.csv.header` is false, checked table is `%s`.`%s`",
		tableMeta.DB, tableMeta.Name)
	level := Warn
	if hasUniqueField && len(rows) > 1 {
		level = Critical
	} else if !checkFieldCompatibility(tableInfo.Core, ignoreColsSet, rows[0]) {
		// if there are only 1 csv file or there is not unique key, try to check if all columns are compatible with string value
		level = Critical
	}
	rc.checkTemplate.Collect(level, false, msg)

	return nil
}

func checkFieldCompatibility(tbl *model.TableInfo, ignoreCols map[string]struct{}, values []types.Datum) bool {
	se := kv.NewSession(&kv.SessionOptions{
		SQLMode: mysql.ModeStrictTransTables,
	})
	for i, col := range tbl.Columns {
		// do not check ignored columns
		if _, ok := ignoreCols[col.Name.L]; ok {
			continue
		}
		if i >= len(values) {
			break
		}
		_, err := table.CastValue(se, values[i], col, true, false)
		if err != nil {
			log.L().Error("field value is not consistent with column type", zap.String("value", values[i].GetString()),
				zap.Any("column_info", col), zap.Error(err))
			return false
		}
	}

	return true
}

func (rc *Controller) sampleDataFromTable(
	ctx context.Context,
	dbName string,
	tableMeta *mydump.MDTableMeta,
	tableInfo *model.TableInfo,
	errMgr *errormanager.ErrorManager,
) error {
	if len(tableMeta.DataFiles) == 0 {
		return nil
	}
	sampleFile := tableMeta.DataFiles[0].FileMeta
	var reader storage.ReadSeekCloser
	var err error
	if sampleFile.Type == mydump.SourceTypeParquet {
		reader, err = mydump.OpenParquetReader(ctx, rc.store, sampleFile.Path, sampleFile.FileSize)
	} else {
		reader, err = rc.store.Open(ctx, sampleFile.Path)
	}
	if err != nil {
		return errors.Trace(err)
	}
	idAlloc := kv.NewPanickingAllocators(0)
	tbl, err := tables.TableFromMeta(idAlloc, tableInfo)
	if err != nil {
		return errors.Trace(err)
	}
	kvEncoder, err := rc.backend.NewEncoder(tbl, &kv.SessionOptions{
		SQLMode:        rc.cfg.TiDB.SQLMode,
		Timestamp:      0,
		SysVars:        rc.sysVars,
		AutoRandomSeed: 0,
	})
	if err != nil {
		return errors.Trace(err)
	}
	blockBufSize := int64(rc.cfg.Mydumper.ReadBlockSize)

	var parser mydump.Parser
	switch tableMeta.DataFiles[0].FileMeta.Type {
	case mydump.SourceTypeCSV:
		hasHeader := rc.cfg.Mydumper.CSV.Header
		// Create a utf8mb4 convertor to encode and decode data with the charset of CSV files.
		charsetConvertor, err := mydump.NewCharsetConvertor(rc.cfg.Mydumper.DataCharacterSet, rc.cfg.Mydumper.DataInvalidCharReplace)
		if err != nil {
			return errors.Trace(err)
		}
		parser, err = mydump.NewCSVParser(&rc.cfg.Mydumper.CSV, reader, blockBufSize, rc.ioWorkers, hasHeader, charsetConvertor)
		if err != nil {
			return errors.Trace(err)
		}
	case mydump.SourceTypeSQL:
		parser = mydump.NewChunkParser(rc.cfg.TiDB.SQLMode, reader, blockBufSize, rc.ioWorkers)
	case mydump.SourceTypeParquet:
		parser, err = mydump.NewParquetParser(ctx, rc.store, reader, sampleFile.Path)
		if err != nil {
			return errors.Trace(err)
		}
	default:
		panic(fmt.Sprintf("file '%s' with unknown source type '%s'", sampleFile.Path, sampleFile.Type.String()))
	}
	defer parser.Close()
	logTask := log.With(zap.String("table", tableMeta.Name)).Begin(zap.InfoLevel, "sample file")
	igCols, err := rc.cfg.Mydumper.IgnoreColumns.GetIgnoreColumns(dbName, tableMeta.Name, rc.cfg.Mydumper.CaseSensitive)
	if err != nil {
		return errors.Trace(err)
	}

	initializedColumns := false
	var columnPermutation []int
	var kvSize uint64 = 0
	var rowSize uint64 = 0
	rowCount := 0
	dataKVs := rc.backend.MakeEmptyRows()
	indexKVs := rc.backend.MakeEmptyRows()
	lastKey := make([]byte, 0)
	tableMeta.IsRowOrdered = true
	tableMeta.IndexRatio = 1.0
outloop:
	for {
		offset, _ := parser.Pos()
		err = parser.ReadRow()
		columnNames := parser.Columns()

		switch errors.Cause(err) {
		case nil:
			if !initializedColumns {
				if len(columnPermutation) == 0 {
					columnPermutation, err = createColumnPermutation(columnNames, igCols.ColumnsMap(), tableInfo)
					if err != nil {
						return errors.Trace(err)
					}
				}
				initializedColumns = true
			}
		case io.EOF:
			break outloop
		default:
			err = errors.Annotatef(err, "in file offset %d", offset)
			return errors.Trace(err)
		}
		lastRow := parser.LastRow()
		rowCount += 1

		var dataChecksum, indexChecksum verification.KVChecksum
		kvs, encodeErr := kvEncoder.Encode(logTask.Logger, lastRow.Row, lastRow.RowID, columnPermutation, sampleFile.Path, offset)
		if encodeErr != nil {
			encodeErr = errMgr.RecordTypeError(ctx, log.L(), tableInfo.Name.O, sampleFile.Path, offset,
				"" /* use a empty string here because we don't actually record */, encodeErr)
			if encodeErr != nil {
				return errors.Annotatef(encodeErr, "in file at offset %d", offset)
			}
			if rowCount < maxSampleRowCount {
				continue
			} else {
				break
			}
		}
		if tableMeta.IsRowOrdered {
			kvs.ClassifyAndAppend(&dataKVs, &dataChecksum, &indexKVs, &indexChecksum)
			for _, kv := range kv.KvPairsFromRows(dataKVs) {
				if len(lastKey) == 0 {
					lastKey = kv.Key
				} else if bytes.Compare(lastKey, kv.Key) > 0 {
					tableMeta.IsRowOrdered = false
					break
				}
			}
			dataKVs = dataKVs.Clear()
			indexKVs = indexKVs.Clear()
		}
		kvSize += kvs.Size()
		rowSize += uint64(lastRow.Length)
		parser.RecycleRow(lastRow)

		failpoint.Inject("mock-kv-size", func(val failpoint.Value) {
			kvSize += uint64(val.(int))
		})
		if rowSize > maxSampleDataSize || rowCount > maxSampleRowCount {
			break
		}
	}

	if rowSize > 0 && kvSize > rowSize {
		tableMeta.IndexRatio = float64(kvSize) / float64(rowSize)
	}
	log.L().Info("Sample source data", zap.String("table", tableMeta.Name), zap.Float64("IndexRatio", tableMeta.IndexRatio), zap.Bool("IsSourceOrder", tableMeta.IsRowOrdered))
	return nil
}

func (rc *Controller) checkTableEmpty(ctx context.Context) error {
	if rc.cfg.TikvImporter.Backend == config.BackendTiDB || rc.cfg.TikvImporter.IncrementalImport {
		return nil
	}
	db, _ := rc.tidbGlue.GetDB()

	tableCount := 0
	for _, db := range rc.dbMetas {
		tableCount += len(db.Tables)
	}

	var lock sync.Mutex
	tableNames := make([]string, 0)
	concurrency := utils.MinInt(tableCount, rc.cfg.App.RegionConcurrency)
	ch := make(chan string, concurrency)
	eg, gCtx := errgroup.WithContext(ctx)

	for i := 0; i < concurrency; i++ {
		eg.Go(func() error {
			for tblName := range ch {
				// skip tables that have checkpoint
				if rc.cfg.Checkpoint.Enable {
					_, err := rc.checkpointsDB.Get(gCtx, tblName)
					switch {
					case err == nil:
						continue
					case errors.IsNotFound(err):
					default:
						return errors.Trace(err)
					}
				}

				hasData, err1 := tableContainsData(gCtx, db, tblName)
				if err1 != nil {
					return err1
				}
				if hasData {
					lock.Lock()
					tableNames = append(tableNames, tblName)
					lock.Unlock()
				}
			}
			return nil
		})
	}
loop:
	for _, db := range rc.dbMetas {
		for _, tbl := range db.Tables {
			select {
			case ch <- common.UniqueTable(tbl.DB, tbl.Name):
			case <-gCtx.Done():
				break loop
			}

		}
	}
	close(ch)
	if err := eg.Wait(); err != nil {
		if common.IsContextCanceledError(err) {
			return nil
		}
		return errors.Annotate(err, "check table contains data failed")
	}

	if len(tableNames) > 0 {
		// sort the failed names
		sort.Strings(tableNames)
		msg := fmt.Sprintf("table(s) [%s] are not empty", strings.Join(tableNames, ", "))
		rc.checkTemplate.Collect(Critical, false, msg)
	}
	return nil
}

func tableContainsData(ctx context.Context, db utils.DBExecutor, tableName string) (bool, error) {
	failpoint.Inject("CheckTableEmptyFailed", func() {
		failpoint.Return(false, errors.New("mock error"))
	})
	query := "select 1 from " + tableName + " limit 1"
	exec := common.SQLWithRetry{
		DB:     db,
		Logger: log.L(),
	}
	var dump int
	err := exec.QueryRow(ctx, "check table empty", query, &dump)

	switch {
	case errors.ErrorEqual(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, errors.Trace(err)
	default:
		return true, nil
	}
}
