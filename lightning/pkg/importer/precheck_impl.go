// Copyright 2023 PingCAP, Inc.
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

package importer

import (
	"cmp"
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/lightning/pkg/precheck"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/cdcutil"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/pingcap/tidb/pkg/util/set"
	pdhttp "github.com/tikv/pd/client/http"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type clusterResourceCheckItem struct {
	preInfoGetter PreImportInfoGetter
}

// NewClusterResourceCheckItem creates a new clusterResourceCheckItem.
func NewClusterResourceCheckItem(preInfoGetter PreImportInfoGetter) precheck.Checker {
	return &clusterResourceCheckItem{
		preInfoGetter: preInfoGetter,
	}
}

// GetCheckItemID implements Checker.GetCheckItemID.
func (*clusterResourceCheckItem) GetCheckItemID() precheck.CheckItemID {
	return precheck.CheckTargetClusterSize
}

func (ci *clusterResourceCheckItem) getClusterAvail(ctx context.Context) (tikvAvail uint64, tiflashAvail uint64, err error) {
	storeInfo, err := ci.preInfoGetter.GetStorageInfo(ctx)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	for _, store := range storeInfo.Stores {
		avail, err := units.RAMInBytes(store.Status.Available)
		if err != nil {
			return 0, 0, errors.Trace(err)
		}
		if engine.IsTiFlashHTTPResp(&store.Store) {
			tiflashAvail += uint64(avail)
		} else {
			tikvAvail += uint64(avail)
		}
	}
	return
}

// Check implements Checker.Check.
func (ci *clusterResourceCheckItem) Check(ctx context.Context) (*precheck.CheckResult, error) {
	theResult := &precheck.CheckResult{
		Item:     ci.GetCheckItemID(),
		Severity: precheck.Warn,
		Passed:   true,
		Message:  "",
	}

	var (
		err               error
		tikvAvail         uint64
		tiflashAvail      uint64
		tikvSourceSize    uint64
		tiflashSourceSize uint64
		taskMgr           taskMetaMgr
	)
	taskMgrVal := ctx.Value(taskManagerKey)
	if taskMgrVal != nil {
		if mgr, ok := taskMgrVal.(taskMetaMgr); ok {
			taskMgr = mgr
		}
	}
	if taskMgr == nil {
		var err error
		estimatedDataSizeResult, err := ci.preInfoGetter.EstimateSourceDataSize(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tikvSourceSize = uint64(estimatedDataSizeResult.SizeWithIndex)
		tiflashSourceSize = uint64(estimatedDataSizeResult.TiFlashSize)
		tikvAvail, tiflashAvail, err = ci.getClusterAvail(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		if err := taskMgr.CheckTasksExclusively(ctx, func(tasks []taskMeta) ([]taskMeta, error) {
			tikvAvail = 0
			tiflashAvail = 0
			tikvSourceSize = 0
			tiflashSourceSize = 0
			restoreStarted := false
			for _, task := range tasks {
				if task.status > taskMetaStatusInitial {
					restoreStarted = true
				}
				tikvSourceSize += task.tikvSourceBytes
				tiflashSourceSize += task.tiflashSourceBytes
				if task.tikvAvail > 0 {
					tikvAvail = task.tikvAvail
				}
				if task.tiflashAvail > 0 {
					tiflashAvail = task.tiflashAvail
				}
			}
			if restoreStarted || tikvAvail > 0 || tiflashAvail > 0 {
				return nil, nil
			}

			tikvAvail, tiflashAvail, err = ci.getClusterAvail(ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			newTasks := append([]taskMeta(nil), tasks...)
			for i := 0; i < len(newTasks); i++ {
				newTasks[i].tikvAvail = tikvAvail
				newTasks[i].tiflashAvail = tiflashAvail
			}
			return newTasks, nil
		}); err != nil {
			return nil, errors.Trace(err)
		}
	}

	replicaCount, err := ci.preInfoGetter.GetMaxReplica(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tikvSourceSize = tikvSourceSize * replicaCount

	if tikvSourceSize <= tikvAvail && tiflashSourceSize <= tiflashAvail {
		theResult.Message = fmt.Sprintf("The storage space is rich, which TiKV/Tiflash is %s/%s. The estimated storage space is %s/%s.",
			units.BytesSize(float64(tikvAvail)), units.BytesSize(float64(tiflashAvail)), units.BytesSize(float64(tikvSourceSize)), units.BytesSize(float64(tiflashSourceSize)))
	}

	if tikvSourceSize > tikvAvail {
		theResult.Passed = false
		theResult.Message += fmt.Sprintf("TiKV requires more storage space. Estimated required size: %s. Actual size: %s.",
			units.BytesSize(float64(tikvSourceSize)), units.BytesSize(float64(tikvAvail)))
	}
	if tiflashAvail > 0 && tiflashSourceSize > tiflashAvail {
		theResult.Passed = false
		theResult.Message += fmt.Sprintf(" TiFlash requires more storage space. Estimated required size: %s. Actual size: %s.",
			units.BytesSize(float64(tiflashSourceSize)), units.BytesSize(float64(tiflashAvail)))
	}
	if !theResult.Passed {
		theResult.Message += " Please increase storage to prevent import task failures."
	}
	return theResult, nil
}

type clusterVersionCheckItem struct {
	preInfoGetter PreImportInfoGetter
	dbMetas       []*mydump.MDDatabaseMeta
}

// NewClusterVersionCheckItem creates a new clusterVersionCheckItem.
func NewClusterVersionCheckItem(preInfoGetter PreImportInfoGetter, dbMetas []*mydump.MDDatabaseMeta) precheck.Checker {
	return &clusterVersionCheckItem{
		preInfoGetter: preInfoGetter,
		dbMetas:       dbMetas,
	}
}

// GetCheckItemID implements Checker.GetCheckItemID.
func (*clusterVersionCheckItem) GetCheckItemID() precheck.CheckItemID {
	return precheck.CheckTargetClusterVersion
}

// Check implements Checker.Check.
func (ci *clusterVersionCheckItem) Check(ctx context.Context) (*precheck.CheckResult, error) {
	theResult := &precheck.CheckResult{
		Item:     ci.GetCheckItemID(),
		Severity: precheck.Critical,
		Passed:   true,
		Message:  "Cluster version check passed",
	}
	checkCtx := WithPreInfoGetterDBMetas(ctx, ci.dbMetas)
	if err := ci.preInfoGetter.CheckVersionRequirements(checkCtx); err != nil {
		err := common.NormalizeError(err)
		theResult.Passed = false
		theResult.Message = fmt.Sprintf("Cluster version check failed: %s", err.Error())
	}
	return theResult, nil
}

type emptyRegionCheckItem struct {
	preInfoGetter PreImportInfoGetter
	dbMetas       []*mydump.MDDatabaseMeta
}

// NewEmptyRegionCheckItem creates a new emptyRegionCheckItem.
func NewEmptyRegionCheckItem(preInfoGetter PreImportInfoGetter, dbMetas []*mydump.MDDatabaseMeta) precheck.Checker {
	return &emptyRegionCheckItem{
		preInfoGetter: preInfoGetter,
		dbMetas:       dbMetas,
	}
}

// GetCheckItemID implements Checker.GetCheckItemID.
func (*emptyRegionCheckItem) GetCheckItemID() precheck.CheckItemID {
	return precheck.CheckTargetClusterEmptyRegion
}

// Check implements Checker.Check.
func (ci *emptyRegionCheckItem) Check(ctx context.Context) (*precheck.CheckResult, error) {
	theResult := &precheck.CheckResult{
		Item:     ci.GetCheckItemID(),
		Severity: precheck.Warn,
		Passed:   true,
		Message:  "Cluster doesn't have too many empty regions",
	}
	dbInfos, err := ci.preInfoGetter.GetAllTableStructures(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	storeInfo, err := ci.preInfoGetter.GetStorageInfo(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(storeInfo.Stores) <= 1 {
		return theResult, nil
	}
	emptyRegionsInfo, err := ci.preInfoGetter.GetEmptyRegionsInfo(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	regions := make(map[int64]int)
	stores := make(map[int64]*pdhttp.StoreInfo)
	for _, region := range emptyRegionsInfo.Regions {
		for _, peer := range region.Peers {
			regions[peer.StoreID]++
		}
	}
	for _, store := range storeInfo.Stores {
		store := store
		stores[store.Store.ID] = &store
	}
	tableCount := 0
	for _, db := range ci.dbMetas {
		info, ok := dbInfos[db.Name]
		if !ok {
			continue
		}
		tableCount += len(info.Tables)
	}
	errorThrehold := max(errorEmptyRegionCntPerStore, tableCount*3)
	warnThrehold := max(warnEmptyRegionCntPerStore, tableCount)
	var (
		errStores  []string
		warnStores []string
	)
	for storeID, regionCnt := range regions {
		if store, ok := stores[storeID]; ok {
			if metapb.StoreState(metapb.StoreState_value[store.Store.StateName]) != metapb.StoreState_Up {
				continue
			}
			if engine.IsTiFlashHTTPResp(&store.Store) {
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
		theResult.Passed = false
		messages = append(messages, fmt.Sprintf("TiKV stores (%s) contains more than %v empty regions respectively, "+
			"which will greatly affect the import speed and success rate", strings.Join(errStores, ", "), errorEmptyRegionCntPerStore))
	}
	if len(warnStores) > 0 {
		messages = append(messages, fmt.Sprintf("TiKV stores (%s) contains more than %v empty regions respectively, "+
			"which will affect the import speed and success rate", strings.Join(warnStores, ", "), warnEmptyRegionCntPerStore))
	}
	if len(messages) > 0 {
		theResult.Message = strings.Join(messages, "\n")
	}
	return theResult, nil
}

type regionDistributionCheckItem struct {
	preInfoGetter PreImportInfoGetter
	dbMetas       []*mydump.MDDatabaseMeta
}

// NewRegionDistributionCheckItem creates a new regionDistributionCheckItem.
func NewRegionDistributionCheckItem(preInfoGetter PreImportInfoGetter, dbMetas []*mydump.MDDatabaseMeta) precheck.Checker {
	return &regionDistributionCheckItem{
		preInfoGetter: preInfoGetter,
		dbMetas:       dbMetas,
	}
}

// GetCheckItemID implements Checker.GetCheckItemID.
func (*regionDistributionCheckItem) GetCheckItemID() precheck.CheckItemID {
	return precheck.CheckTargetClusterRegionDist
}

// Check implements Checker.Check.
func (ci *regionDistributionCheckItem) Check(ctx context.Context) (*precheck.CheckResult, error) {
	theResult := &precheck.CheckResult{
		Item:     ci.GetCheckItemID(),
		Severity: precheck.Warn,
		Passed:   true,
		Message:  "Cluster region distribution is balanced",
	}

	storesInfo, err := ci.preInfoGetter.GetStorageInfo(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	stores := make([]*pdhttp.StoreInfo, 0, len(storesInfo.Stores))
	for _, store := range storesInfo.Stores {
		store := store
		if metapb.StoreState(metapb.StoreState_value[store.Store.StateName]) != metapb.StoreState_Up {
			continue
		}
		if engine.IsTiFlashHTTPResp(&store.Store) {
			continue
		}
		stores = append(stores, &store)
	}
	if len(stores) <= 1 {
		return theResult, nil
	}
	slices.SortFunc(stores, func(i, j *pdhttp.StoreInfo) int {
		return cmp.Compare(i.Status.RegionCount, j.Status.RegionCount)
	})
	minStore := stores[0]
	maxStore := stores[len(stores)-1]

	dbInfos, err := ci.preInfoGetter.GetAllTableStructures(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tableCount := 0
	for _, db := range ci.dbMetas {
		info, ok := dbInfos[db.Name]
		if !ok {
			continue
		}
		tableCount += len(info.Tables)
	}
	threhold := max(checkRegionCntRatioThreshold, tableCount)
	if maxStore.Status.RegionCount <= int64(threhold) {
		return theResult, nil
	}
	ratio := float64(minStore.Status.RegionCount) / float64(maxStore.Status.RegionCount)
	if ratio < errorRegionCntMinMaxRatio {
		theResult.Passed = false
		theResult.Message = fmt.Sprintf("Region distribution is unbalanced, the ratio of the regions count of the store(%v) "+
			"with least regions(%v) to the store(%v) with most regions(%v) is %v, but we expect it must not be less than %v",
			minStore.Store.ID, minStore.Status.RegionCount, maxStore.Store.ID, maxStore.Status.RegionCount, ratio, errorRegionCntMinMaxRatio)
	} else if ratio < warnRegionCntMinMaxRatio {
		theResult.Message = fmt.Sprintf("Region distribution is unbalanced, the ratio of the regions count of the store(%v) "+
			"with least regions(%v) to the store(%v) with most regions(%v) is %v, but we expect it should not be less than %v",
			minStore.Store.ID, minStore.Status.RegionCount, maxStore.Store.ID, maxStore.Status.RegionCount, ratio, warnRegionCntMinMaxRatio)
	}
	return theResult, nil
}

type storagePermissionCheckItem struct {
	cfg *config.Config
}

// NewStoragePermissionCheckItem creates a new storagePermissionCheckItem.
func NewStoragePermissionCheckItem(cfg *config.Config) precheck.Checker {
	return &storagePermissionCheckItem{
		cfg: cfg,
	}
}

// GetCheckItemID implements Checker.GetCheckItemID.
func (*storagePermissionCheckItem) GetCheckItemID() precheck.CheckItemID {
	return precheck.CheckSourcePermission
}

// Check implements Checker.Check.
func (ci *storagePermissionCheckItem) Check(ctx context.Context) (*precheck.CheckResult, error) {
	theResult := &precheck.CheckResult{
		Item:     ci.GetCheckItemID(),
		Severity: precheck.Critical,
		Passed:   true,
		Message:  "Lightning has the correct storage permission",
	}

	u, err := storage.ParseBackend(ci.cfg.Mydumper.SourceDir, nil)
	if err != nil {
		return nil, common.NormalizeError(err)
	}
	_, err = storage.New(ctx, u, &storage.ExternalStorageOptions{
		CheckPermissions: []storage.Permission{
			storage.ListObjects,
			storage.GetObject,
		},
	})
	if err != nil {
		theResult.Passed = false
		theResult.Message = err.Error()
	}
	return theResult, nil
}

type largeFileCheckItem struct {
	cfg     *config.Config
	dbMetas []*mydump.MDDatabaseMeta
}

// NewLargeFileCheckItem creates a new largeFileCheckItem.
func NewLargeFileCheckItem(cfg *config.Config, dbMetas []*mydump.MDDatabaseMeta) precheck.Checker {
	return &largeFileCheckItem{
		cfg:     cfg,
		dbMetas: dbMetas,
	}
}

// GetCheckItemID implements Checker.GetCheckItemID.
func (*largeFileCheckItem) GetCheckItemID() precheck.CheckItemID {
	return precheck.CheckLargeDataFile
}

// Check implements Checker.Check.
func (ci *largeFileCheckItem) Check(_ context.Context) (*precheck.CheckResult, error) {
	theResult := &precheck.CheckResult{
		Item:     ci.GetCheckItemID(),
		Severity: precheck.Warn,
		Passed:   true,
		Message:  "Source data files size is proper",
	}

	if !ci.cfg.Mydumper.StrictFormat {
		for _, db := range ci.dbMetas {
			for _, t := range db.Tables {
				for _, f := range t.DataFiles {
					if f.FileMeta.RealSize > defaultCSVSize {
						theResult.Message = fmt.Sprintf("large data file: %s file exists and it will slow down import performance", f.FileMeta.Path)
						theResult.Passed = false
					}
				}
			}
		}
	} else {
		theResult.Message = "Skip the data file size check, because config.StrictFormat is true"
	}
	return theResult, nil
}

type localDiskPlacementCheckItem struct {
	cfg *config.Config
}

// NewLocalDiskPlacementCheckItem creates a new localDiskPlacementCheckItem.
func NewLocalDiskPlacementCheckItem(cfg *config.Config) precheck.Checker {
	return &localDiskPlacementCheckItem{
		cfg: cfg,
	}
}

// GetCheckItemID implements Checker.GetCheckItemID.
func (*localDiskPlacementCheckItem) GetCheckItemID() precheck.CheckItemID {
	return precheck.CheckLocalDiskPlacement
}

// Check implements Checker.Check.
func (ci *localDiskPlacementCheckItem) Check(_ context.Context) (*precheck.CheckResult, error) {
	theResult := &precheck.CheckResult{
		Item:     ci.GetCheckItemID(),
		Severity: precheck.Warn,
		Passed:   true,
		Message:  "local source dir and temp-kv dir are in different disks",
	}
	sourceDir := strings.TrimPrefix(ci.cfg.Mydumper.SourceDir, storage.LocalURIPrefix)
	same, err := common.SameDisk(sourceDir, ci.cfg.TikvImporter.SortedKVDir)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if same {
		theResult.Passed = false
		theResult.Message = fmt.Sprintf("sorted-kv-dir:%s and data-source-dir:%s are in the same disk, may slow down performance",
			ci.cfg.TikvImporter.SortedKVDir, sourceDir)
	}
	return theResult, nil
}

type localTempKVDirCheckItem struct {
	cfg           *config.Config
	preInfoGetter PreImportInfoGetter
	dbMetas       []*mydump.MDDatabaseMeta
}

// NewLocalTempKVDirCheckItem creates a new localTempKVDirCheckItem.
func NewLocalTempKVDirCheckItem(cfg *config.Config, preInfoGetter PreImportInfoGetter, dbMetas []*mydump.MDDatabaseMeta) precheck.Checker {
	return &localTempKVDirCheckItem{
		cfg:           cfg,
		preInfoGetter: preInfoGetter,
		dbMetas:       dbMetas,
	}
}

// GetCheckItemID implements Checker.GetCheckItemID.
func (*localTempKVDirCheckItem) GetCheckItemID() precheck.CheckItemID {
	return precheck.CheckLocalTempKVDir
}

func (ci *localTempKVDirCheckItem) hasCompressedFiles() bool {
	for _, dbMeta := range ci.dbMetas {
		for _, tbMeta := range dbMeta.Tables {
			for _, file := range tbMeta.DataFiles {
				if file.FileMeta.Compression != mydump.CompressionNone {
					return true
				}
			}
		}
	}
	return false
}

// Check implements Checker.Check.
func (ci *localTempKVDirCheckItem) Check(ctx context.Context) (*precheck.CheckResult, error) {
	severity := precheck.Critical
	// for cases that have compressed files, the estimated size may not be accurate, set severity to Warn to avoid failure
	if ci.hasCompressedFiles() {
		severity = precheck.Warn
	}
	theResult := &precheck.CheckResult{
		Item:     ci.GetCheckItemID(),
		Severity: severity,
	}
	storageSize, err := common.GetStorageSize(ci.cfg.TikvImporter.SortedKVDir)
	if err != nil {
		return nil, errors.Trace(err)
	}
	localAvailable := int64(storageSize.Available)
	estimatedDataSizeResult, err := ci.preInfoGetter.EstimateSourceDataSize(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	estimatedDataSizeWithIndex := estimatedDataSizeResult.SizeWithIndex

	switch {
	case localAvailable > estimatedDataSizeWithIndex:
		theResult.Message = fmt.Sprintf("local disk resources are rich, estimate sorted data size %s, local available is %s",
			units.BytesSize(float64(estimatedDataSizeWithIndex)), units.BytesSize(float64(localAvailable)))
		theResult.Passed = true
	case int64(ci.cfg.TikvImporter.DiskQuota) > localAvailable:
		theResult.Message = fmt.Sprintf("local disk space may not enough to finish import, estimate sorted data size is %s,"+
			" but local available is %s, please set `tikv-importer.disk-quota` to a smaller value than %s"+
			" or change `mydumper.sorted-kv-dir` to another disk with enough space to finish imports",
			units.BytesSize(float64(estimatedDataSizeWithIndex)),
			units.BytesSize(float64(localAvailable)), units.BytesSize(float64(localAvailable)))
		theResult.Passed = false
		log.FromContext(ctx).Error(theResult.Message)
	default:
		theResult.Message = fmt.Sprintf("local disk space may not enough to finish import, "+
			"estimate sorted data size is %s, but local available is %s,"+
			"we will use disk-quota (size: %s) to finish imports, which may slow down import",
			units.BytesSize(float64(estimatedDataSizeWithIndex)),
			units.BytesSize(float64(localAvailable)), units.BytesSize(float64(ci.cfg.TikvImporter.DiskQuota)))
		theResult.Passed = true
		log.FromContext(ctx).Warn(theResult.Message)
	}
	return theResult, nil
}

type checkpointCheckItem struct {
	cfg           *config.Config
	preInfoGetter PreImportInfoGetter
	dbMetas       []*mydump.MDDatabaseMeta
	checkpointsDB checkpoints.DB
}

// NewCheckpointCheckItem creates a new checkpointCheckItem.
func NewCheckpointCheckItem(cfg *config.Config, preInfoGetter PreImportInfoGetter, dbMetas []*mydump.MDDatabaseMeta, checkpointsDB checkpoints.DB) precheck.Checker {
	return &checkpointCheckItem{
		cfg:           cfg,
		preInfoGetter: preInfoGetter,
		dbMetas:       dbMetas,
		checkpointsDB: checkpointsDB,
	}
}

// GetCheckItemID implements Checker.GetCheckItemID.
func (*checkpointCheckItem) GetCheckItemID() precheck.CheckItemID {
	return precheck.CheckCheckpoints
}

// Check implements Checker.Check.
func (ci *checkpointCheckItem) Check(ctx context.Context) (*precheck.CheckResult, error) {
	if !ci.cfg.Checkpoint.Enable || ci.checkpointsDB == nil {
		return nil, nil
	}
	theResult := &precheck.CheckResult{
		Item:     ci.GetCheckItemID(),
		Severity: precheck.Critical,
		Passed:   true,
		Message:  "the checkpoints are valid",
	}

	checkMsgs := []string{}
	dbInfos, err := ci.preInfoGetter.GetAllTableStructures(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, dbInfo := range ci.dbMetas {
		for _, tableInfo := range dbInfo.Tables {
			msgs, err := ci.checkpointIsValid(ctx, tableInfo, dbInfos)
			if err != nil {
				return nil, errors.Trace(err)
			}
			checkMsgs = append(checkMsgs, msgs...)
		}
	}
	if len(checkMsgs) > 0 {
		theResult.Passed = false
		theResult.Message = strings.Join(checkMsgs, "\n")
	}
	return theResult, nil
}

// checkpointIsValid checks whether we can start this import with this checkpoint.
func (ci *checkpointCheckItem) checkpointIsValid(ctx context.Context, tableInfo *mydump.MDTableMeta, dbInfos map[string]*checkpoints.TidbDBInfo) ([]string, error) {
	msgs := make([]string, 0)
	uniqueName := common.UniqueTable(tableInfo.DB, tableInfo.Name)
	tableCheckPoint, err := ci.checkpointsDB.Get(ctx, uniqueName)
	if err != nil {
		if errors.IsNotFound(err) {
			// there is no checkpoint
			log.FromContext(ctx).Debug("no checkpoint detected", zap.String("table", uniqueName))
			return nil, nil
		}
		return nil, errors.Trace(err)
	}
	// if checkpoint enable and not missing, we skip the check table empty progress.
	if tableCheckPoint.Status <= checkpoints.CheckpointStatusMissing {
		return nil, nil
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
		return msgs, nil
	}

	dbInfo, ok := dbInfos[tableInfo.DB]
	if ok {
		t, ok := dbInfo.Tables[tableInfo.Name]
		if ok {
			if tableCheckPoint.TableID > 0 && tableCheckPoint.TableID != t.ID {
				msgs = append(msgs, fmt.Sprintf("TiDB Lightning has detected tables with illegal checkpoints. To prevent data loss, this run will stop now,"+
					"please run command \"./tidb-lightning-ctl --checkpoint-remove='%s' --config=...\""+
					"You may also run `./tidb-lightning-ctl --checkpoint-error-destroy=all --config=...` to start from scratch,"+
					"For details of this failure, read the log file from the PREVIOUS run",
					uniqueName))
				return msgs, nil
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
			if filepath.Dir(chunk.FileMeta.Path) != ci.cfg.Mydumper.SourceDir {
				message := fmt.Sprintf("chunk checkpoints path is not equal to config"+
					"checkpoint is %s, config source dir is %s", chunk.FileMeta.Path, ci.cfg.Mydumper.SourceDir)
				msgs = append(msgs, message)
			}
		}
	}
	if len(columns) == 0 {
		log.FromContext(ctx).Debug("no valid checkpoint detected", zap.String("table", uniqueName))
		return nil, nil
	}
	info := dbInfos[tableInfo.DB].Tables[tableInfo.Name]
	if info != nil {
		permFromTiDB, err := parseColumnPermutations(info.Core, columns, nil, log.FromContext(ctx))
		if err != nil {
			msgs = append(msgs, fmt.Sprintf("failed to calculate columns %s, table %s's info has changed,"+
				"consider remove this checkpoint, and start import again.", err.Error(), uniqueName))
		}
		if !reflect.DeepEqual(permFromCheckpoint, permFromTiDB) {
			msgs = append(msgs, fmt.Sprintf("compare columns perm failed. table %s's info has changed,"+
				"consider remove this checkpoint, and start import again.", uniqueName))
		}
	}
	return msgs, nil
}

// CDCPITRCheckItem check downstream has enabled CDC or PiTR. It's exposed to let
// caller override the Instruction message.
type CDCPITRCheckItem struct {
	cfg           *config.Config
	Instruction   string
	pdAddrsGetter func(context.Context) []string
	// used in test
	etcdCli *clientv3.Client
}

// NewCDCPITRCheckItem creates a checker to check downstream has enabled CDC or PiTR.
func NewCDCPITRCheckItem(cfg *config.Config, pdAddrsGetter func(context.Context) []string) precheck.Checker {
	return &CDCPITRCheckItem{
		cfg:           cfg,
		Instruction:   "local backend is not compatible with them. Please switch to tidb backend then try again.",
		pdAddrsGetter: pdAddrsGetter,
	}
}

// GetCheckItemID implements Checker interface.
func (*CDCPITRCheckItem) GetCheckItemID() precheck.CheckItemID {
	return precheck.CheckTargetUsingCDCPITR
}

func dialEtcdWithCfg(
	ctx context.Context,
	cfg *config.Config,
	addrs []string,
) (*clientv3.Client, error) {
	cfg2, err := cfg.ToTLS()
	if err != nil {
		return nil, err
	}
	tlsConfig := cfg2.TLSConfig()

	return clientv3.New(clientv3.Config{
		TLS:              tlsConfig,
		Endpoints:        addrs,
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			config.DefaultGrpcKeepaliveParams,
			grpc.WithBlock(),
			grpc.WithReturnConnectionError(),
		},
		Context: ctx,
	})
}

// Check implements Checker interface.
func (ci *CDCPITRCheckItem) Check(ctx context.Context) (*precheck.CheckResult, error) {
	theResult := &precheck.CheckResult{
		Item:     ci.GetCheckItemID(),
		Severity: precheck.Critical,
	}

	if ci.cfg.TikvImporter.Backend != config.BackendLocal {
		theResult.Passed = true
		theResult.Message = "TiDB Lightning is not using local backend, skip this check"
		return theResult, nil
	}

	if ci.etcdCli == nil {
		var err error
		ci.etcdCli, err = dialEtcdWithCfg(ctx, ci.cfg, ci.pdAddrsGetter(ctx))
		if err != nil {
			return nil, errors.Trace(err)
		}
		//nolint: errcheck
		defer ci.etcdCli.Close()
	}

	errorMsg := make([]string, 0, 2)

	pitrCli := streamhelper.NewMetaDataClient(ci.etcdCli)
	tasks, err := pitrCli.GetAllTasks(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(tasks) > 0 {
		names := make([]string, 0, len(tasks))
		for _, task := range tasks {
			names = append(names, task.Info.GetName())
		}
		errorMsg = append(errorMsg, fmt.Sprintf("found PiTR log streaming task(s): %v,", names))
	}

	nameSet, err := cdcutil.GetRunningChangefeeds(ctx, ci.etcdCli)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if !nameSet.Empty() {
		errorMsg = append(errorMsg, nameSet.MessageToUser())
	}

	if len(errorMsg) > 0 {
		errorMsg = append(errorMsg, ci.Instruction)
		theResult.Passed = false
		theResult.Message = strings.Join(errorMsg, "\n")
	} else {
		theResult.Passed = true
		theResult.Message = "no CDC or PiTR task found"
	}

	return theResult, nil
}

type onlyState struct {
	State string `json:"state"`
}

type schemaCheckItem struct {
	cfg           *config.Config
	preInfoGetter PreImportInfoGetter
	dbMetas       []*mydump.MDDatabaseMeta
	checkpointsDB checkpoints.DB
}

// NewSchemaCheckItem creates a checker to check whether the schema is valid.
func NewSchemaCheckItem(cfg *config.Config, preInfoGetter PreImportInfoGetter, dbMetas []*mydump.MDDatabaseMeta, cpdb checkpoints.DB) precheck.Checker {
	return &schemaCheckItem{
		cfg:           cfg,
		preInfoGetter: preInfoGetter,
		dbMetas:       dbMetas,
		checkpointsDB: cpdb,
	}
}

// GetCheckItemID implements Checker interface.
func (*schemaCheckItem) GetCheckItemID() precheck.CheckItemID {
	return precheck.CheckSourceSchemaValid
}

// Check implements Checker interface.
func (ci *schemaCheckItem) Check(ctx context.Context) (*precheck.CheckResult, error) {
	theResult := &precheck.CheckResult{
		Item:     ci.GetCheckItemID(),
		Severity: precheck.Critical,
		Passed:   true,
		Message:  "table schemas are valid",
	}

	dbInfos, err := ci.preInfoGetter.GetAllTableStructures(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	checkMsgs := []string{}
	for _, dbInfo := range ci.dbMetas {
		for _, tableInfo := range dbInfo.Tables {
			if ci.cfg.Checkpoint.Enable && ci.checkpointsDB != nil {
				uniqueName := common.UniqueTable(tableInfo.DB, tableInfo.Name)
				if _, err := ci.checkpointsDB.Get(ctx, uniqueName); err == nil {
					// there is a checkpoint
					log.L().Debug("checkpoint detected, skip the schema check", zap.String("table", uniqueName))
					continue
				}
			}
			msgs, err := ci.SchemaIsValid(ctx, tableInfo, dbInfos)
			if err != nil {
				return nil, errors.Trace(err)
			}
			checkMsgs = append(checkMsgs, msgs...)
		}
	}
	if len(checkMsgs) > 0 {
		theResult.Passed = false
		theResult.Message = strings.Join(checkMsgs, "\n")
	}
	return theResult, nil
}

// SchemaIsValid checks the import file and cluster schema is match.
func (ci *schemaCheckItem) SchemaIsValid(ctx context.Context, tableInfo *mydump.MDTableMeta, dbInfos map[string]*checkpoints.TidbDBInfo) ([]string, error) {
	if len(tableInfo.DataFiles) == 0 {
		log.FromContext(ctx).Info("no data files detected", zap.String("db", tableInfo.DB), zap.String("table", tableInfo.Name))
		return nil, nil
	}

	msgs := make([]string, 0)

	info, ok := dbInfos[tableInfo.DB].Tables[tableInfo.Name]
	if !ok {
		msgs = append(msgs, fmt.Sprintf("TiDB schema `%s`.`%s` doesn't exists,"+
			"please give a schema file in source dir or create table manually", tableInfo.DB, tableInfo.Name))
		return msgs, nil
	}

	igCol, err := ci.cfg.Mydumper.IgnoreColumns.GetIgnoreColumns(tableInfo.DB, tableInfo.Name, ci.cfg.Mydumper.CaseSensitive)
	if err != nil {
		return nil, errors.Trace(err)
	}
	igCols := igCol.ColumnsMap()

	fullExtendColsSet := make(set.StringSet)
	for _, fileInfo := range tableInfo.DataFiles {
		for _, col := range fileInfo.FileMeta.ExtendData.Columns {
			if _, ok = igCols[col]; ok {
				msgs = append(msgs, fmt.Sprintf("extend column %s is also assigned in ignore-column for table `%s`.`%s`, "+
					"please keep only either one of them", col, tableInfo.DB, tableInfo.Name))
			}
			fullExtendColsSet.Insert(col)
		}
	}
	if len(msgs) > 0 {
		return msgs, nil
	}

	colCountFromTiDB := len(info.Core.Columns)
	if len(fullExtendColsSet) > 0 {
		log.FromContext(ctx).Info("check extend column count through data files", zap.String("db", tableInfo.DB),
			zap.String("table", tableInfo.Name))
		igColCnt := 0
		for _, col := range info.Core.Columns {
			if _, ok = igCols[col.Name.L]; ok {
				igColCnt++
			}
		}
		for _, f := range tableInfo.DataFiles {
			cols, previewRows, err := ci.preInfoGetter.ReadFirstNRowsByFileMeta(ctx, f.FileMeta, 1)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(cols) > 0 {
				colsSet := set.NewStringSet(cols...)
				for _, extendCol := range f.FileMeta.ExtendData.Columns {
					if colsSet.Exist(strings.ToLower(extendCol)) {
						msgs = append(msgs, fmt.Sprintf("extend column %s is contained in table `%s`.`%s`'s header, "+
							"please remove this column in data or remove this extend rule", extendCol, tableInfo.DB, tableInfo.Name))
					}
				}
			} else if len(previewRows) > 0 && len(previewRows[0])+len(f.FileMeta.ExtendData.Columns) > colCountFromTiDB+igColCnt {
				msgs = append(msgs, fmt.Sprintf("row count %d adding with extend column length %d is larger than columnCount %d plus ignore column count %d for table `%s`.`%s`, "+
					"please make sure your source data don't have extend columns and target schema has all of them", len(previewRows[0]), len(f.FileMeta.ExtendData.Columns), colCountFromTiDB, igColCnt, tableInfo.DB, tableInfo.Name))
			}
		}
	}
	if len(msgs) > 0 {
		return msgs, nil
	}

	core := info.Core
	defaultCols := make(map[string]struct{})
	autoRandomCol := common.GetAutoRandomColumn(core)
	for _, col := range core.Columns {
		// we can extend column the same with columns with default values
		if _, isExtendCol := fullExtendColsSet[col.Name.O]; isExtendCol || hasDefault(col) || (autoRandomCol != nil && autoRandomCol.ID == col.ID) {
			// this column has default value or it's auto random id, so we can ignore it
			defaultCols[col.Name.L] = struct{}{}
		}
		delete(fullExtendColsSet, col.Name.O)
	}
	if len(fullExtendColsSet) > 0 {
		extendCols := make([]string, 0, len(fullExtendColsSet))
		for col := range fullExtendColsSet {
			extendCols = append(extendCols, col)
		}
		msgs = append(msgs, fmt.Sprintf("extend column [%s] don't exist in target table `%s`.`%s` schema, "+
			"please add these extend columns manually in downstream database/schema file", strings.Join(extendCols, ","), tableInfo.DB, tableInfo.Name))
		return msgs, nil
	}

	// tidb_rowid have a default value.
	defaultCols[model.ExtraHandleName.String()] = struct{}{}

	// only check the first file of this table.
	dataFile := tableInfo.DataFiles[0]
	log.FromContext(ctx).Info("datafile to check", zap.String("db", tableInfo.DB),
		zap.String("table", tableInfo.Name), zap.String("path", dataFile.FileMeta.Path))
	// get columns name from data file.
	dataFileMeta := dataFile.FileMeta

	if tp := dataFileMeta.Type; tp != mydump.SourceTypeCSV && tp != mydump.SourceTypeSQL && tp != mydump.SourceTypeParquet {
		msgs = append(msgs, fmt.Sprintf("file '%s' with unknown source type '%s'", dataFileMeta.Path, dataFileMeta.Type.String()))
		return msgs, nil
	}
	row := []types.Datum{}
	colsFromDataFile, rows, err := ci.preInfoGetter.ReadFirstNRowsByFileMeta(ctx, dataFileMeta, 1)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(rows) > 0 {
		row = rows[0]
	}
	if colsFromDataFile == nil && len(row) == 0 {
		log.FromContext(ctx).Info("file contains no data, skip checking against schema validity", zap.String("path", dataFileMeta.Path))
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
			if dataFileMeta.Type == mydump.SourceTypeCSV && ci.cfg.Mydumper.CSV.Header {
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

type csvHeaderCheckItem struct {
	cfg           *config.Config
	preInfoGetter PreImportInfoGetter
	dbMetas       []*mydump.MDDatabaseMeta
}

// NewCSVHeaderCheckItem creates a new csvHeaderCheckItem.
func NewCSVHeaderCheckItem(cfg *config.Config, preInfoGetter PreImportInfoGetter, dbMetas []*mydump.MDDatabaseMeta) precheck.Checker {
	return &csvHeaderCheckItem{
		cfg:           cfg,
		preInfoGetter: preInfoGetter,
		dbMetas:       dbMetas,
	}
}

// GetCheckItemID implements Checker interface.
func (*csvHeaderCheckItem) GetCheckItemID() precheck.CheckItemID {
	return precheck.CheckCSVHeader
}

// Check tries to check whether the csv header config is consistent with the source csv files by:
//  1. pick one table with two CSV files and a unique/primary key
//  2. read the first row of those two CSV files
//  3. checks if the content of those first rows are compatible with the table schema, and whether the
//     two rows are identical, to determine if the first rows are a header rows.
func (ci *csvHeaderCheckItem) Check(ctx context.Context) (*precheck.CheckResult, error) {
	// if cfg set header = true but source files actually contain not header, former SchemaCheck should
	// return error in this situation, so we need do it again.
	if ci.cfg.Mydumper.CSV.Header {
		return nil, nil
	}
	theResult := &precheck.CheckResult{
		Item:     ci.GetCheckItemID(),
		Severity: precheck.Critical,
		Passed:   true,
		Message:  "the config [mydumper.csv.header] is set to false, and CSV header lines are really not detected in the data files",
	}
	var (
		tableMeta    *mydump.MDTableMeta
		csvCount     int
		hasUniqueIdx bool
	)
	dbInfos, err := ci.preInfoGetter.GetAllTableStructures(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// only check one table source files for better performance. The checked table is chosen based on following two factor:
	// 1. contains at least 1 csv source file, 2 is preferable
	// 2. table schema contains primary key or unique key
	// if the two factors can't be both satisfied, the first one has a higher priority
outer:
	for _, dbMeta := range ci.dbMetas {
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

			info := dbInfos[tblMeta.DB].Tables[tblMeta.Name]
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
		return theResult, nil
	}

	var rows [][]types.Datum
	for _, f := range tableMeta.DataFiles {
		if f.FileMeta.Type != mydump.SourceTypeCSV {
			continue
		}

		row := []types.Datum{}
		_, previewRows, err := ci.preInfoGetter.ReadFirstNRowsByFileMeta(ctx, f.FileMeta, 1)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(previewRows) > 0 {
			row = previewRows[0]
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
		return theResult, nil
	} else if len(rows) >= 2 {
		// if the first row in two source files are not the same, they should not be the header line
		// NOTE: though lightning's logic allows different source files contains different columns or the
		// order is difference, here we only check if they are exactly the same because this is the common case.
		if len(rows[0]) != len(rows[1]) {
			return theResult, nil
		}

		for i := 0; i < len(rows[0]); i++ {
			if rows[0][i].GetString() != rows[1][i].GetString() {
				return theResult, nil
			}
		}
	}

	// check if some fields are unique and not ignored
	// if at least one field appears in a unique key, we can sure there is something wrong,
	// they should be either the header line or the data is duplicated.
	tableInfo := dbInfos[tableMeta.DB].Tables[tableMeta.Name]
	tableFields := make(map[string]struct{})
	uniqueIdxFields := make(map[string]struct{})
	ignoreColumns, err := ci.cfg.Mydumper.IgnoreColumns.GetIgnoreColumns(tableMeta.DB, tableMeta.Name, ci.cfg.Mydumper.CaseSensitive)
	if err != nil {
		return nil, errors.Trace(err)
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
			return theResult, nil
		}
		if _, ok := uniqueIdxFields[val]; ok {
			hasUniqueField = true
			break
		}
	}

	theResult.Passed = false
	theResult.Message = fmt.Sprintf("source csv files contains header row but `mydumper.csv.header` is false, checked table is `%s`.`%s`",
		tableMeta.DB, tableMeta.Name)
	theResult.Severity = precheck.Warn
	if hasUniqueField && len(rows) > 1 {
		theResult.Severity = precheck.Critical
	} else if !checkFieldCompatibility(tableInfo.Core, ignoreColsSet, rows[0], log.FromContext(ctx)) {
		// if there are only 1 csv file or there is not unique key, try to check if all columns are compatible with string value
		theResult.Severity = precheck.Critical
	}
	return theResult, nil
}

func checkFieldCompatibility(
	tbl *model.TableInfo,
	ignoreCols map[string]struct{},
	values []types.Datum,
	logger log.Logger,
) bool {
	se := kv.NewSessionCtx(&encode.SessionOptions{
		SQLMode: mysql.ModeStrictTransTables,
	}, logger)
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
			logger.Error("field value is not consistent with column type", zap.String("value", values[i].GetString()),
				zap.Any("column_info", col), zap.Error(err))
			return false
		}
	}

	return true
}

type tableEmptyCheckItem struct {
	cfg           *config.Config
	preInfoGetter PreImportInfoGetter
	dbMetas       []*mydump.MDDatabaseMeta
	checkpointsDB checkpoints.DB
}

// NewTableEmptyCheckItem creates a new tableEmptyCheckItem
func NewTableEmptyCheckItem(cfg *config.Config, preInfoGetter PreImportInfoGetter, dbMetas []*mydump.MDDatabaseMeta, cpdb checkpoints.DB) precheck.Checker {
	return &tableEmptyCheckItem{
		cfg:           cfg,
		preInfoGetter: preInfoGetter,
		dbMetas:       dbMetas,
		checkpointsDB: cpdb,
	}
}

// GetCheckItemID implements Checker interface
func (*tableEmptyCheckItem) GetCheckItemID() precheck.CheckItemID {
	return precheck.CheckTargetTableEmpty
}

// Check implements Checker interface
func (ci *tableEmptyCheckItem) Check(ctx context.Context) (*precheck.CheckResult, error) {
	theResult := &precheck.CheckResult{
		Item:     ci.GetCheckItemID(),
		Severity: precheck.Critical,
		Passed:   true,
		Message:  "all importing tables on the target are empty",
	}

	tableCount := 0
	for _, db := range ci.dbMetas {
		tableCount += len(db.Tables)
	}

	var lock sync.Mutex
	tableNames := make([]string, 0)
	concurrency := min(tableCount, ci.cfg.App.RegionConcurrency)
	type tableNameComponents struct {
		DBName    string
		TableName string
	}
	ch := make(chan tableNameComponents, concurrency)
	eg, gCtx := errgroup.WithContext(ctx)

	for i := 0; i < concurrency; i++ {
		eg.Go(func() error {
			for tblNameComp := range ch {
				fullTableName := common.UniqueTable(tblNameComp.DBName, tblNameComp.TableName)
				// skip tables that have checkpoint
				if ci.cfg.Checkpoint.Enable && ci.checkpointsDB != nil {
					_, err := ci.checkpointsDB.Get(gCtx, fullTableName)
					switch {
					case err == nil:
						continue
					case errors.IsNotFound(err):
					default:
						return errors.Trace(err)
					}
				}

				isEmptyPtr, err1 := ci.preInfoGetter.IsTableEmpty(gCtx, tblNameComp.DBName, tblNameComp.TableName)
				if err1 != nil {
					return err1
				}
				if !(*isEmptyPtr) {
					lock.Lock()
					tableNames = append(tableNames, fullTableName)
					lock.Unlock()
				}
			}
			return nil
		})
	}
loop:
	for _, db := range ci.dbMetas {
		for _, tbl := range db.Tables {
			select {
			case ch <- tableNameComponents{tbl.DB, tbl.Name}:
			case <-gCtx.Done():
				break loop
			}
		}
	}
	close(ch)
	if err := eg.Wait(); err != nil {
		if common.IsContextCanceledError(err) {
			return nil, nil
		}
		return nil, errors.Annotate(err, "check table contains data failed")
	}

	if len(tableNames) > 0 {
		// sort the failed names
		slices.Sort(tableNames)
		theResult.Passed = false
		theResult.Message = fmt.Sprintf("table(s) [%s] are not empty", strings.Join(tableNames, ", "))
	}
	return theResult, nil
}

// hasDefault represents col has default value.
func hasDefault(col *model.ColumnInfo) bool {
	return col.DefaultIsExpr || col.DefaultValue != nil || !mysql.HasNotNullFlag(col.GetFlag()) ||
		col.IsGenerated() || mysql.HasAutoIncrementFlag(col.GetFlag())
}
