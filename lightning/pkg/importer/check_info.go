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

package importer

import (
	"context"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/lightning/pkg/precheck"
	"github.com/pingcap/tidb/pkg/lightning/config"
)

const (
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

func (rc *Controller) doPreCheckOnItem(ctx context.Context, checkItemID precheck.CheckItemID) error {
	theChecker, err := rc.precheckItemBuilder.BuildPrecheckItem(checkItemID)
	if err != nil {
		return errors.Trace(err)
	}
	result, err := theChecker.Check(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if result != nil {
		rc.checkTemplate.Collect(result.Severity, result.Passed, result.Message)
	}
	return nil
}

// clusterResource check cluster has enough resource to import data. this test can be skipped.
func (rc *Controller) clusterResource(ctx context.Context) error {
	checkCtx := ctx
	if rc.taskMgr != nil {
		checkCtx = WithPrecheckKey(ctx, taskManagerKey, rc.taskMgr)
	}
	return rc.doPreCheckOnItem(checkCtx, precheck.CheckTargetClusterSize)
}

// ClusterIsAvailable check cluster is available to import data. this test can be skipped.
func (rc *Controller) ClusterIsAvailable(ctx context.Context) error {
	return rc.doPreCheckOnItem(ctx, precheck.CheckTargetClusterVersion)
}

func (rc *Controller) checkEmptyRegion(ctx context.Context) error {
	return rc.doPreCheckOnItem(ctx, precheck.CheckTargetClusterEmptyRegion)
}

// checkRegionDistribution checks if regions distribution is unbalanced.
func (rc *Controller) checkRegionDistribution(ctx context.Context) error {
	return rc.doPreCheckOnItem(ctx, precheck.CheckTargetClusterRegionDist)
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
func (rc *Controller) StoragePermission(ctx context.Context) error {
	return rc.doPreCheckOnItem(ctx, precheck.CheckSourcePermission)
}

// HasLargeCSV checks whether input csvs is fit for Lightning import.
// If strictFormat is false, and csv file is large. Lightning will have performance issue.
// this test cannot be skipped.
func (rc *Controller) HasLargeCSV(ctx context.Context) error {
	return rc.doPreCheckOnItem(ctx, precheck.CheckLargeDataFile)
}

// localResource checks the local node has enough resources for this import when local backend enabled;
func (rc *Controller) localResource(ctx context.Context) error {
	if rc.isSourceInLocal() {
		if err := rc.doPreCheckOnItem(ctx, precheck.CheckLocalDiskPlacement); err != nil {
			return errors.Trace(err)
		}
	}

	return rc.doPreCheckOnItem(ctx, precheck.CheckLocalTempKVDir)
}

func (rc *Controller) checkCSVHeader(ctx context.Context) error {
	return rc.doPreCheckOnItem(ctx, precheck.CheckCSVHeader)
}

func (rc *Controller) checkTableEmpty(ctx context.Context) error {
	if rc.cfg.TikvImporter.Backend == config.BackendTiDB || rc.cfg.TikvImporter.ParallelImport {
		return nil
	}
	return rc.doPreCheckOnItem(ctx, precheck.CheckTargetTableEmpty)
}

func (rc *Controller) checkCheckpoints(ctx context.Context) error {
	if !rc.cfg.Checkpoint.Enable {
		return nil
	}
	return rc.doPreCheckOnItem(ctx, precheck.CheckCheckpoints)
}

func (rc *Controller) checkSourceSchema(ctx context.Context) error {
	if rc.cfg.TikvImporter.Backend == config.BackendTiDB {
		return nil
	}
	return rc.doPreCheckOnItem(ctx, precheck.CheckSourceSchemaValid)
}

func (rc *Controller) checkCDCPiTR(ctx context.Context) error {
	if rc.cfg.TikvImporter.Backend == config.BackendTiDB {
		return nil
	}
	return rc.doPreCheckOnItem(ctx, precheck.CheckTargetUsingCDCPITR)
}
