// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"context"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/ddl/serverstate"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/ddl/testargsv1"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)
// detect versions of all TiDB instances and choose a job version to use, rules:
//   - if it's in test or run in uni-store, use V2 directly if ForceDDLJobVersionToV1InTest
//     is not set, else use V1, we use this rule to run unit-tests using V1.
//   - if all TiDB instances have version >= 8.4.0, use V2
//   - otherwise, we must during upgrade from lower version, then start a background
//     routine to detect the version of all TiDB instances repeatedly, when upgrade
//     is done, we will use V2, and exit the routine.
//
// Note: at the time of this PR, some job types hasn't finished migrating to V2,
// they will stay in V1 regardless of the version we choose here.
//
// It's possible that user start a new TiDB of version < 8.4.0 after we detect that
// all instances have version >= 8.4.0, we will not consider this case here as we
// don't support downgrade cluster version right now. And even if we try to change
// the job version in use back to V1, it still will not work when owner transfer
// to the new TiDB instance which cannot not handle existing submitted jobs of V2.
func (d *ddl) detectAndUpdateJobVersion() {
	if d.etcdCli == nil {
		if testargsv1.ForceV1 {
			model.SetJobVerInUse(model.JobVersion1)
			return
		}
		model.SetJobVerInUse(model.JobVersion2)
		return
	}

	err := d.detectAndUpdateJobVersionOnce()
	if err != nil {
		logutil.DDLLogger().Warn("detect job version failed", zap.String("err", err.Error()))
	}

	if model.GetJobVerInUse() == model.JobVersion2 {
		return
	}

	logutil.DDLLogger().Info("job version in use is not v2, maybe in upgrade, start detecting",
		zap.Stringer("current", model.GetJobVerInUse()))
	d.wg.RunWithLog(func() {
		ticker := time.NewTicker(detectJobVerInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			case <-d.ctx.Done():
				return
			}
			err = d.detectAndUpdateJobVersionOnce()
			if err != nil {
				logutil.SampleLogger().Warn("detect job version failed", zap.String("err", err.Error()))
			}
			failpoint.InjectCall("afterDetectAndUpdateJobVersionOnce")
			if model.GetJobVerInUse() == model.JobVersion2 {
				logutil.DDLLogger().Info("job version in use is v2 now, stop detecting")
				return
			}
		}
	})
}

// when all TiDB instances have version >= 8.4.0, we can use job version 2, otherwise
// we should use job version 1 to keep compatibility.
func (d *ddl) detectAndUpdateJobVersionOnce() error {
	infos, err := infosync.GetAllServerInfo(d.ctx)
	if err != nil {
		return err
	}
	allSupportV2 := true
	for _, info := range infos {
		// we don't store TiDB version directly, but concatenated with a MySQL version,
		// separated by mysql.VersionSeparator.
		tidbVer := info.Version
		idx := strings.Index(tidbVer, mysql.VersionSeparator)
		if idx < 0 {
			allSupportV2 = false
			// see https://github.com/pingcap/tidb/issues/31823
			logutil.SampleLogger().Warn("unknown server version, might be changed directly in config",
				zap.String("version", tidbVer))
			break
		}
		tidbVer = tidbVer[idx+len(mysql.VersionSeparator):]
		tidbVer = strings.TrimPrefix(tidbVer, "v")
		ver, err2 := semver.NewVersion(tidbVer)
		if err2 != nil {
			allSupportV2 = false
			logutil.SampleLogger().Warn("parse server version failed", zap.String("version", info.Version),
				zap.String("err", err2.Error()))
			break
		}
		// sem-ver also compares pre-release labels, but we don't need to consider
		// them here, so we clear them.
		ver.PreRelease = ""
		if ver.LessThan(jobV2FirstVer) {
			allSupportV2 = false
			break
		}
	}
	targetVer := model.JobVersion1
	if allSupportV2 {
		targetVer = model.JobVersion2
	}
	if model.GetJobVerInUse() != targetVer {
		logutil.DDLLogger().Info("change job version in use",
			zap.Stringer("old", model.GetJobVerInUse()),
			zap.Stringer("new", targetVer))
		model.SetJobVerInUse(targetVer)
	}
	return nil
}

func (d *ddl) CleanUpTempDirLoop(ctx context.Context, path string) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			se, err := d.sessPool.Get()
			if err != nil {
				logutil.DDLLogger().Warn("get session from pool failed", zap.Error(err))
				return
			}
			ingest.CleanUpTempDir(ctx, se, path)
			d.sessPool.Put(se)
		case <-d.ctx.Done():
			return
		}
	}
}

// EnableDDL enable this node to execute ddl.
// Since ownerManager.CampaignOwner will start a new goroutine to run ownerManager.campaignLoop,
// we should make sure that before invoking EnableDDL(), ddl is DISABLE.
func (d *ddl) EnableDDL() error {
	err := d.ownerManager.CampaignOwner()
	return errors.Trace(err)
}

// DisableDDL disable this node to execute ddl.
// We should make sure that before invoking DisableDDL(), ddl is ENABLE.
func (d *ddl) DisableDDL() error {
	if d.ownerManager.IsOwner() {
		// If there is only one node, we should NOT disable ddl.
		serverInfo, err := infosync.GetAllServerInfo(d.ctx)
		if err != nil {
			logutil.DDLLogger().Error("error when GetAllServerInfo", zap.Error(err))
			return err
		}
		if len(serverInfo) <= 1 {
			return dbterror.ErrDDLSetting.GenWithStackByArgs("disabling", "can not disable ddl owner when it is the only one tidb instance")
		}
		// FIXME: if possible, when this node is the only node with DDL, ths setting of DisableDDL should fail.
	}

	// disable campaign by interrupting campaignLoop
	d.ownerManager.CampaignCancel()
	return nil
}

func (d *ddl) close() {
	if d.ctx.Err() != nil {
		return
	}

	startTime := time.Now()
	d.cancel()
	failpoint.InjectCall("afterDDLCloseCancel")
	d.wg.Wait()
	// when run with real-tikv, the lifecycle of ownerManager is managed by globalOwnerManager,
	// when run with uni-store BreakCampaignLoop is same as Close.
	// hope we can unify it after refactor to let some components only start once.
	if d.ownerManager != nil {
		d.ownerManager.BreakCampaignLoop()
	}
	d.schemaVerSyncer.Close()

	// d.delRangeMgr using sessions from d.sessPool.
	// Put it before d.sessPool.close to reduce the time spent by d.sessPool.close.
	if d.delRangeMgr != nil {
		d.delRangeMgr.clear()
	}
	if d.sessPool != nil {
		d.sessPool.Close()
	}
	variable.UnregisterStatistics(d)

	logutil.DDLLogger().Info("DDL closed", zap.String("ID", d.uuid), zap.Duration("take time", time.Since(startTime)))
}

// SchemaSyncer implements DDL.SchemaSyncer interface.
func (d *ddl) SchemaSyncer() schemaver.Syncer {
	return d.schemaVerSyncer
}

// StateSyncer implements DDL.StateSyncer interface.
func (d *ddl) StateSyncer() serverstate.Syncer {
	return d.serverStateSyncer
}

// OwnerManager implements DDL.OwnerManager interface.
func (d *ddl) OwnerManager() owner.Manager {
	return d.ownerManager
}

// GetID implements DDL.GetID interface.
func (d *ddl) GetID() string {
	return d.uuid
}

func (d *ddl) GetMinJobIDRefresher() *systable.MinJobIDRefresher {
	return d.minJobIDRefresher
}
