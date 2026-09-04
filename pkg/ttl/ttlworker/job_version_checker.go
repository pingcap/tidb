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

package ttlworker

import (
	"context"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	serverVersionAllowCacheInterval    = 10 * time.Second
	serverVersionMismatchCacheInterval = time.Minute
)

type ttlJobVersionCheckResult int

const (
	// Unknown versions fall back to the old primary-key scan path. This keeps
	// TTL available without creating index scan tasks that an older TiDB may
	// not understand during a rolling upgrade.
	ttlJobVersionFallbackToPK ttlJobVersionCheckResult = iota
	ttlJobVersionAllowIndexScan
	ttlJobVersionBlockJob
)

type getServerInfoForTestContextKey struct{}
type getAllServerInfoForTestContextKey struct{}

// ttlJobVersionChecker gates TTL index scans while TiDB server versions are
// inconsistent during a rolling upgrade. It is not safe for concurrent use.
type ttlJobVersionChecker struct {
	lastCheckTime   time.Time
	lastCheckResult ttlJobVersionCheckResult
}

func getServerInfoForTTLJob(ctx context.Context) (*serverinfo.ServerInfo, error) {
	if intest.InTest && ctx != nil {
		if getter, ok := ctx.Value(getServerInfoForTestContextKey{}).(func() (*serverinfo.ServerInfo, error)); ok {
			return getter()
		}
	}
	return infosync.GetServerInfo()
}

func getAllServerInfoForTTLJob(ctx context.Context) (map[string]*serverinfo.ServerInfo, error) {
	if intest.InTest && ctx != nil {
		if getter, ok := ctx.Value(getAllServerInfoForTestContextKey{}).(func(context.Context) (map[string]*serverinfo.ServerInfo, error)); ok {
			return getter(ctx)
		}
	}
	return infosync.GetAllServerInfo(ctx)
}

func (c *ttlJobVersionChecker) cachedResult(now time.Time) (ttlJobVersionCheckResult, bool) {
	if c.lastCheckTime.IsZero() {
		return ttlJobVersionFallbackToPK, false
	}

	cacheInterval := serverVersionMismatchCacheInterval
	if c.lastCheckResult != ttlJobVersionBlockJob {
		cacheInterval = serverVersionAllowCacheInterval
	}
	if now.Sub(c.lastCheckTime) < cacheInterval {
		return c.lastCheckResult, true
	}
	return ttlJobVersionFallbackToPK, false
}

func (c *ttlJobVersionChecker) cacheResult(now time.Time, result ttlJobVersionCheckResult) ttlJobVersionCheckResult {
	c.lastCheckTime = now
	c.lastCheckResult = result
	return result
}

// check compares every known TiDB server's normalized semver with the current
// server. It compares only the semantic version part after "TiDB-v" and ignores
// prerelease/build metadata, including Git hashes. Equal versions may use the
// new index scan path; unequal versions block new TTL jobs. Lookup or parse
// failures allow a job but make it use the old PK scan.
func (c *ttlJobVersionChecker) check(ctx context.Context) ttlJobVersionCheckResult {
	now := time.Now()
	if result, ok := c.cachedResult(now); ok {
		return result
	}

	localInfo, err := getServerInfoForTTLJob(ctx)
	if err != nil {
		logutil.Logger(ctx).Warn("failed to get current TiDB server version, create TTL job with PK scan", zap.Error(err))
		return c.cacheResult(now, ttlJobVersionFallbackToPK)
	}
	if localInfo == nil {
		logutil.Logger(ctx).Warn("current TiDB server info is nil, create TTL job with PK scan")
		return c.cacheResult(now, ttlJobVersionFallbackToPK)
	}

	serverInfos, err := getAllServerInfoForTTLJob(ctx)
	if err != nil {
		logutil.Logger(ctx).Warn("failed to get TiDB server versions, create TTL job with PK scan", zap.Error(err))
		return c.cacheResult(now, ttlJobVersionFallbackToPK)
	}

	consistent, err := tiDBServerVersionsConsistent(localInfo.Version, serverInfos)
	if err != nil {
		logutil.Logger(ctx).Warn("failed to check TiDB server versions, create TTL job with PK scan", zap.Error(err))
		return c.cacheResult(now, ttlJobVersionFallbackToPK)
	}
	if consistent {
		return c.cacheResult(now, ttlJobVersionAllowIndexScan)
	}

	logutil.Logger(ctx).Warn("skip creating TTL job because TiDB server versions are inconsistent",
		zap.String("currentVersion", localInfo.Version))
	return c.cacheResult(now, ttlJobVersionBlockJob)
}

func tiDBServerVersionsConsistent(currentVersion string, serverInfos map[string]*serverinfo.ServerInfo) (bool, error) {
	if len(serverInfos) == 0 {
		return false, errors.New("TiDB server info list is empty")
	}

	current, err := normalizedTiDBVersion(currentVersion)
	if err != nil {
		return false, errors.Wrap(err, "parse current TiDB server version")
	}
	consistent := true
	for id, info := range serverInfos {
		if info == nil {
			return false, errors.Errorf("TiDB server info is nil, server ID: %s", id)
		}
		version, err := normalizedTiDBVersion(info.Version)
		if err != nil {
			return false, errors.Wrapf(err, "parse TiDB server version, server ID: %s", id)
		}
		if !current.Equal(*version) {
			consistent = false
		}
	}
	return consistent, nil
}

func normalizedTiDBVersion(serverVersion string) (*semver.Version, error) {
	idx := strings.Index(serverVersion, mysql.VersionSeparator)
	if idx < 0 {
		return nil, errors.Errorf("unknown server version: %s", serverVersion)
	}
	tidbVersion := strings.TrimPrefix(serverVersion[idx+len(mysql.VersionSeparator):], "v")
	version, err := semver.NewVersion(tidbVersion)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Keep this normalization consistent with DDL job version detection. Build
	// metadata does not affect semver equality, and prerelease labels are ignored.
	version.PreRelease = ""
	return version, nil
}
