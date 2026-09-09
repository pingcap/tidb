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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	serverVersionAllowCacheInterval    = 10 * time.Second
	serverVersionFallbackCacheInterval = time.Minute
)

type getServerInfoForTestContextKey struct{}
type getAllServerInfoForTestContextKey struct{}

// ttlJobVersionChecker enables TTL index scans only when all TiDB servers have
// the same build. Otherwise, TTL jobs fall back to the old PK scan path. It is
// not safe for concurrent use.
type ttlJobVersionChecker struct {
	lastCheckTime            time.Time
	lastCheckAllowsIndexScan bool
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

func (c *ttlJobVersionChecker) cachedResult(now time.Time) (allowIndexScan bool, cached bool) {
	if c.lastCheckTime.IsZero() {
		return false, false
	}

	cacheInterval := serverVersionFallbackCacheInterval
	if c.lastCheckAllowsIndexScan {
		cacheInterval = serverVersionAllowCacheInterval
	}
	if now.Sub(c.lastCheckTime) < cacheInterval {
		return c.lastCheckAllowsIndexScan, true
	}
	return false, false
}

func (c *ttlJobVersionChecker) cacheResult(now time.Time, allowIndexScan bool) bool {
	c.lastCheckTime = now
	c.lastCheckAllowsIndexScan = allowIndexScan
	return allowIndexScan
}

// check compares every real TiDB server's complete VersionInfo (the reported
// version string and Git hash) with the current server. Index scan tasks use a
// new range format that old workers cannot interpret, so they are enabled only
// when every server has the same build. A mismatch or any failure falls back to
// the old PK scan task format instead of preventing the TTL job from running.
func (c *ttlJobVersionChecker) check(ctx context.Context) bool {
	now := time.Now()
	if result, ok := c.cachedResult(now); ok {
		return result
	}

	localInfo, err := getServerInfoForTTLJob(ctx)
	if err != nil {
		logutil.Logger(ctx).Warn("failed to get current TiDB server version, create TTL job with PK scan", zap.Error(err))
		return c.cacheResult(now, false)
	}
	if localInfo == nil {
		logutil.Logger(ctx).Warn("current TiDB server info is nil, create TTL job with PK scan")
		return c.cacheResult(now, false)
	}

	serverInfos, err := getAllServerInfoForTTLJob(ctx)
	if err != nil {
		logutil.Logger(ctx).Warn("failed to get TiDB server versions, create TTL job with PK scan", zap.Error(err))
		return c.cacheResult(now, false)
	}

	consistent, err := tiDBServerVersionInfosConsistent(localInfo.VersionInfo, serverInfos)
	if err != nil {
		logutil.Logger(ctx).Warn("failed to check TiDB server build versions, create TTL job with PK scan", zap.Error(err))
		return c.cacheResult(now, false)
	}
	if consistent {
		return c.cacheResult(now, true)
	}

	logutil.Logger(ctx).Warn("TiDB server build versions are inconsistent, create TTL job with PK scan",
		zap.String("currentVersion", localInfo.Version), zap.String("currentGitHash", localInfo.GitHash))
	return c.cacheResult(now, false)
}

func tiDBServerVersionInfosConsistent(currentVersion serverinfo.VersionInfo, serverInfos map[string]*serverinfo.ServerInfo) (bool, error) {
	if len(serverInfos) == 0 {
		return false, errors.New("TiDB server info list is empty")
	}

	realServerCount := 0
	for id, info := range serverInfos {
		if info == nil {
			return false, errors.Errorf("TiDB server info is nil, server ID: %s", id)
		}
		if info.IsAssumed() {
			continue
		}
		realServerCount++
		if currentVersion != info.VersionInfo {
			return false, nil
		}
	}
	if realServerCount == 0 {
		return false, errors.New("TiDB server info list contains no real servers")
	}
	return true, nil
}
