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
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestTiDBServerVersionInfosConsistent(t *testing.T) {
	serverInfo := func(id string, version serverinfo.VersionInfo) *serverinfo.ServerInfo {
		return &serverinfo.ServerInfo{
			StaticInfo: serverinfo.StaticInfo{
				ID:          id,
				VersionInfo: version,
			},
		}
	}
	serverInfos := func(versions ...serverinfo.VersionInfo) map[string]*serverinfo.ServerInfo {
		infos := make(map[string]*serverinfo.ServerInfo, len(versions))
		for i, version := range versions {
			id := strconv.Itoa(i)
			infos[id] = serverInfo(id, version)
		}
		return infos
	}
	serverInfoGettersContext := func(
		getServerInfo func() (*serverinfo.ServerInfo, error),
		getAllServerInfo func(context.Context) (map[string]*serverinfo.ServerInfo, error),
	) context.Context {
		ctx := context.WithValue(context.Background(), getServerInfoForTestContextKey{}, getServerInfo)
		return context.WithValue(ctx, getAllServerInfoForTestContextKey{}, getAllServerInfo)
	}

	version := func(version, gitHash string) serverinfo.VersionInfo {
		return serverinfo.VersionInfo{Version: version, GitHash: gitHash}
	}
	current := version("8.0.11-TiDB-v9.0.0-alpha-123-g1111111", "1111111")
	tests := []struct {
		name        string
		current     serverinfo.VersionInfo
		serverInfos map[string]*serverinfo.ServerInfo
		consistent  bool
		err         bool
	}{
		{
			name:        "same build",
			current:     current,
			serverInfos: serverInfos(current, current),
			consistent:  true,
		},
		{
			name:        "same version with different Git hash",
			current:     current,
			serverInfos: serverInfos(version(current.Version, "2222222")),
		},
		{
			name:    "different prerelease",
			current: current,
			serverInfos: serverInfos(version(
				"8.0.11-TiDB-v9.0.0-alpha-456-g2222222-dirty", current.GitHash,
			)),
		},
		{
			name:        "different release",
			current:     current,
			serverInfos: serverInfos(version("8.0.11-TiDB-v8.5.0", current.GitHash)),
		},
		{name: "empty server info", current: current, err: true},
		{
			name:        "opaque version strings match",
			current:     version("custom-build", current.GitHash),
			serverInfos: serverInfos(version("custom-build", current.GitHash)),
			consistent:  true,
		},
		{
			name:        "opaque version strings differ",
			current:     version("custom-build-1", current.GitHash),
			serverInfos: serverInfos(version("custom-build-2", current.GitHash)),
		},
		{
			name:        "nil remote server info",
			current:     current,
			serverInfos: map[string]*serverinfo.ServerInfo{"nil": nil},
			err:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consistent, err := tiDBServerVersionInfosConsistent(tt.current, tt.serverInfos)
			if tt.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.consistent, consistent)
		})
	}

	t.Run("ignore assumed server info", func(t *testing.T) {
		assumed := serverInfo("assumed", version("8.0.11-TiDB-v8.5.0", "2222222"))
		assumed.AssumedKeyspace = "system"
		consistent, err := tiDBServerVersionInfosConsistent(current, map[string]*serverinfo.ServerInfo{
			"real":    serverInfo("real", current),
			"assumed": assumed,
		})
		require.NoError(t, err)
		require.True(t, consistent)
	})

	t.Run("only assumed server info", func(t *testing.T) {
		assumed := serverInfo("assumed", current)
		assumed.AssumedKeyspace = "system"
		_, err := tiDBServerVersionInfosConsistent(current, map[string]*serverinfo.ServerInfo{"assumed": assumed})
		require.Error(t, err)
	})

	t.Run("cache version check", func(t *testing.T) {
		localVersion := "8.0.11" + mysql.VersionSeparator + "v9.0.0"
		differentVersion := "8.0.11" + mysql.VersionSeparator + "v10.0.0"
		localVersionInfo := version(localVersion, "1111111")

		calls := 0
		remoteVersionInfo := localVersionInfo
		checker := &ttlJobVersionChecker{}
		ctx := serverInfoGettersContext(
			func() (*serverinfo.ServerInfo, error) {
				return serverInfo("local", localVersionInfo), nil
			},
			func(context.Context) (map[string]*serverinfo.ServerInfo, error) {
				calls++
				return serverInfos(remoteVersionInfo), nil
			},
		)

		require.Equal(t, ttlJobVersionAllowIndexScan, checker.check(ctx))
		require.Equal(t, ttlJobVersionAllowIndexScan, checker.check(ctx))
		require.Equal(t, 1, calls)

		checker.lastCheckTime = time.Now().Add(-serverVersionAllowCacheInterval)
		require.Equal(t, ttlJobVersionAllowIndexScan, checker.check(ctx))
		require.Equal(t, 2, calls)

		remoteVersionInfo = version(differentVersion, "2222222")
		checker.lastCheckTime = time.Now().Add(-serverVersionAllowCacheInterval)
		require.Equal(t, ttlJobVersionBlockJob, checker.check(ctx))
		require.Equal(t, ttlJobVersionBlockJob, checker.check(ctx))
		require.Equal(t, 3, calls)

		// Even if the cluster has converged, the mismatch remains cached briefly
		// to avoid repeated server-info requests during a rolling upgrade.
		remoteVersionInfo = localVersionInfo
		require.Equal(t, ttlJobVersionBlockJob, checker.check(ctx))
		require.Equal(t, 3, calls)

		checker.lastCheckTime = time.Now().Add(-serverVersionMismatchCacheInterval)
		require.Equal(t, ttlJobVersionAllowIndexScan, checker.check(ctx))
		require.Equal(t, 4, calls)
	})

	t.Run("fallback to PK", func(t *testing.T) {
		validVersion := "8.0.11" + mysql.VersionSeparator + "v9.0.0"
		validVersionInfo := version(validVersion, "1111111")
		for _, tt := range []struct {
			name             string
			localInfo        *serverinfo.ServerInfo
			localErr         error
			allServerInfo    map[string]*serverinfo.ServerInfo
			allServerInfoErr error
			expectedAllCalls int
		}{
			{
				name:             "current server lookup fails",
				localErr:         errors.New("mock current server info error"),
				allServerInfo:    serverInfos(validVersionInfo),
				expectedAllCalls: 0,
			},
			{
				name:             "server list lookup fails",
				localInfo:        serverInfo("local", validVersionInfo),
				allServerInfoErr: errors.New("mock server info error"),
				expectedAllCalls: 1,
			},
			{
				name:             "versions cannot be compared",
				localInfo:        serverInfo("local", validVersionInfo),
				allServerInfo:    map[string]*serverinfo.ServerInfo{},
				expectedAllCalls: 1,
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				calls := 0
				checker := &ttlJobVersionChecker{}
				ctx := serverInfoGettersContext(
					func() (*serverinfo.ServerInfo, error) { return tt.localInfo, tt.localErr },
					func(context.Context) (map[string]*serverinfo.ServerInfo, error) {
						calls++
						return tt.allServerInfo, tt.allServerInfoErr
					},
				)
				require.Equal(t, ttlJobVersionFallbackToPK, checker.check(ctx))
				require.Equal(t, ttlJobVersionFallbackToPK, checker.check(ctx))
				require.Equal(t, tt.expectedAllCalls, calls)
			})
		}
	})
}
