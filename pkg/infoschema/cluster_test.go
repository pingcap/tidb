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

package infoschema

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/stretchr/testify/require"
)

func TestSlowQueryPhaseBackoffTypesCompatible(t *testing.T) {
	local := &serverinfo.ServerInfo{StaticInfo: serverinfo.StaticInfo{VersionInfo: serverinfo.VersionInfo{
		Version: "v9.0.0",
		GitHash: "new",
	}}}

	tests := []struct {
		name    string
		servers map[string]*serverinfo.ServerInfo
		want    bool
	}{
		{
			name: "same build",
			servers: map[string]*serverinfo.ServerInfo{
				"local": local,
				"peer":  {StaticInfo: serverinfo.StaticInfo{VersionInfo: serverinfo.VersionInfo{Version: "v9.0.0", GitHash: "new"}}},
			},
			want: true,
		},
		{
			name: "different build",
			servers: map[string]*serverinfo.ServerInfo{
				"local": local,
				"peer":  {StaticInfo: serverinfo.StaticInfo{VersionInfo: serverinfo.VersionInfo{Version: "v9.0.0", GitHash: "old"}}},
			},
			want: false,
		},
		{
			name:    "nil server",
			servers: map[string]*serverinfo.ServerInfo{"peer": nil},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, slowQueryPhaseBackoffTypesCompatible(local, tt.servers))
		})
	}
}
