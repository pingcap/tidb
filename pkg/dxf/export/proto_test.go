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

package export

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestTaskMetaRoundTrip(t *testing.T) {
	meta := &TaskMeta{
		Tables: []TableSpec{
			{DBName: "db1", TableInfo: &model.TableInfo{ID: 100, Name: ast.NewCIStr("t1")}},
			{DBName: "db1", TableInfo: &model.TableInfo{ID: 200, Name: ast.NewCIStr("t2")}},
		},
		SnapshotTS:     123456789,
		Dest:           "s3://bucket/prefix?access-key=x",
		Format:         "csv",
		FileSize:       256 << 20,
		SubtaskRegions: 8,
	}
	bs, err := json.Marshal(meta)
	require.NoError(t, err)
	got := &TaskMeta{}
	require.NoError(t, json.Unmarshal(bs, got))
	require.Len(t, got.Tables, 2)
	require.Equal(t, int64(100), got.Tables[0].TableInfo.ID)
	require.Equal(t, "t2", got.Tables[1].TableInfo.Name.O)
	require.Equal(t, meta.SnapshotTS, got.SnapshotTS)
	require.Equal(t, meta.Dest, got.Dest)
	require.Equal(t, meta.FileSize, got.FileSize)
}

func TestSubtaskMetaRoundTrip(t *testing.T) {
	meta := &SubtaskMeta{Units: []Unit{
		{TableIdx: 0, PhysicalID: 100, Start: []byte("a"), End: []byte("m"), NameOrdinal: 0},
		{TableIdx: 3, PhysicalID: 401, Start: []byte("m"), End: []byte("z"), NameOrdinal: 5},
	}}
	bs, err := json.Marshal(meta)
	require.NoError(t, err)
	got := &SubtaskMeta{}
	require.NoError(t, json.Unmarshal(bs, got))
	require.Equal(t, meta.Units, got.Units)
}
