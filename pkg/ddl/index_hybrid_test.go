// Copyright 2025 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestEnsureHybridIndexReorgMeta(t *testing.T) {
	cases := []struct {
		name string
		job  *model.Job
		want string
	}{
		{
			name: "missing reorg meta",
			job:  &model.Job{},
			want: "hybrid index requires",
		},
		{
			name: "not dist reorg",
			job: &model.Job{
				ReorgMeta: &model.DDLReorgMeta{
					IsDistReorg: false,
					IsFastReorg: true,
					ReorgTp:     model.ReorgTypeIngest,
				},
			},
			want: "hybrid index requires",
		},
		{
			name: "not fast reorg",
			job: &model.Job{
				ReorgMeta: &model.DDLReorgMeta{
					IsDistReorg: true,
					IsFastReorg: false,
					ReorgTp:     model.ReorgTypeIngest,
				},
			},
			want: "hybrid index requires",
		},
		{
			name: "non ingest reorg type",
			job: &model.Job{
				ReorgMeta: &model.DDLReorgMeta{
					IsDistReorg: true,
					IsFastReorg: true,
					ReorgTp:     model.ReorgTypeTxn,
				},
			},
			want: "hybrid index requires",
		},
		{
			name: "ingest ok",
			job: &model.Job{
				ReorgMeta: &model.DDLReorgMeta{
					IsDistReorg: true,
					IsFastReorg: true,
					ReorgTp:     model.ReorgTypeIngest,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ensureHybridIndexReorgMeta(tc.job)
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestShouldSkipTempIndexMerge(t *testing.T) {
	require.False(t, shouldSkipTempIndexMerge(nil))

	require.False(t, shouldSkipTempIndexMerge([]*model.IndexInfo{
		{VectorInfo: &model.VectorIndexInfo{}},
	}))

	require.True(t, shouldSkipTempIndexMerge([]*model.IndexInfo{
		{HybridInfo: &model.HybridIndexInfo{}},
	}))
}
