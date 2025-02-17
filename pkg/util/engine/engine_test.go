// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

func TestIsTiFlashHTTPResp(t *testing.T) {
	tests := []struct {
		name  string
		store *pdhttp.MetaStore
		want  bool
	}{
		{
			name: "Test with TiFlash label",
			store: &pdhttp.MetaStore{
				Labels: []pdhttp.StoreLabel{
					{
						Key:   "engine",
						Value: "tiflash",
					},
				},
			},
			want: true,
		},
		{
			name: "Test with TiFlash label 2",
			store: &pdhttp.MetaStore{
				Labels: []pdhttp.StoreLabel{
					{
						Key:   "engine",
						Value: "tiflash_compute",
					},
				},
			},
			want: true,
		},
		{
			name: "Test without TiFlash label",
			store: &pdhttp.MetaStore{
				Labels: []pdhttp.StoreLabel{
					{
						Key:   "engine",
						Value: "not_tiflash",
					},
				},
			},
			want: false,
		},
		{
			name: "Test with no labels",
			store: &pdhttp.MetaStore{
				Labels: []pdhttp.StoreLabel{},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsTiFlashHTTPResp(tt.store))
		})
	}
}
