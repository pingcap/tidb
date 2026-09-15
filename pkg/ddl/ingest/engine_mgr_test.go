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

package ingest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestConfigureTiCIWriteForIndex(t *testing.T) {
	idxID := int64(123)

	cfg := &backend.EngineConfig{}
	configureTiCIWriteForIndex(cfg, nil, idxID)
	require.False(t, cfg.TiCIWriteEnabled)
	require.Zero(t, cfg.TiCIIndexID)

	cfg = &backend.EngineConfig{}
	configureTiCIWriteForIndex(cfg, &model.IndexInfo{}, idxID)
	require.False(t, cfg.TiCIWriteEnabled)
	require.Zero(t, cfg.TiCIIndexID)

	cfg = &backend.EngineConfig{}
	configureTiCIWriteForIndex(cfg, &model.IndexInfo{FullTextInfo: &model.FullTextIndexInfo{}}, idxID)
	require.True(t, cfg.TiCIWriteEnabled)
	require.Equal(t, idxID, cfg.TiCIIndexID)

	cfg = &backend.EngineConfig{}
	configureTiCIWriteForIndex(cfg, &model.IndexInfo{HybridInfo: &model.HybridIndexInfo{}}, idxID)
	require.True(t, cfg.TiCIWriteEnabled)
	require.Equal(t, idxID, cfg.TiCIIndexID)
}
