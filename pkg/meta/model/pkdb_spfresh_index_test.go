// Copyright 2024 PingCAP, Inc.
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

package model

import (
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestSPFreshDistanceMetricAPIString(t *testing.T) {
	cases := []struct {
		metric DistanceMetric
		want   string
		ok     bool
	}{
		{metric: DistanceMetricL2, want: "l2", ok: true},
		{metric: DistanceMetricCosine, want: "cosine", ok: true},
		{metric: DistanceMetricInnerProduct, want: "inner_product", ok: true},
		{metric: DistanceMetric("unknown")},
	}
	for _, tc := range cases {
		got, ok := SPFreshDistanceMetricAPIString(tc.metric)
		require.Equal(t, tc.ok, ok)
		require.Equal(t, tc.want, got)
	}
}

func TestParseSPFreshVectorIndexParam(t *testing.T) {
	indexOptions, searchConfig, runtimeConfig, err := ParseSPFreshVectorIndexParam(
		" rot_algorithm = matrix , min_partition_size = 32 , max_partition_size = 256 , build_shard = 4 , write_beam_size = 24 , search_beam_size = 64 , oversample_factor = 1.25 , read_only = true, max_fixup_workers=37,max_fixups=400,stalled_op_timeout_ms=2000,max_insert_attempts=64 ",
	)
	require.NoError(t, err)
	require.Equal(t, &SPFreshVectorIndexOptions{
		RotAlgorithm:     "matrix",
		MinPartitionSize: 32,
		MaxPartitionSize: 256,
		BuildShard:       4,
		WriteBeamSize:    24,
	}, indexOptions)
	require.Equal(t, &SPFreshVectorSearchConfig{
		SearchBeamSize:   64,
		OversampleFactor: 1.25,
		ReadOnly:         spfreshTestPtr(true),
	}, searchConfig)
	require.Equal(t, SPFreshVectorRuntimeConfig{
		HammingFactor:      96,
		Rabitq4Factor:      4,
		MaxFixupWorkers:    37,
		MaxFixups:          400,
		StalledOpTimeoutMS: 2000,
		MaxInsertAttempts:  64,
	}, runtimeConfig)

	indexOptions, searchConfig, runtimeConfig, err = ParseSPFreshVectorIndexParam("write_beam_size=24")
	require.NoError(t, err)
	require.Equal(t, &SPFreshVectorIndexOptions{WriteBeamSize: 24}, indexOptions)
	require.Nil(t, searchConfig)
	for _, shards := range []uint32{1, SPFreshMaxBuildShards} {
		param := fmt.Sprintf("build_shard=%d", shards)
		indexOptions, searchConfig, _, err = ParseSPFreshVectorIndexParam(param)
		require.NoError(t, err)
		require.Equal(t, &SPFreshVectorIndexOptions{BuildShard: shards}, indexOptions)
		require.Nil(t, searchConfig)
		require.Equal(t, param, FormatSPFreshVectorIndexParam(indexOptions, nil, nil))
	}

	indexOptions, searchConfig, _, err = ParseSPFreshVectorIndexParam("min_partition_size=512")
	require.NoError(t, err)
	require.Equal(t, &SPFreshVectorIndexOptions{MinPartitionSize: 512}, indexOptions)
	require.Nil(t, searchConfig)

	indexOptions, searchConfig, _, err = ParseSPFreshVectorIndexParam("max_partition_size=64")
	require.NoError(t, err)
	require.Equal(t, &SPFreshVectorIndexOptions{MaxPartitionSize: 64}, indexOptions)
	require.Nil(t, searchConfig)

	indexOptions, searchConfig, _, err = ParseSPFreshVectorIndexParam("min_partition_size=16,max_partition_size=32")
	require.NoError(t, err)
	require.Equal(t, &SPFreshVectorIndexOptions{MinPartitionSize: 16, MaxPartitionSize: 32}, indexOptions)
	require.Nil(t, searchConfig)

	indexOptions, searchConfig, runtimeConfig, err = ParseSPFreshVectorIndexParam(" ")
	require.NoError(t, err)
	require.Nil(t, indexOptions)
	require.Nil(t, searchConfig)
	require.Equal(t, DefaultSPFreshVectorRuntimeConfig(), runtimeConfig)
}

func TestParseSPFreshVectorIndexParamRejectsInvalidCreateConfig(t *testing.T) {
	cases := []string{
		"rot_algorithm=bad",
		"rot_algorithm=Matrix",
		"min_partition_size=0",
		"max_partition_size=1",
		"min_partition_size=513",
		"max_partition_size=63",
		"build_shard=0",
		"build_shard=-1",
		"build_shard=257",
		"build_shard=4294967296",
		"build_shard=1.5",
		"build_shard=auto",
		"build_shard=1,build_shard=4",
		"write_beam_size=0",
		"write_beam_size=4097",
		"search_beam_size=4097",
		"min_partition_size=256,max_partition_size=32",
		"max_fixup_workers=-1",
		"max_fixups=0",
		"max_fixups=1025",
		"stalled_op_timeout_ms=3600001",
		"max_insert_attempts=0",
		"max_insert_attempts=1025",
	}
	for _, param := range cases {
		_, _, _, err := ParseSPFreshVectorIndexParam(param)
		require.Error(t, err, param)
	}
}

func TestSPFreshVectorSearchConfigClone(t *testing.T) {
	readOnly := false
	cfg := &SPFreshVectorSearchConfig{
		SearchBeamSize:   128,
		OversampleFactor: 2,
		ReadOnly:         &readOnly,
	}
	cloned := cfg.Clone()
	require.Equal(t, cfg, cloned)
	require.NotSame(t, cfg, cloned)
	require.NotSame(t, cfg.ReadOnly, cloned.ReadOnly)
}

func TestSPFreshTopKValid(t *testing.T) {
	require.False(t, SPFreshTopKValid(0))
	require.True(t, SPFreshTopKValid(1))
	require.True(t, SPFreshTopKValid(SPFreshMaxTopK))
	require.False(t, SPFreshTopKValid(SPFreshMaxTopK+1))
}

func TestSPFreshRerankCandidateBudget(t *testing.T) {
	cases := []struct {
		name               string
		topK               uint64
		cfg                *SPFreshVectorSearchConfig
		wantCountKnown     bool
		wantCandidateCount uint64
		wantValid          bool
	}{
		{
			name:      "unset_nil_uses_worker_default",
			topK:      SPFreshMaxTopK,
			wantValid: true,
		},
		{
			name:      "unset_zero_uses_worker_default",
			topK:      SPFreshMaxTopK,
			cfg:       &SPFreshVectorSearchConfig{},
			wantValid: true,
		},
		{
			name:               "configured_exact_boundary",
			topK:               SPFreshMaxTopK,
			cfg:                &SPFreshVectorSearchConfig{OversampleFactor: 1.25},
			wantCountKnown:     true,
			wantCandidateCount: SPFreshMaxRerankCandidates,
			wantValid:          true,
		},
		{
			name:               "configured_fraction_rounds_up",
			topK:               4096,
			cfg:                &SPFreshVectorSearchConfig{OversampleFactor: 2.0001},
			wantCountKnown:     true,
			wantCandidateCount: 8193,
			wantValid:          true,
		},
		{
			name:               "configured_over_boundary",
			topK:               6401,
			cfg:                &SPFreshVectorSearchConfig{OversampleFactor: 1.6},
			wantCountKnown:     true,
			wantCandidateCount: 10_242,
		},
		{
			name: "overflow",
			topK: ^uint64(0),
			cfg:  &SPFreshVectorSearchConfig{OversampleFactor: 16},
		},
		{
			name: "invalid_configured_oversample",
			topK: 1,
			cfg:  &SPFreshVectorSearchConfig{OversampleFactor: 0.5},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidates, ok := SPFreshRerankCandidateCount(tc.topK, tc.cfg)
			require.Equal(t, tc.wantCountKnown, ok)
			if tc.wantCountKnown {
				require.Equal(t, tc.wantCandidateCount, candidates)
			}
			require.Equal(t, tc.wantValid, SPFreshRerankCandidateBudgetValid(tc.topK, tc.cfg))
		})
	}
}

func TestSPFreshVectorIndexParamStringAndClone(t *testing.T) {
	indexOptions := &SPFreshVectorIndexOptions{
		RotAlgorithm:     "givens",
		MinPartitionSize: 32,
		MaxPartitionSize: 256,
		BuildShard:       4,
		WriteBeamSize:    24,
	}
	searchConfig := &SPFreshVectorSearchConfig{
		SearchBeamSize:   64,
		OversampleFactor: 1.25,
		ReadOnly:         spfreshTestPtr(false),
	}
	runtimeConfig := SPFreshVectorRuntimeConfig{
		HammingFactor:      96,
		Rabitq4Factor:      4,
		MaxFixupWorkers:    37,
		MaxFixups:          400,
		StalledOpTimeoutMS: 2000,
		MaxInsertAttempts:  64,
	}
	require.Equal(t,
		"rot_algorithm=givens,min_partition_size=32,max_partition_size=256,build_shard=4,write_beam_size=24,search_beam_size=64,oversample_factor=1.25,read_only=false,max_fixup_workers=37,max_fixups=400,stalled_op_timeout_ms=2000,max_insert_attempts=64",
		FormatSPFreshVectorIndexParam(indexOptions, searchConfig, &runtimeConfig),
	)
	defaultRuntimeConfig := DefaultSPFreshVectorRuntimeConfig()
	require.Empty(t, FormatSPFreshVectorIndexParam(nil, nil, &defaultRuntimeConfig))
	cloned := indexOptions.Clone()
	require.Equal(t, indexOptions, cloned)
	require.NotSame(t, indexOptions, cloned)
}

func TestIndexInfoCloneClonesSPFreshStoredColumnIDs(t *testing.T) {
	idx := &IndexInfo{
		Name: model.NewCIStr("idx"),
		VectorInfo: &VectorIndexInfo{
			Kind:            VectorIndexKindSPFresh,
			Dimension:       3,
			DistanceMetric:  DistanceMetricCosine,
			StoredColumnIDs: []int64{123},
		},
	}

	cloned := idx.Clone()
	require.NotNil(t, cloned)
	require.NotNil(t, cloned.VectorInfo)
	require.Equal(t, idx.VectorInfo.StoredColumnIDs, cloned.VectorInfo.StoredColumnIDs)
	require.NotSame(t, &idx.VectorInfo.StoredColumnIDs[0], &cloned.VectorInfo.StoredColumnIDs[0])
}

func TestSPFreshCandidateFactorsCreateAlterAndShow(t *testing.T) {
	defaults := DefaultSPFreshVectorRuntimeConfig()
	for _, pair := range [][2]uint64{{1, 1}, {8, 4}, {64, 3}, {256, 256}} {
		param := fmt.Sprintf("hamming_factor=%d,rabitq4_factor=%d", pair[0], pair[1])
		_, _, cfg, err := ParseSPFreshVectorIndexParam(param)
		require.NoError(t, err)
		require.Equal(t, pair[0], cfg.HammingFactor)
		require.Equal(t, pair[1], cfg.Rabitq4Factor)
		canonical := FormatSPFreshVectorIndexParam(nil, nil, &cfg)
		_, _, roundTrip, err := ParseSPFreshVectorIndexParam(canonical)
		require.NoError(t, err)
		require.Equal(t, cfg, roundTrip)
	}
	_, cfg, err := ParseSPFreshVectorMutableConfig("hamming_factor=8", defaults)
	require.NoError(t, err)
	want := defaults
	want.HammingFactor = 8
	require.Equal(t, want, cfg)
	_, _, err = ParseSPFreshVectorMutableConfig("rabitq4_factor=9", cfg)
	require.Error(t, err) // Validate against the merged pair, not the patch.
	_, cfg, err = ParseSPFreshVectorMutableConfig("rabitq4_factor=9,hamming_factor=10", cfg)
	require.NoError(t, err)
	want.HammingFactor, want.Rabitq4Factor = 10, 9
	require.Equal(t, want, cfg)
	_, cfg, err = ParseSPFreshVectorMutableConfig("", cfg)
	require.NoError(t, err)
	require.Equal(t, defaults, cfg)
	for _, param := range []string{"hamming_factor=0", "hamming_factor=3", "hamming_factor=257", "rabitq4_factor=0", "rabitq4_factor=97", "rabitq4_factor=1.5", "hamming_factor=-1", "hamming_factor=NaN", "hamming_factor=8,hamming_factor=16"} {
		_, _, _, err := ParseSPFreshVectorIndexParam(param)
		require.Error(t, err, param)
	}
}

// Existing HNSW and partial-index metadata must retain its meaning after upgrade.
func TestSPFreshBackportPreservesReleaseIndexMetadata(t *testing.T) {
	require.Equal(t, model.IndexType(5), model.IndexTypeHNSW)
	require.Equal(t, model.IndexType(8), model.IndexTypeSPFresh)
	var legacy IndexInfo
	require.NoError(t, json.Unmarshal([]byte(`{"index_type":5,"vector_index":{"dimension":3,"distance_metric":"L2"}}`), &legacy))
	require.True(t, legacy.IsTiFlashLocalIndex())
	require.False(t, legacy.IsSPFreshIndex())
	require.Equal(t, legacy.VectorInfo, legacy.Clone().VectorInfo)
	_, supported := IndexableFnNameToDistanceMetric[ast.VecNegativeInnerProduct]
	require.False(t, supported)
	_, supported = VectorIndexDistanceMetricByFnName(VectorIndexKindHNSW, ast.VecNegativeInnerProduct)
	require.False(t, supported)
	_, supported = VectorIndexDistanceMetricByFnName(VectorIndexKindSPFresh, ast.VecNegativeInnerProduct)
	require.True(t, supported)

	var partial IndexInfo
	require.NoError(t, json.Unmarshal([]byte(`{"index_type":1,"partial_condition_expr_string":"a > 0","affect_column":[{"name":{"O":"a","L":"a"},"offset":0,"length":-1}]}`), &partial))
	require.True(t, partial.HasCondition())
	clone := partial.Clone()
	require.Equal(t, partial.ConditionExprString, clone.ConditionExprString)
	require.Equal(t, partial.AffectColumn, clone.AffectColumn)
	require.Equal(t, partial.Tp, clone.Tp)
	clone.AffectColumn[0].Offset = 3
	require.Equal(t, 0, partial.AffectColumn[0].Offset)
}

func spfreshTestPtr[T any](value T) *T { return &value }
