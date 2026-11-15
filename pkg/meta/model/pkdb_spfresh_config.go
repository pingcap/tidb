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
	"fmt"
	"math"
	"strconv"
	"strings"
)

// SPFreshVectorIndexOptions is the create-time SPFresh index config.
type SPFreshVectorIndexOptions struct {
	// RotAlgorithm chooses the random orthogonal transform. Empty means unset.
	RotAlgorithm string `json:"rot_algorithm,omitempty"`
	// MinPartitionSize is the minimum SPFresh partition size. Zero means unset.
	MinPartitionSize uint64 `json:"min_partition_size,omitempty"`
	// MaxPartitionSize is the maximum SPFresh partition size. Zero means unset.
	MaxPartitionSize uint64 `json:"max_partition_size,omitempty"`
	// BuildShard is the requested coarse bulk-build shard count. Zero selects memory-based planning.
	BuildShard uint32 `json:"build_shard,omitempty"`
	// WriteBeamSize is the write-route beam size. Zero means unset.
	WriteBeamSize uint64 `json:"write_beam_size,omitempty"`
}

// SPFreshVectorSearchConfig is the index-level SPFresh search-time config.
type SPFreshVectorSearchConfig struct {
	// SearchBeamSize is sent to TiKV as SearchBeamSize. Zero means unset.
	SearchBeamSize uint32 `json:"search_beam_size,omitempty"`
	// OversampleFactor is sent to TiKV as OversampleFactor. Zero means unset.
	OversampleFactor float64 `json:"oversample_factor,omitempty"`
	// ReadOnly is sent to TiKV as ReadOnly when non-nil. Nil means unset.
	ReadOnly *bool `json:"read_only,omitempty"`
}

// SPFreshVectorRuntimeConfig is the complete online-mutable SPFresh configuration.
type SPFreshVectorRuntimeConfig struct {
	// HammingFactor bounds global Hamming survivors relative to ceil(top-k * F).
	HammingFactor uint64 `json:"hamming_factor"`
	// Rabitq4Factor bounds global four-bit survivors relative to ceil(top-k * F).
	Rabitq4Factor uint64 `json:"rabitq4_factor"`
	// MaxFixupWorkers limits per-index fixup workers. Zero selects half of the TiKV process CPU quota.
	MaxFixupWorkers uint64 `json:"max_fixup_workers"`
	// MaxFixups limits queued and running background fixups.
	MaxFixups uint64 `json:"max_fixups"`
	// StalledOpTimeoutMS controls stalled-operation detection. Zero selects the adaptive default.
	StalledOpTimeoutMS uint64 `json:"stalled_op_timeout_ms"`
	// MaxInsertAttempts limits retries for a newly started insert operation.
	MaxInsertAttempts uint64 `json:"max_insert_attempts"`
}

const (
	spfreshVectorIndexParamRotAlgorithm         = "rot_algorithm"
	spfreshVectorIndexParamMinPartitionSize     = "min_partition_size"
	spfreshVectorIndexParamMaxPartitionSize     = "max_partition_size"
	spfreshVectorIndexParamBuildShard           = "build_shard"
	spfreshVectorIndexParamWriteBeamSize        = "write_beam_size"
	spfreshVectorSearchParamSearchBeamSize      = "search_beam_size"
	spfreshVectorSearchParamOversampleFactor    = "oversample_factor"
	spfreshVectorSearchParamReadOnly            = "read_only"
	spfreshVectorRuntimeParamHammingFactor      = "hamming_factor"
	spfreshVectorRuntimeParamRabitq4Factor      = "rabitq4_factor"
	spfreshVectorRuntimeParamMaxFixupWorkers    = "max_fixup_workers"
	spfreshVectorRuntimeParamMaxFixups          = "max_fixups"
	spfreshVectorRuntimeParamStalledOpTimeoutMS = "stalled_op_timeout_ms"
	spfreshVectorRuntimeParamMaxInsertAttempts  = "max_insert_attempts"
	defaultSPFreshVectorMinPartitionSize        = 64
	defaultSPFreshVectorMaxPartitionSize        = 512
	maxSPFreshVectorWriteBeamSize               = 4096
	maxSPFreshVectorSearchBeamSize              = 4096
	maxSPFreshVectorSearchOversampleFactor      = 16
	maxSPFreshVectorCandidateFactor             = 256
	maxSPFreshVectorFixups                      = 1024
	maxSPFreshVectorStalledOpTimeoutMS          = 3600000
	maxSPFreshVectorInsertAttempts              = 1024
)

// DefaultSPFreshVectorMaxPartitionSize returns the effective `max_partition_size`
// used when a create-time SPFresh index option leaves it unset. Callers that
// need to reason about the default (for example to build a value that must be
// rejected) read it from here so the rule and its default stay in one place.
func DefaultSPFreshVectorMaxPartitionSize() uint64 {
	return defaultSPFreshVectorMaxPartitionSize
}

const (
	defaultSPFreshVectorMaxFixups         = 200
	defaultSPFreshVectorMaxInsertAttempts = 32
)

const (
	// SPFreshMaxBuildShards bounds the current flat coarse router, not the number of refined tasks.
	SPFreshMaxBuildShards uint32 = 256
	// SPFreshMaxTopK is the maximum top-k accepted by the TiKV SPFresh search worker.
	SPFreshMaxTopK uint64 = 8_192
	// SPFreshMaxRerankCandidates is the maximum rough-candidate target accepted by
	// the TiKV SPFresh search worker.
	SPFreshMaxRerankCandidates uint64 = 10_240
)

// SPFreshTopKValid reports whether topK satisfies the worker's independent wire limit.
func SPFreshTopKValid(topK uint64) bool {
	return topK > 0 && topK <= SPFreshMaxTopK
}

// SPFreshRerankCandidateCount returns ceil(topK * oversample) for an explicit
// oversample setting. It returns false when oversample is unset because the
// worker owns the default and must perform the final candidate-budget check.
func SPFreshRerankCandidateCount(topK uint64, cfg *SPFreshVectorSearchConfig) (uint64, bool) {
	if cfg == nil || cfg.OversampleFactor == 0 {
		return 0, false
	}
	oversample := cfg.OversampleFactor
	if math.IsNaN(oversample) || math.IsInf(oversample, 0) ||
		oversample < 1 || oversample > maxSPFreshVectorSearchOversampleFactor {
		return 0, false
	}
	candidates := float64(topK) * oversample
	if math.IsNaN(candidates) || math.IsInf(candidates, 0) || candidates >= math.Ldexp(1, 64) {
		return 0, false
	}
	return uint64(math.Ceil(candidates)), true
}

// SPFreshRerankCandidateBudgetValid reports whether an explicit oversample
// setting fits the worker's rough-candidate limit. An unset setting is valid
// here because the worker applies its configured default and final budget check.
func SPFreshRerankCandidateBudgetValid(topK uint64, cfg *SPFreshVectorSearchConfig) bool {
	if cfg == nil || cfg.OversampleFactor == 0 {
		return true
	}
	candidates, ok := SPFreshRerankCandidateCount(topK, cfg)
	return ok && candidates <= SPFreshMaxRerankCandidates
}

// DefaultSPFreshVectorRuntimeConfig returns the complete default runtime configuration.
func DefaultSPFreshVectorRuntimeConfig() SPFreshVectorRuntimeConfig {
	return SPFreshVectorRuntimeConfig{
		HammingFactor:      96,
		Rabitq4Factor:      4,
		MaxFixupWorkers:    0,
		MaxFixups:          defaultSPFreshVectorMaxFixups,
		StalledOpTimeoutMS: 0,
		MaxInsertAttempts:  defaultSPFreshVectorMaxInsertAttempts,
	}
}

func parseSPFreshVectorParamItems(param string, apply func(key, value string) error) error {
	param = strings.TrimSpace(param)
	if param == "" {
		return nil
	}

	seen := make(map[string]struct{}, 4)
	for _, rawItem := range strings.Split(param, ",") {
		item := strings.TrimSpace(rawItem)
		if item == "" {
			return fmt.Errorf("VECTOR_INDEX_PARAM contains an empty item")
		}
		rawKey, rawValue, ok := strings.Cut(item, "=")
		if !ok {
			return fmt.Errorf("VECTOR_INDEX_PARAM item %q must use key=value syntax", item)
		}
		key := strings.ToLower(strings.TrimSpace(rawKey))
		value := strings.TrimSpace(rawValue)
		if key == "" || value == "" {
			return fmt.Errorf("VECTOR_INDEX_PARAM item %q must use non-empty key and value", item)
		}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate VECTOR_INDEX_PARAM key %q", key)
		}
		seen[key] = struct{}{}
		if err := apply(key, value); err != nil {
			return err
		}
	}
	return nil
}

func parsePositiveSPFreshVectorUintParam(key string, value string, max uint64) (uint64, error) {
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil || n == 0 || n > max {
		return 0, fmt.Errorf("%s must be an unsigned integer in [1, %d]", key, max)
	}
	return n, nil
}

func parseSPFreshVectorSearchConfigItem(cfg *SPFreshVectorSearchConfig, key string, value string) error {
	switch key {
	case spfreshVectorSearchParamSearchBeamSize:
		beamSize, err := parsePositiveSPFreshVectorUintParam(key, value, maxSPFreshVectorSearchBeamSize)
		if err != nil {
			return err
		}
		cfg.SearchBeamSize = uint32(beamSize)
	case spfreshVectorSearchParamOversampleFactor:
		oversampleFactor, err := strconv.ParseFloat(value, 64)
		if err != nil ||
			math.IsNaN(oversampleFactor) ||
			math.IsInf(oversampleFactor, 0) ||
			oversampleFactor < 1 ||
			oversampleFactor > maxSPFreshVectorSearchOversampleFactor {
			return fmt.Errorf("oversample_factor must be finite and in [1, %d]", maxSPFreshVectorSearchOversampleFactor)
		}
		cfg.OversampleFactor = oversampleFactor
	case spfreshVectorSearchParamReadOnly:
		switch value {
		case "true":
			readOnly := true
			cfg.ReadOnly = &readOnly
		case "false":
			readOnly := false
			cfg.ReadOnly = &readOnly
		default:
			return fmt.Errorf("read_only must be true or false")
		}
	default:
		return fmt.Errorf("unknown VECTOR_INDEX_PARAM key %q", key)
	}
	return nil
}

func parseSPFreshVectorRuntimeConfigItem(cfg *SPFreshVectorRuntimeConfig, key string, value string) error {
	switch key {
	case spfreshVectorRuntimeParamHammingFactor, spfreshVectorRuntimeParamRabitq4Factor:
		factor, err := parsePositiveSPFreshVectorUintParam(key, value, maxSPFreshVectorCandidateFactor)
		if err != nil {
			return err
		}
		if key == spfreshVectorRuntimeParamHammingFactor {
			cfg.HammingFactor = factor
		} else {
			cfg.Rabitq4Factor = factor
		}
	case spfreshVectorRuntimeParamMaxFixupWorkers:
		workers, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return fmt.Errorf("max_fixup_workers must be an unsigned integer")
		}
		cfg.MaxFixupWorkers = workers
	case spfreshVectorRuntimeParamMaxFixups:
		maxFixups, err := parsePositiveSPFreshVectorUintParam(key, value, maxSPFreshVectorFixups)
		if err != nil {
			return err
		}
		cfg.MaxFixups = maxFixups
	case spfreshVectorRuntimeParamStalledOpTimeoutMS:
		timeout, err := strconv.ParseUint(value, 10, 64)
		if err != nil || timeout > maxSPFreshVectorStalledOpTimeoutMS {
			return fmt.Errorf("stalled_op_timeout_ms must be an unsigned integer in [0, %d]", maxSPFreshVectorStalledOpTimeoutMS)
		}
		cfg.StalledOpTimeoutMS = timeout
	case spfreshVectorRuntimeParamMaxInsertAttempts:
		attempts, err := parsePositiveSPFreshVectorUintParam(key, value, maxSPFreshVectorInsertAttempts)
		if err != nil {
			return err
		}
		cfg.MaxInsertAttempts = attempts
	default:
		return fmt.Errorf("unknown VECTOR_INDEX_PARAM key %q", key)
	}
	return nil
}

func parseSPFreshVectorMutableConfigItem(
	searchConfig *SPFreshVectorSearchConfig,
	runtimeConfig *SPFreshVectorRuntimeConfig,
	key string,
	value string,
) error {
	switch key {
	case spfreshVectorSearchParamSearchBeamSize,
		spfreshVectorSearchParamOversampleFactor,
		spfreshVectorSearchParamReadOnly:
		return parseSPFreshVectorSearchConfigItem(searchConfig, key, value)
	case spfreshVectorRuntimeParamHammingFactor, spfreshVectorRuntimeParamRabitq4Factor,
		spfreshVectorRuntimeParamMaxFixupWorkers,
		spfreshVectorRuntimeParamMaxFixups,
		spfreshVectorRuntimeParamStalledOpTimeoutMS,
		spfreshVectorRuntimeParamMaxInsertAttempts:
		return parseSPFreshVectorRuntimeConfigItem(runtimeConfig, key, value)
	default:
		return fmt.Errorf("unknown VECTOR_INDEX_PARAM key %q", key)
	}
}

// ParseSPFreshVectorIndexParam parses create-time and search-time SPFresh
// VECTOR_INDEX_PARAM values.
func ParseSPFreshVectorIndexParam(param string) (*SPFreshVectorIndexOptions, *SPFreshVectorSearchConfig, SPFreshVectorRuntimeConfig, error) {
	indexOptions := &SPFreshVectorIndexOptions{}
	searchConfig := &SPFreshVectorSearchConfig{}
	runtimeConfig := DefaultSPFreshVectorRuntimeConfig()
	if err := parseSPFreshVectorParamItems(param, func(key, value string) error {
		switch key {
		case spfreshVectorIndexParamRotAlgorithm:
			switch value {
			case "none", "matrix", "givens":
				indexOptions.RotAlgorithm = value
			default:
				return fmt.Errorf("unknown rot_algorithm: %s", value)
			}
		case spfreshVectorIndexParamMinPartitionSize:
			size, err := parsePositiveSPFreshVectorUintParam(key, value, ^uint64(0))
			if err != nil {
				return err
			}
			indexOptions.MinPartitionSize = size
		case spfreshVectorIndexParamMaxPartitionSize:
			size, err := parsePositiveSPFreshVectorUintParam(key, value, ^uint64(0))
			if err != nil {
				return err
			}
			if size < 2 {
				return fmt.Errorf("max_partition_size must be >= 2")
			}
			indexOptions.MaxPartitionSize = size
		case spfreshVectorIndexParamBuildShard:
			shards, err := parsePositiveSPFreshVectorUintParam(key, value, uint64(SPFreshMaxBuildShards))
			if err != nil {
				return err
			}
			indexOptions.BuildShard = uint32(shards)
		case spfreshVectorIndexParamWriteBeamSize:
			size, err := parsePositiveSPFreshVectorUintParam(key, value, maxSPFreshVectorWriteBeamSize)
			if err != nil {
				return err
			}
			indexOptions.WriteBeamSize = size
		default:
			return parseSPFreshVectorMutableConfigItem(searchConfig, &runtimeConfig, key, value)
		}
		return nil
	}); err != nil {
		return nil, nil, SPFreshVectorRuntimeConfig{}, err
	}

	effectiveMinPartitionSize := indexOptions.MinPartitionSize
	if effectiveMinPartitionSize == 0 {
		effectiveMinPartitionSize = defaultSPFreshVectorMinPartitionSize
	}
	effectiveMaxPartitionSize := indexOptions.MaxPartitionSize
	if effectiveMaxPartitionSize == 0 {
		effectiveMaxPartitionSize = defaultSPFreshVectorMaxPartitionSize
	}
	if effectiveMinPartitionSize > effectiveMaxPartitionSize {
		return nil, nil, SPFreshVectorRuntimeConfig{}, fmt.Errorf("min_partition_size must be <= max_partition_size")
	}
	if indexOptions.IsEmpty() {
		indexOptions = nil
	}
	if searchConfig.IsEmpty() {
		searchConfig = nil
	}
	if err := runtimeConfig.validateCandidateFactors(); err != nil {
		return nil, nil, SPFreshVectorRuntimeConfig{}, err
	}
	return indexOptions, searchConfig, runtimeConfig, nil
}

// ParseSPFreshVectorMutableConfig parses ALTER-time Search and Runtime patches over a copy of runtimeConfig.
func ParseSPFreshVectorMutableConfig(param string, runtimeConfig SPFreshVectorRuntimeConfig) (*SPFreshVectorSearchConfig, SPFreshVectorRuntimeConfig, error) {
	if strings.TrimSpace(param) == "" {
		return &SPFreshVectorSearchConfig{}, DefaultSPFreshVectorRuntimeConfig(), nil
	}
	searchConfig := &SPFreshVectorSearchConfig{}
	if err := parseSPFreshVectorParamItems(param, func(key, value string) error {
		return parseSPFreshVectorMutableConfigItem(searchConfig, &runtimeConfig, key, value)
	}); err != nil {
		return nil, SPFreshVectorRuntimeConfig{}, err
	}
	if searchConfig.IsEmpty() {
		searchConfig = nil
	}
	if err := runtimeConfig.validateCandidateFactors(); err != nil {
		return nil, SPFreshVectorRuntimeConfig{}, err
	}
	return searchConfig, runtimeConfig, nil
}

// Clone returns a deep copy of cfg.
func (cfg *SPFreshVectorIndexOptions) Clone() *SPFreshVectorIndexOptions {
	if cfg == nil {
		return nil
	}
	cloned := *cfg
	return &cloned
}

// IsEmpty returns true when no SPFresh create-time index options are set.
func (cfg *SPFreshVectorIndexOptions) IsEmpty() bool {
	return cfg == nil ||
		(cfg.RotAlgorithm == "" &&
			cfg.MinPartitionSize == 0 &&
			cfg.MaxPartitionSize == 0 &&
			cfg.BuildShard == 0 &&
			cfg.WriteBeamSize == 0)
}

// Clone returns a deep copy of cfg.
func (cfg *SPFreshVectorSearchConfig) Clone() *SPFreshVectorSearchConfig {
	if cfg == nil {
		return nil
	}
	cloned := *cfg
	if cfg.ReadOnly != nil {
		readOnly := *cfg.ReadOnly
		cloned.ReadOnly = &readOnly
	}
	return &cloned
}

// IsEmpty returns true when no SPFresh search config fields are set.
func (cfg *SPFreshVectorSearchConfig) IsEmpty() bool {
	return cfg == nil ||
		(cfg.SearchBeamSize == 0 &&
			cfg.OversampleFactor == 0 &&
			cfg.ReadOnly == nil)
}

// Apply merges patch over cfg. An empty patch clears the search configuration.
func (cfg *SPFreshVectorSearchConfig) Apply(patch *SPFreshVectorSearchConfig) *SPFreshVectorSearchConfig {
	if patch == nil {
		return cfg.Clone()
	}
	if patch.IsEmpty() {
		return nil
	}
	merged := cfg.Clone()
	if merged == nil {
		merged = &SPFreshVectorSearchConfig{}
	}
	if patch.SearchBeamSize != 0 {
		merged.SearchBeamSize = patch.SearchBeamSize
	}
	if patch.OversampleFactor != 0 {
		merged.OversampleFactor = patch.OversampleFactor
	}
	if patch.ReadOnly != nil {
		readOnly := *patch.ReadOnly
		merged.ReadOnly = &readOnly
	}
	return merged
}

func (cfg SPFreshVectorRuntimeConfig) validateCandidateFactors() error {
	if cfg.Rabitq4Factor < 1 || cfg.HammingFactor < cfg.Rabitq4Factor || cfg.HammingFactor > maxSPFreshVectorCandidateFactor {
		return fmt.Errorf("candidate factors must satisfy 1 <= rabitq4_factor <= hamming_factor <= %d", maxSPFreshVectorCandidateFactor)
	}
	return nil
}

// Clone returns a copy of cfg.
func (cfg *SPFreshVectorRuntimeConfig) Clone() *SPFreshVectorRuntimeConfig {
	if cfg == nil {
		return nil
	}
	cloned := *cfg
	return &cloned
}

// FormatSPFreshVectorIndexParam returns the canonical VECTOR_INDEX_PARAM string.
func FormatSPFreshVectorIndexParam(indexOptions *SPFreshVectorIndexOptions, searchConfig *SPFreshVectorSearchConfig, runtimeConfig *SPFreshVectorRuntimeConfig) string {
	if indexOptions.IsEmpty() && searchConfig.IsEmpty() && runtimeConfig == nil {
		return ""
	}
	parts := make([]string, 0, 12)
	if !indexOptions.IsEmpty() {
		if indexOptions.RotAlgorithm != "" {
			parts = append(parts, fmt.Sprintf("%s=%s", spfreshVectorIndexParamRotAlgorithm, indexOptions.RotAlgorithm))
		}
		if indexOptions.MinPartitionSize != 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", spfreshVectorIndexParamMinPartitionSize, indexOptions.MinPartitionSize))
		}
		if indexOptions.MaxPartitionSize != 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", spfreshVectorIndexParamMaxPartitionSize, indexOptions.MaxPartitionSize))
		}
		if indexOptions.BuildShard != 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", spfreshVectorIndexParamBuildShard, indexOptions.BuildShard))
		}
		if indexOptions.WriteBeamSize != 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", spfreshVectorIndexParamWriteBeamSize, indexOptions.WriteBeamSize))
		}
	}
	if !searchConfig.IsEmpty() {
		if searchConfig.SearchBeamSize != 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", spfreshVectorSearchParamSearchBeamSize, searchConfig.SearchBeamSize))
		}
		if searchConfig.OversampleFactor != 0 {
			parts = append(parts, fmt.Sprintf("%s=%s",
				spfreshVectorSearchParamOversampleFactor,
				strconv.FormatFloat(searchConfig.OversampleFactor, 'f', -1, 64)))
		}
		if searchConfig.ReadOnly != nil {
			parts = append(parts, fmt.Sprintf("%s=%t", spfreshVectorSearchParamReadOnly, *searchConfig.ReadOnly))
		}
	}
	if runtimeConfig != nil && (runtimeConfig.MaxFixupWorkers != 0 || runtimeConfig.MaxFixups != defaultSPFreshVectorMaxFixups || runtimeConfig.StalledOpTimeoutMS != 0 || runtimeConfig.MaxInsertAttempts != defaultSPFreshVectorMaxInsertAttempts) {
		parts = append(parts,
			fmt.Sprintf("%s=%d", spfreshVectorRuntimeParamMaxFixupWorkers, runtimeConfig.MaxFixupWorkers),
			fmt.Sprintf("%s=%d", spfreshVectorRuntimeParamMaxFixups, runtimeConfig.MaxFixups),
			fmt.Sprintf("%s=%d", spfreshVectorRuntimeParamStalledOpTimeoutMS, runtimeConfig.StalledOpTimeoutMS),
			fmt.Sprintf("%s=%d", spfreshVectorRuntimeParamMaxInsertAttempts, runtimeConfig.MaxInsertAttempts),
		)
	}
	if runtimeConfig != nil {
		defaults := DefaultSPFreshVectorRuntimeConfig()
		if runtimeConfig.HammingFactor != defaults.HammingFactor {
			parts = append(parts, fmt.Sprintf("%s=%d", spfreshVectorRuntimeParamHammingFactor, runtimeConfig.HammingFactor))
		}
		if runtimeConfig.Rabitq4Factor != defaults.Rabitq4Factor {
			parts = append(parts, fmt.Sprintf("%s=%d", spfreshVectorRuntimeParamRabitq4Factor, runtimeConfig.Rabitq4Factor))
		}
	}
	return strings.Join(parts, ",")
}
