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

package testutil

import (
	"bytes"
	"fmt"
	"strconv"
)

// RegionLayoutOption appends or transforms boundaries while preserving a valid layout.
type RegionLayoutOption func([]RegionBoundary) ([]RegionBoundary, error)

// BuildRegionLayout applies options in order and returns a validated region layout.
func BuildRegionLayout(opts ...RegionLayoutOption) ([]RegionBoundary, error) {
	boundaries := make([]RegionBoundary, 0)
	var err error
	for _, opt := range opts {
		boundaries, err = opt(boundaries)
		if err != nil {
			return nil, err
		}
	}
	if len(boundaries) == 0 {
		return nil, fmt.Errorf("region layout must contain at least one region")
	}
	if len(boundaries[len(boundaries)-1].EndKey) != 0 {
		return nil, fmt.Errorf("last region must end with empty key")
	}
	return cloneRegionBoundaries(boundaries), nil
}

// StoreIDRange returns continuous store IDs: [start, start+count).
func StoreIDRange(start uint64, count int) []uint64 {
	if count <= 0 {
		return nil
	}
	ids := make([]uint64, 0, count)
	for i := range count {
		ids = append(ids, start+uint64(i))
	}
	return ids
}

// AddRegion appends one continuous region to the current layout.
func AddRegion(startKey, endKey string, storeID uint64) RegionLayoutOption {
	return func(boundaries []RegionBoundary) ([]RegionBoundary, error) {
		return appendRegion(boundaries, startKey, endKey, storeID)
	}
}

// AddRegionsBySplitKeys appends continuous regions split by splitKeys.
// It auto-assigns store IDs in round-robin order and closes with an empty end key.
func AddRegionsBySplitKeys(splitKeys []string, storeIDs ...uint64) RegionLayoutOption {
	return func(boundaries []RegionBoundary) ([]RegionBoundary, error) {
		if len(storeIDs) == 0 {
			return nil, fmt.Errorf("store ids must not be empty")
		}

		startKey := ""
		storeIndex := 0
		result := cloneRegionBoundaries(boundaries)
		if len(result) > 0 {
			last := result[len(result)-1]
			if len(last.EndKey) == 0 {
				return nil, fmt.Errorf("cannot append regions after layout is already closed")
			}
			startKey = string(last.EndKey)
			storeIndex = len(result) % len(storeIDs)
		}

		var err error
		for _, splitKey := range splitKeys {
			result, err = appendRegion(result, startKey, splitKey, storeIDs[storeIndex%len(storeIDs)])
			if err != nil {
				return nil, err
			}
			startKey = splitKey
			storeIndex++
		}
		return appendRegion(result, startKey, "", storeIDs[storeIndex%len(storeIDs)])
	}
}

// AddRoundRobinRegions appends regionCount continuous regions with generated split keys.
// Generated keys are k01, k02 ...; stores are assigned round-robin.
func AddRoundRobinRegions(regionCount int, storeIDs ...uint64) RegionLayoutOption {
	return func(boundaries []RegionBoundary) ([]RegionBoundary, error) {
		if regionCount <= 0 {
			return nil, fmt.Errorf("region count must be greater than zero")
		}
		if len(storeIDs) == 0 {
			return nil, fmt.Errorf("store ids must not be empty")
		}
		if len(boundaries) > 0 {
			return nil, fmt.Errorf("AddRoundRobinRegions must start from an empty layout")
		}
		if regionCount == 1 {
			return appendRegion(nil, "", "", storeIDs[0])
		}

		width := len(strconv.Itoa(regionCount - 1))
		if width < 2 {
			width = 2
		}
		splitKeys := make([]string, 0, regionCount-1)
		for i := 1; i < regionCount; i++ {
			splitKeys = append(splitKeys, fmt.Sprintf("k%0*d", width, i))
		}
		return AddRegionsBySplitKeys(splitKeys, storeIDs...)(boundaries)
	}
}

func appendRegion(boundaries []RegionBoundary, startKey, endKey string, storeID uint64) ([]RegionBoundary, error) {
	if storeID == 0 {
		return nil, fmt.Errorf("store id must not be zero")
	}

	start := []byte(startKey)
	end := []byte(endKey)
	if len(boundaries) == 0 {
		if len(start) != 0 {
			return nil, fmt.Errorf("first region must start from empty key")
		}
	} else {
		prev := boundaries[len(boundaries)-1]
		if !bytes.Equal(prev.EndKey, start) {
			return nil, fmt.Errorf("region boundary is not continuous: prev end %q, next start %q", prev.EndKey, start)
		}
	}
	if len(end) != 0 && bytes.Compare(start, end) >= 0 {
		return nil, fmt.Errorf("invalid region range [%q, %q)", start, end)
	}

	result := cloneRegionBoundaries(boundaries)
	result = append(result, RegionBoundary{
		StartKey: bytes.Clone(start),
		EndKey:   bytes.Clone(end),
		StoreID:  storeID,
	})
	return result, nil
}

func cloneRegionBoundaries(boundaries []RegionBoundary) []RegionBoundary {
	result := make([]RegionBoundary, 0, len(boundaries))
	for _, boundary := range boundaries {
		result = append(result, RegionBoundary{
			StartKey: bytes.Clone(boundary.StartKey),
			EndKey:   bytes.Clone(boundary.EndKey),
			StoreID:  boundary.StoreID,
		})
	}
	return result
}
