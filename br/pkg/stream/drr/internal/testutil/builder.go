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

// RegionLayoutBuilder incrementally builds region boundaries for DRR test cases.
type RegionLayoutBuilder struct {
	boundaries []RegionBoundary
	err        error
}

func NewRegionLayoutBuilder() *RegionLayoutBuilder {
	return &RegionLayoutBuilder{}
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

func (b *RegionLayoutBuilder) AddRegion(startKey, endKey string, storeID uint64) *RegionLayoutBuilder {
	if b.err != nil {
		return b
	}
	if storeID == 0 {
		b.err = fmt.Errorf("store id must not be zero")
		return b
	}

	start := []byte(startKey)
	end := []byte(endKey)
	if len(b.boundaries) == 0 {
		if len(start) != 0 {
			b.err = fmt.Errorf("first region must start from empty key")
			return b
		}
	} else {
		prev := b.boundaries[len(b.boundaries)-1]
		if !bytes.Equal(prev.EndKey, start) {
			b.err = fmt.Errorf("region boundary is not continuous: prev end %q, next start %q", prev.EndKey, start)
			return b
		}
	}
	if len(end) != 0 && bytes.Compare(start, end) >= 0 {
		b.err = fmt.Errorf("invalid region range [%q, %q)", start, end)
		return b
	}

	b.boundaries = append(b.boundaries, RegionBoundary{
		StartKey: bytes.Clone(start),
		EndKey:   bytes.Clone(end),
		StoreID:  storeID,
	})
	return b
}

// AddRegionsBySplitKeys appends continuous regions split by splitKeys.
// It auto-assigns store IDs in round-robin order and closes with an empty end key.
func (b *RegionLayoutBuilder) AddRegionsBySplitKeys(splitKeys []string, storeIDs ...uint64) *RegionLayoutBuilder {
	if b.err != nil {
		return b
	}
	if len(storeIDs) == 0 {
		b.err = fmt.Errorf("store ids must not be empty")
		return b
	}

	startKey := ""
	storeIndex := 0
	if len(b.boundaries) > 0 {
		last := b.boundaries[len(b.boundaries)-1]
		if len(last.EndKey) == 0 {
			b.err = fmt.Errorf("cannot append regions after layout is already closed")
			return b
		}
		startKey = string(last.EndKey)
		storeIndex = len(b.boundaries) % len(storeIDs)
	}

	for _, splitKey := range splitKeys {
		b.AddRegion(startKey, splitKey, storeIDs[storeIndex%len(storeIDs)])
		if b.err != nil {
			return b
		}
		startKey = splitKey
		storeIndex++
	}
	b.AddRegion(startKey, "", storeIDs[storeIndex%len(storeIDs)])
	return b
}

// AddRoundRobinRegions appends regionCount continuous regions with generated split keys.
// Generated keys are k01, k02 ...; stores are assigned round-robin.
func (b *RegionLayoutBuilder) AddRoundRobinRegions(regionCount int, storeIDs ...uint64) *RegionLayoutBuilder {
	if b.err != nil {
		return b
	}
	if regionCount <= 0 {
		b.err = fmt.Errorf("region count must be greater than zero")
		return b
	}
	if len(storeIDs) == 0 {
		b.err = fmt.Errorf("store ids must not be empty")
		return b
	}
	if len(b.boundaries) > 0 {
		b.err = fmt.Errorf("AddRoundRobinRegions must start from an empty builder")
		return b
	}
	if regionCount == 1 {
		return b.AddRegion("", "", storeIDs[0])
	}

	width := len(strconv.Itoa(regionCount - 1))
	if width < 2 {
		width = 2
	}
	splitKeys := make([]string, 0, regionCount-1)
	for i := 1; i < regionCount; i++ {
		splitKeys = append(splitKeys, fmt.Sprintf("k%0*d", width, i))
	}
	return b.AddRegionsBySplitKeys(splitKeys, storeIDs...)
}

func (b *RegionLayoutBuilder) Build() ([]RegionBoundary, error) {
	if b.err != nil {
		return nil, b.err
	}
	if len(b.boundaries) == 0 {
		return nil, fmt.Errorf("region layout must contain at least one region")
	}
	if len(b.boundaries[len(b.boundaries)-1].EndKey) != 0 {
		return nil, fmt.Errorf("last region must end with empty key")
	}

	result := make([]RegionBoundary, 0, len(b.boundaries))
	for _, boundary := range b.boundaries {
		result = append(result, RegionBoundary{
			StartKey: bytes.Clone(boundary.StartKey),
			EndKey:   bytes.Clone(boundary.EndKey),
			StoreID:  boundary.StoreID,
		})
	}
	return result, nil
}
