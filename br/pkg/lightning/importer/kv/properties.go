// Copyright 2022 PingCAP, Inc.
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

package kv

import (
	"bytes"
	"encoding/json"
)

type Properties struct {
	Range      KeyRange        `json:"range"`
	TotalSize  int64           `json:"totalSize"`
	TotalKeys  int             `json:"totalKeys"`
	RangeProps RangeProperties `json:"rangeProps"`
}

func (p Properties) String() string {
	buf, _ := json.Marshal(&p)
	return string(buf)
}

type RangeProperties struct {
	Offsets []RangeOffset `json:"offsets"`
}

type RangeOffset struct {
	Key  []byte `json:"key"`
	Size int64  `json:"size"`
	Keys int    `json:"keys"`
}

func MergeProperties(props ...Properties) Properties {
	if len(props) == 0 {
		return Properties{}
	}
	if len(props) == 1 {
		return props[0]
	}
	result := props[0]
	for i := 1; i < len(props); i++ {
		result.Range = result.Range.Union(props[i].Range)
		result.TotalSize += props[i].TotalSize
		result.TotalKeys += props[i].TotalKeys
		result.RangeProps = mergeTwoRangeProps(result.RangeProps, props[i].RangeProps)
	}
	return result
}

func mergeTwoRangeProps(rp1, rp2 RangeProperties) RangeProperties {
	var (
		leftSize, rightSize int64
		leftKeys, rightKeys int
	)
	var result RangeProperties
	leftLen, rightLen := len(rp1.Offsets), len(rp2.Offsets)
	i, j := 0, 0
	for i < leftLen || j < rightLen {
		var offset RangeOffset
		if i < leftLen && (j == rightLen || bytes.Compare(rp1.Offsets[i].Key, rp2.Offsets[j].Key) <= 0) {
			leftSize = rp1.Offsets[i].Size
			leftKeys = rp1.Offsets[i].Keys
			offset.Key = rp1.Offsets[i].Key
			offset.Size += rightSize
			offset.Keys += rightKeys
			i++
		} else {
			rightSize = rp2.Offsets[j].Size
			rightKeys = rp2.Offsets[j].Keys
			offset.Key = rp2.Offsets[j].Key
			offset.Size += leftSize
			offset.Keys += leftKeys
			j++
		}
		if len(result.Offsets) > 0 && bytes.Equal(result.Offsets[len(result.Offsets)-1].Key, offset.Key) {
			result.Offsets[len(result.Offsets)-1] = offset
		} else {
			result.Offsets = append(result.Offsets, offset)
		}
	}
	return result
}

type PropertiesCollector struct {
	w Writer

	firstKey          []byte
	lastKey           []byte
	totalSize         int64
	totalKeys         int
	lastSize          int64
	lastKeys          int
	rangeProps        RangeProperties
	sizeIndexDistance int64
	keysIndexDistance int
}

func NewPropertiesCollector(
	w Writer,
	sizeIndexDistance int64,
	keysIndexDistance int,
) *PropertiesCollector {
	return &PropertiesCollector{
		w:                 w,
		sizeIndexDistance: sizeIndexDistance,
		keysIndexDistance: keysIndexDistance,
	}
}

func (pc *PropertiesCollector) Write(key, val []byte) error {
	if err := pc.w.Write(key, val); err != nil {
		return err
	}
	if len(pc.firstKey) == 0 {
		pc.firstKey = append(pc.firstKey, key...)
	}
	pc.lastKey = append(pc.lastKey[:0], key...)
	pc.totalSize += int64(len(key) + len(val))
	pc.totalKeys++
	if pc.totalSize-pc.lastSize >= pc.sizeIndexDistance ||
		pc.totalKeys-pc.lastKeys >= pc.keysIndexDistance {
		pc.rangeProps.Offsets = append(pc.rangeProps.Offsets, RangeOffset{
			Key:  append([]byte{}, key...),
			Size: pc.totalSize,
			Keys: pc.totalKeys,
		})
		pc.lastSize = pc.totalSize
		pc.lastKeys = pc.totalKeys
	}
	return nil
}

func (pc *PropertiesCollector) Close() error {
	return pc.w.Close()
}

func (pc *PropertiesCollector) Properties() Properties {
	return Properties{
		Range:      KeyRange{StartKey: pc.firstKey, EndKey: nextKey(pc.lastKey)},
		TotalSize:  pc.totalSize,
		TotalKeys:  pc.totalKeys,
		RangeProps: pc.rangeProps,
	}
}
