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

package infoschema

const (
	// maxMapLevel is the maximum number of layers in the layeredMap.
	// it's a good balance point between cost of copying map elements, and overhead
	// of 'get' operation.
	maxMapLevel = 8
	// compactThreshold is the threshold to trigger compaction for the top layer
	// if there are read-only layers below it.
	// this value is close to the optimal for maxMapLevel = 8.
	compactThreshold = 16
	initialMapCap    = 16
)

type itemT[V any] struct {
	v         V
	tombstone bool
}

// layeredMap is a copy-on-write map with multiple layers. it's similar to an LSM tree.
// infoschema builder uses a copy-on-write strategy to build new schema info from
// the old schema info. in order to avoid copying the whole map on every modification,
// we use this layered map to store the schema information. the builder must call
// forCOW to get a new map for writing.
// to support delete without copying the whole map, we use tombstone to mark the deleted.
type layeredMap[K comparable, V any] struct {
	// layers is the map layers inside this map, top layer is the last element.
	// items in top layer shadows the items in the bottom layer.
	// for a newly created map or after full compaction, we only have one layer which
	// is the top layer.
	layers []map[K]itemT[V]
	// topLayer is the top writable layer, it's the last element in layers, and all
	// layers below it are read-only, might be shared and accessed concurrently,
	// so we need copy-on-write.
	topLayer         map[K]itemT[V]
	maxLevel         int
	compactThreshold int
}

func newLayeredMap[K comparable, V any]() *layeredMap[K, V] {
	return newLayeredMap0[K, V](maxMapLevel, compactThreshold)
}

func newLayeredMapFrom[K comparable, V any](m map[K]V) *layeredMap[K, V] {
	topLayer := make(map[K]itemT[V], len(m))
	for k, v := range m {
		topLayer[k] = itemT[V]{v: v}
	}
	lm := newLayeredMap[K, V]()
	lm.layers[0] = topLayer
	lm.topLayer = topLayer
	return lm
}

func newLayeredMap0[K comparable, V any](maxLevel, compactThreshold int) *layeredMap[K, V] {
	m := &layeredMap[K, V]{
		layers: make([]map[K]itemT[V], 0, maxLevel),
	}
	m.topLayer = make(map[K]itemT[V], initialMapCap)
	m.layers = append(m.layers, m.topLayer)
	m.maxLevel = maxLevel
	m.compactThreshold = compactThreshold
	return m
}

// get returns the value for the key, and a bool indicates whether the key exists.
// we search the layers from top to bottom, return the first value found.
// i.e. the value in the top layer shadows the value in the bottom layer.
func (m *layeredMap[K, V]) get(key K) (V, bool) {
	var zero V
	for i := len(m.layers) - 1; i >= 0; i-- {
		item, ok := m.layers[i][key]
		if ok {
			if item.tombstone {
				return zero, false
			}
			return item.v, true
		}
	}
	return zero, false
}

func (m *layeredMap[K, V]) getVal(key K) V {
	v, _ := m.get(key)
	return v
}

func (m *layeredMap[K, V]) estimatedLen() int {
	var count int
	for _, layer := range m.layers {
		count += len(layer)
	}
	return count
}

func (m *layeredMap[K, V]) empty() bool {
	empty := true
	m.scan(func(_ K, _ V) bool {
		empty = false
		return false
	})
	return empty
}

// scan scans all elements in the map, from top layer to bottom layer.
// if fn returns false, the scan will be stopped.
func (m *layeredMap[K, V]) scan(fn func(K, V) bool) {
	layerCnt := len(m.layers)
	if layerCnt == 1 {
		for k, item := range m.layers[0] {
			if item.tombstone {
				continue
			}
			if !fn(k, item.v) {
				return
			}
		}
		return
	}

	// keys is used to avoid duplicate scan.
	keys := make(map[K]struct{}, m.estimatedLen())
	for i := layerCnt - 1; i >= 0; i-- {
		l := m.layers[i]
		for k, item := range l {
			if _, ok := keys[k]; ok {
				continue
			}
			keys[k] = struct{}{}
			if item.tombstone {
				continue
			}
			if !fn(k, item.v) {
				return
			}
		}
	}
}

func (m *layeredMap[K, V]) forCOW() *layeredMap[K, V] {
	var newTopLayer map[K]itemT[V]
	newLayers := make([]map[K]itemT[V], 0, m.maxLevel)
	for _, layer := range m.layers {
		newLayers = append(newLayers, layer)
	}
	topLayer := newLayers[len(newLayers)-1]
	if len(topLayer) < m.compactThreshold || len(newLayers) >= m.maxLevel {
		newTopLayer = make(map[K]itemT[V], len(topLayer))
		for k, v := range topLayer {
			newTopLayer[k] = v
		}
		newLayers[len(newLayers)-1] = newTopLayer
	} else {
		newTopLayer = make(map[K]itemT[V], initialMapCap)
		newLayers = append(newLayers, newTopLayer)
	}
	return &layeredMap[K, V]{
		layers:           newLayers,
		topLayer:         newTopLayer,
		maxLevel:         m.maxLevel,
		compactThreshold: m.compactThreshold,
	}
}

func (m *layeredMap[K, V]) add(key K, value V) {
	m.add0(key, value, false)
}

func (m *layeredMap[K, V]) del(key K) {
	if len(m.layers) == 1 {
		// happens after full compaction or a new created map, just delete from top layer.
		delete(m.topLayer, key)
		return
	}
	var zero V
	m.add0(key, zero, true)
}

func (m *layeredMap[K, V]) add0(key K, value V, tombstone bool) {
	if len(m.topLayer) >= m.compactThreshold {
		if len(m.layers) >= m.maxLevel {
			m.compact(false)
		}
		// append a new layer even after compaction to avoid map capacity growth
		// automatically by golang.
		m.topLayer = make(map[K]itemT[V], initialMapCap)
		m.layers = append(m.layers, m.topLayer)
	}
	m.topLayer[key] = itemT[V]{v: value, tombstone: tombstone}
}

// compact compacting into the lowest layer with its item count <= sum of above layers,
// to avoid copying too many elements. after compaction, len(layers[i]) < len(layers[i+1]).
func (m *layeredMap[K, V]) compact(full bool) {
	if len(m.layers) <= 1 {
		return
	}
	// sums[i] is the sum of the number of elements in layers[i:]
	sums := make([]int, len(m.layers))
	for i := len(m.layers) - 1; i >= 0; i-- {
		if i == len(m.layers)-1 {
			sums[i] = len(m.layers[i])
			continue
		}
		sums[i] = sums[i+1] + len(m.layers[i])
	}
	var compactDstLayerIdx int
	if !full {
		compactDstLayerIdx = len(m.layers) - 2
		for i := 0; i < len(m.layers)-1; i++ {
			if len(m.layers[i]) <= sums[i+1] {
				compactDstLayerIdx = i
				break
			}
		}
		if compactDstLayerIdx == 0 {
			full = true
		}
	}
	// merge all layers from compactDstLayerIdx to the top layer into a new map
	mergedLayer := make(map[K]itemT[V], sums[compactDstLayerIdx])
	for i := compactDstLayerIdx; i < len(m.layers); i++ {
		for k, v := range m.layers[i] {
			if full && v.tombstone {
				delete(mergedLayer, k)
				continue
			}
			mergedLayer[k] = v
		}
	}
	m.layers = m.layers[:compactDstLayerIdx]
	m.layers = append(m.layers, mergedLayer)
	m.topLayer = mergedLayer
}
