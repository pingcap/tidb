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

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func layeredROMapLen(m *layeredMap[string, string]) int {
	count := 0
	m.scan(func(_, _ string) bool { count++; return true })
	return count
}

func BenchmarkLayeredMapGet(b *testing.B) {
	mapItemCounts := []int{1024, 50000, 100000, 300000, 1 << 20}
	for _, mapItemCount := range mapItemCounts {
		m1 := newLayeredMap0[string, string](initialMapCap, 1, 1)
		for i := 0; i < mapItemCount; i++ {
			m1.add(fmt.Sprintf("%d", i), fmt.Sprintf("%d", i))
		}
		m4 := newLayeredMap0[string, string](initialMapCap, 4, 192)
		for i := 0; i < mapItemCount; i++ {
			m4 = m4.forCOW()
			str := fmt.Sprintf("%d", i)
			m4.add(str, str)
		}
		m8 := newLayeredMap0[string, string](initialMapCap, 8, 16)
		for i := 0; i < mapItemCount; i++ {
			m8 = m8.forCOW()
			str := fmt.Sprintf("%d", i)
			m8.add(str, str)
		}
		maps := []*layeredMap[string, string]{m1, m4, m8}
		for _, m := range maps {
			counts := make([]int, 0, len(m.layers))
			for _, l := range m.layers {
				counts = append(counts, len(l))
			}
			fmt.Printf("%7d/%2d/%3d: counts: %v\n", mapItemCount, m.maxLevel, m.compactThreshold, counts)
		}
		seed := time.Now().UnixNano()
		b.Logf("seed: %d", seed)
		for _, m := range maps {
			rnd := rand.New(rand.NewSource(seed))
			b.Run(fmt.Sprintf("%7d/%2d/%3d", mapItemCount, m.maxLevel, m.compactThreshold), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					m.get(fmt.Sprintf("%d", rnd.Int()))
				}
			})
		}
	}
}

func TestLayeredROMapGet(t *testing.T) {
	layerCounts := []int{5, 6, 8}
	thresholds := [][]int{
		{256},
		{128, 256},
		{32, 64},
		//{64, 128, 192, 256, 512},
		//{32, 64, 96, 128, 156},
		//{16, 32, 64},
		//{4, 8, 16},
	}
	for i, layerCount := range layerCounts {
		for _, threshold := range thresholds[i] {
			totalCopy = 0
			start := time.Now()
			m := newLayeredMap0[string, string](initialMapCap, layerCount, threshold)
			for i := 0; i < 1<<20; i++ {
				m = m.forCOW()
				str := fmt.Sprintf("%d", i)
				m.add(str, str)
			}
			fmt.Printf("%2d/%3d, totalCopy: %9d, cost: %v\n", layerCount, threshold, totalCopy, time.Since(start))
		}
	}
}

//func TestLayeredMap(t *testing.T) {
//	mapEquals := func(expect map[string]string, m *layeredMap[string, string]) {
//		require.Equal(t, len(expect), layeredROMapLen(m))
//		for k, v := range expect {
//			v1, ok := m.get(k)
//			require.True(t, ok)
//			require.Equal(t, v, v1)
//		}
//	}
//	layersEquals := func(expect []map[string]string, m *layeredMap[string, string]) {
//		require.Len(t, m.layers, len(expect))
//		for i, layer := range expect {
//			require.Equal(t, layer, m.layers[i])
//		}
//	}
//	m := newLayeredMap[string, string]()
//	// add 'a'
//	m.add("a", "1")
//	im := m.immutable()
//	require.Len(t, im.layers, 1)
//	mapEquals(map[string]string{"a": "1"}, im)
//	// add 'b'
//	m = im.mutable()
//	m.add("b", "2")
//	im = m.immutable()
//	require.Len(t, im.layers, 2)
//	mapEquals(map[string]string{"a": "1", "b": "2"}, im)
//	layersEquals([]map[string]string{{"a": "1"}, {"b": "2"}}, im)
//	// add 'a' again
//	m = im.mutable()
//	m.add("a", "3")
//	im = m.immutable()
//	require.Len(t, im.layers, 3)
//	mapEquals(map[string]string{"a": "3", "b": "2"}, im)
//	layersEquals([]map[string]string{{"a": "1"}, {"b": "2"}, {"a": "3"}}, im)
//	// del 'a'
//	m = im.mutable()
//	m.del("a")
//	im = m.immutable()
//	// empty topLayer layer, so the number of layers is not changed.
//	require.Len(t, im.layers, 2)
//	mapEquals(map[string]string{"b": "2"}, im)
//	layersEquals([]map[string]string{{}, {"b": "2"}}, im)
//	// del 'b'
//	m = im.mutable()
//	m.del("b")
//	im = m.immutable()
//	require.Len(t, im.layers, 0)
//	// add 'c', 'd', 'e', 'f'
//	strings := []string{"c", "4", "d", "5", "e", "6", "f", "7"}
//	for i := 0; i < len(strings); i += 2 {
//		m = im.mutable()
//		m.add(strings[i], strings[i+1])
//		im = m.immutable()
//	}
//	require.Len(t, im.layers, 4)
//	mapEquals(map[string]string{"c": "4", "d": "5", "e": "6", "f": "7"}, im)
//	layersEquals([]map[string]string{{"c": "4"}, {"d": "5"}, {"e": "6"}, {"f": "7"}}, im)
//	// add 'g'
//	topLayer := im.layers[3]
//	m = im.mutable()
//	m.add("g", "8")
//	im = m.immutable()
//	require.Len(t, im.layers, 4)
//	layersEquals([]map[string]string{{"c": "4"}, {"d": "5"}, {"e": "6"}, {"f": "7", "g": "8"}}, im)
//	require.NotContains(t, topLayer, "g")
//}
