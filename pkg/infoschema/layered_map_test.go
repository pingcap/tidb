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
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func layeredROMapLen(m *layeredMap[string, string]) int {
	count := 0
	m.scan(func(_, _ string) bool { count++; return true })
	return count
}

func BenchmarkLayeredMapGet(b *testing.B) {
	mapItemCounts := []int{1024, 50000, 100000, 300000, 1 << 20}
	for _, mapItemCount := range mapItemCounts {
		// this is same as a normal map
		m1 := newLayeredMap0[string, string](1, math.MaxInt)
		for i := 0; i < mapItemCount; i++ {
			m1.add(fmt.Sprintf("%d", i), fmt.Sprintf("%d", i))
		}
		m4 := newLayeredMap0[string, string](4, 192)
		for i := 0; i < mapItemCount; i++ {
			m4 = m4.forCOW()
			str := fmt.Sprintf("%d", i)
			m4.add(str, str)
		}
		m8 := newLayeredMap0[string, string](8, 16)
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

//func TestLayeredROMapGet(t *testing.T) {
//	layerCounts := []int{8}
//	thresholds := [][]int{
//		//{256},
//		//{128, 256},
//		//{32, 64},
//		//{64, 128, 192, 256, 512},
//		//{32, 64, 96, 128, 156},
//		{8, 16, 32, 64},
//		//{4, 8, 16},
//	}
//	for i, layerCount := range layerCounts {
//		for _, threshold := range thresholds[i] {
//			totalCopy = 0
//			start := time.Now()
//			m := newLayeredMap0[string, string](initialMapCap, layerCount, threshold)
//			for i := 0; i < 1<<20; i++ {
//				m = m.forCOW()
//				str := fmt.Sprintf("%d", i)
//				m.add(str, str)
//			}
//			fmt.Printf("%2d/%3d, totalCopy: %9d, cost: %v\n", layerCount, threshold, totalCopy, time.Since(start))
//		}
//	}
//}

func TestLayeredMap(t *testing.T) {
	mapEquals := func(expect map[string]string, m *layeredMap[string, string]) {
		require.Equal(t, len(expect), layeredROMapLen(m))
		for k, v := range expect {
			v1, ok := m.get(k)
			require.True(t, ok)
			require.Equal(t, v, v1)
		}
	}
	layerLenEquals := func(m *layeredMap[string, string], lens []int) {
		require.Equal(t, len(m.layers), len(lens))
		for i := 0; i < len(m.layers); i++ {
			require.Equal(t, lens[i], len(m.layers[i]))
		}
	}
	prepareMaps := func(count int) (map[string]string, *layeredMap[string, string]) {
		m := newLayeredMap[string, string]()
		gomap := make(map[string]string, count)
		for i := 0; i < count; i++ {
			kv := fmt.Sprintf("%d", i)
			m.add(kv, kv)
			gomap[kv] = kv
		}
		require.Len(t, gomap, count)
		mapEquals(gomap, m)
		return gomap, m
	}

	t.Run("scan", func(t *testing.T) {
		// empty map
		m := newLayeredMap[string, string]()
		count := 0
		countFn := func(_, _ string) bool {
			count++
			return true
		}
		m.scan(countFn)
		require.Zero(t, count)
		// map with 1 layer
		_, m = prepareMaps(100)
		m.compact(true)
		count = 0
		m.scan(countFn)
		require.Equal(t, 100, count)
		// map with multiple layers
		for i := 0; i < 20; i++ {
			if i%10 == 1 { // 1, 11
				kv := fmt.Sprintf("%d", i)
				m.del(kv)
			}
			kv := fmt.Sprintf("%d", i+100)
			m.add(kv, kv)
		}
		layerLenEquals(m, []int{100, 16, 6})
		newgomap := make(map[string]string, 100)
		m.scan(func(k, v string) bool {
			newgomap[k] = v
			return true
		})
		require.Len(t, newgomap, 118)
		require.NotContains(t, newgomap, "1")
		require.NotContains(t, newgomap, "11")
		mapEquals(newgomap, m)
	})

	t.Run("newLayeredMapFrom", func(t *testing.T) {
		gomap, _ := prepareMaps(100)
		m := newLayeredMapFrom[string, string](gomap)
		require.Len(t, m.layers, 1)
		mapEquals(gomap, m)
	})

	t.Run("getVal-empty", func(t *testing.T) {
		m := newLayeredMap[string, string]()
		require.True(t, m.empty())
		require.Equal(t, "", m.getVal("0"))
		m.add("0", "0")
		require.False(t, m.empty())
		require.Equal(t, "0", m.getVal("0"))
		_, m = prepareMaps(100)
		require.False(t, m.empty())
	})

	t.Run("same-behavior-as-gomap", func(t *testing.T) {
		seed := time.Now().UnixNano()
		t.Logf("seed: %d", seed)
		rnd := rand.New(rand.NewSource(seed))
		distinctCnt := int(rnd.Int31n(256) + 256)
		gomap := make(map[string]string, distinctCnt)
		m := newLayeredMap[string, string]()
		for i := 0; i < 1<<15; i++ {
			kv := fmt.Sprintf("%d", rnd.Int()%distinctCnt)
			if rnd.Int()%2 == 0 {
				m.add(kv, kv)
				gomap[kv] = kv
			} else {
				m.del(kv)
				delete(gomap, kv)
			}
			if i%distinctCnt == 0 {
				mapEquals(gomap, m)
			}
		}
		mapEquals(gomap, m)
	})

	t.Run("forCOW-shouldn't-change-old-map", func(t *testing.T) {
		_, m := prepareMaps(20)
		layerLenEquals(m, []int{16, 4})

		wm := m.forCOW()
		wm.add("20", "20")
		layerLenEquals(wm, []int{16, 5})
		wm.del("0")
		layerLenEquals(wm, []int{16, 6})
		layerLenEquals(m, []int{16, 4})
		wm.compact(true)
		layerLenEquals(wm, []int{20})
		layerLenEquals(m, []int{16, 4})

		wwm := wm.forCOW()
		wwm.del("1")
		layerLenEquals(wwm, []int{20, 1})
		layerLenEquals(wm, []int{20})
	})

	t.Run("del-after-full-compact", func(t *testing.T) {
		m := newLayeredMap[string, string]()
		wm := m.forCOW()
		for i := 0; i < 100; i++ {
			kv := fmt.Sprintf("%d", i)
			wm.add(kv, kv)
		}
		require.Equal(t, 100, wm.estimatedLen())
		require.Len(t, wm.layers, 7)
		wm.compact(true)
		require.Len(t, wm.layers, 1)
		require.Equal(t, 100, wm.estimatedLen())
		wm.del("0")
		require.Equal(t, 99, wm.estimatedLen())
		_, ok := wm.get("0")
		require.False(t, ok)
	})

	t.Run("del-add-tombstone-on-mul-layer", func(t *testing.T) {
		gomap, m := prepareMaps(100)
		require.Equal(t, 100, m.estimatedLen())
		require.Len(t, m.layers, 7)
		mapEquals(gomap, m)
		_, ok := m.get("0")
		require.True(t, ok)
		// 0 stays on layer-0
		m.del("0")
		delete(gomap, "0")
		require.Equal(t, 101, m.estimatedLen())
		mapEquals(gomap, m)
		_, ok = m.get("0")
		require.False(t, ok)
	})

	t.Run("partial-compact-keeps-tombstone", func(t *testing.T) {
		gomap, m := prepareMaps(148)
		layerLenEquals(m, []int{128, 16, 4})
		// del 8 item on layer 0, 4 on layer 1
		for i := 0; i < 12; i++ {
			kv := fmt.Sprintf("%d", i+120)
			m.del(kv)
			delete(gomap, kv)
		}
		layerLenEquals(m, []int{128, 16, 16})
		require.Len(t, gomap, 136)
		mapEquals(gomap, m)
		m.compact(false)
		layerLenEquals(m, []int{128, 28})
		mapEquals(gomap, m)
	})

	t.Run("full-compact-clear-tombstone", func(t *testing.T) {
		m := newLayeredMap[string, string]()
		gomap := make(map[string]string, 50)
		for i := 0; i < 50; i++ {
			kv := fmt.Sprintf("%d", i)
			m.add(kv, kv)
			gomap[kv] = kv
			if i > 16 && i%16 == 1 { // on 17, 33, 49, del 1, 17, 33
				kv2 := fmt.Sprintf("%d", i-16)
				m.del(kv2)
				delete(gomap, kv2)
			}
		}
		layerLenEquals(m, []int{16, 16, 16, 5})
		require.Len(t, gomap, 47)
		mapEquals(gomap, m)
		m.compact(true)
		layerLenEquals(m, []int{47})
		mapEquals(gomap, m)
		// compact again does nothing
		m.compact(true)
		layerLenEquals(m, []int{47})
		mapEquals(gomap, m)
	})
}
