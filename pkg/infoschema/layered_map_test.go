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
	"testing"

	"github.com/stretchr/testify/require"
)

func layeredROMapLen(m *layeredMap[string, string]) int {
	count := 0
	m.scan(func(_, _ string) bool { count++; return true })
	return count
}

func TestLayeredROMapGet(t *testing.T) {
}

func TestLayeredMap(t *testing.T) {
	mapEquals := func(expect map[string]string, m *layeredMap[string, string]) {
		require.Equal(t, len(expect), layeredROMapLen(m))
		for k, v := range expect {
			v1, ok := m.get(k)
			require.True(t, ok)
			require.Equal(t, v, v1)
		}
	}
	layersEquals := func(expect []map[string]string, m *layeredMap[string, string]) {
		require.Len(t, m.layers, len(expect))
		for i, layer := range expect {
			require.Equal(t, layer, m.layers[i])
		}
	}
	m := newLayeredMap[string, string]()
	// add 'a'
	m.add("a", "1")
	im := m.immutable()
	require.Len(t, im.layers, 1)
	mapEquals(map[string]string{"a": "1"}, im)
	// add 'b'
	m = im.mutable()
	m.add("b", "2")
	im = m.immutable()
	require.Len(t, im.layers, 2)
	mapEquals(map[string]string{"a": "1", "b": "2"}, im)
	layersEquals([]map[string]string{{"a": "1"}, {"b": "2"}}, im)
	// add 'a' again
	m = im.mutable()
	m.add("a", "3")
	im = m.immutable()
	require.Len(t, im.layers, 3)
	mapEquals(map[string]string{"a": "3", "b": "2"}, im)
	layersEquals([]map[string]string{{"a": "1"}, {"b": "2"}, {"a": "3"}}, im)
	// del 'a'
	m = im.mutable()
	m.del("a")
	im = m.immutable()
	// empty topLayer layer, so the number of layers is not changed.
	require.Len(t, im.layers, 2)
	mapEquals(map[string]string{"b": "2"}, im)
	layersEquals([]map[string]string{{}, {"b": "2"}}, im)
	// del 'b'
	m = im.mutable()
	m.del("b")
	im = m.immutable()
	require.Len(t, im.layers, 0)
	// add 'c', 'd', 'e', 'f'
	strings := []string{"c", "4", "d", "5", "e", "6", "f", "7"}
	for i := 0; i < len(strings); i += 2 {
		m = im.mutable()
		m.add(strings[i], strings[i+1])
		im = m.immutable()
	}
	require.Len(t, im.layers, 4)
	mapEquals(map[string]string{"c": "4", "d": "5", "e": "6", "f": "7"}, im)
	layersEquals([]map[string]string{{"c": "4"}, {"d": "5"}, {"e": "6"}, {"f": "7"}}, im)
	// add 'g'
	topLayer := im.layers[3]
	m = im.mutable()
	m.add("g", "8")
	im = m.immutable()
	require.Len(t, im.layers, 4)
	layersEquals([]map[string]string{{"c": "4"}, {"d": "5"}, {"e": "6"}, {"f": "7", "g": "8"}}, im)
	require.NotContains(t, topLayer, "g")
}
