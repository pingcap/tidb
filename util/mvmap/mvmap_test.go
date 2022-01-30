// Copyright 2017 PingCAP, Inc.
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

package mvmap

import (
	"fmt"
	"hash/fnv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMVMap(t *testing.T) {
	m := NewMVMap()
	m.Put([]byte("abc"), []byte("abc1"))
	m.Put([]byte("abc"), []byte("abc2"))
	m.Put([]byte("def"), []byte("def1"))
	m.Put([]byte("def"), []byte("def2"))

	var v [][]byte

	v = m.Get([]byte("abc"), v[:0])
	require.Equal(t, "[abc1 abc2]", fmt.Sprintf("%s", v))
	v = m.Get([]byte("def"), v[:0])
	require.Equal(t, "[def1 def2]", fmt.Sprintf("%s", v))
	require.Equal(t, 4, m.Len())

	results := []string{"abc abc1", "abc abc2", "def def1", "def def2"}
	it := m.NewIterator()
	for i := 0; i < 4; i++ {
		key, val := it.Next()
		require.Equal(t, results[i], fmt.Sprintf("%s %s", key, val))
	}

	key, val := it.Next()
	require.Nil(t, key)
	require.Nil(t, val)
}

func TestFNVHash(t *testing.T) {
	b := []byte{0xcb, 0xf2, 0x9c, 0xe4, 0x84, 0x22, 0x23, 0x25}
	sum1 := fnvHash64(b)
	hash := fnv.New64()
	hash.Reset()

	_, err := hash.Write(b)
	require.NoError(t, err)

	sum2 := hash.Sum64()
	require.Equal(t, sum1, sum2)
}
