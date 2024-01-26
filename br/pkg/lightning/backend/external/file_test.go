// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func getEncodedData(key, value []byte) []byte {
	buf := make([]byte, 8*2+len(key)+len(value))
	binary.BigEndian.PutUint64(buf, uint64(len(key)))
	binary.BigEndian.PutUint64(buf[8:], uint64(len(value)))
	copy(buf[8*2:], key)
	copy(buf[8*2+len(key):], value)
	return buf
}

func TestAddKeyValueMaintainRangeProperty(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	writer, err := memStore.Create(ctx, "/test", nil)
	require.NoError(t, err)
	rc := &rangePropertiesCollector{
		propSizeDist: 100,
		propKeysDist: 2,
	}
	rc.reset()
	initRC := *rc
	kvStore, err := NewKeyValueStore(ctx, writer, rc)
	require.NoError(t, err)

	require.Equal(t, &initRC, rc)
	encoded := rc.encode()
	require.Len(t, encoded, 0)

	k1, v1 := []byte("key1"), []byte("value1")
	err = kvStore.addEncodedData(getEncodedData(k1, v1))
	require.NoError(t, err)
	// when not accumulated enough data, no range property will be added.
	require.Equal(t, &initRC, rc)

	// propKeysDist = 2, so after adding 2 keys, a new range property will be added.
	k2, v2 := []byte("key2"), []byte("value2")
	err = kvStore.addEncodedData(getEncodedData(k2, v2))
	require.NoError(t, err)
	require.Len(t, rc.props, 1)
	expected := &rangeProperty{
		firstKey: k1,
		lastKey:  k2,
		offset:   0,
		size:     uint64(len(k1) + len(v1) + len(k2) + len(v2)),
		keys:     2,
	}
	require.Equal(t, expected, rc.props[0])
	encoded = rc.encode()
	require.Greater(t, len(encoded), 0)

	// when not accumulated enough data, no range property will be added.
	k3, v3 := []byte("key3"), []byte("value3")
	err = kvStore.addEncodedData(getEncodedData(k3, v3))
	require.NoError(t, err)
	require.Len(t, rc.props, 1)

	kvStore.Close()
	err = writer.Close(ctx)
	require.NoError(t, err)
	expected = &rangeProperty{
		firstKey: k3,
		lastKey:  k3,
		offset:   uint64(len(k1) + len(v1) + 16 + len(k2) + len(v2) + 16),
		size:     uint64(len(k3) + len(v3)),
		keys:     1,
	}
	require.Len(t, rc.props, 2)
	require.Equal(t, expected, rc.props[1])

	writer, err = memStore.Create(ctx, "/test2", nil)
	require.NoError(t, err)
	rc = &rangePropertiesCollector{
		propSizeDist: 1,
		propKeysDist: 100,
	}
	rc.reset()
	kvStore, err = NewKeyValueStore(ctx, writer, rc)
	require.NoError(t, err)
	err = kvStore.addEncodedData(getEncodedData(k1, v1))
	require.NoError(t, err)
	require.Len(t, rc.props, 1)
	expected = &rangeProperty{
		firstKey: k1,
		lastKey:  k1,
		offset:   0,
		size:     uint64(len(k1) + len(v1)),
		keys:     1,
	}
	require.Equal(t, expected, rc.props[0])

	err = kvStore.addEncodedData(getEncodedData(k2, v2))
	require.NoError(t, err)
	require.Len(t, rc.props, 2)
	expected = &rangeProperty{
		firstKey: k2,
		lastKey:  k2,
		offset:   uint64(len(k1) + len(v1) + 16),
		size:     uint64(len(k2) + len(v2)),
		keys:     1,
	}
	require.Equal(t, expected, rc.props[1])
	kvStore.Close()
	// Length of properties should not change after close.
	require.Len(t, rc.props, 2)
	err = writer.Close(ctx)
	require.NoError(t, err)
}

func TestKVReadWrite(t *testing.T) {
	seed := time.Now().Unix()
	rand.Seed(uint64(seed))
	t.Logf("seed: %d", seed)
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	writer, err := memStore.Create(ctx, "/test", nil)
	require.NoError(t, err)
	rc := &rangePropertiesCollector{
		propSizeDist: 100,
		propKeysDist: 2,
	}
	rc.reset()
	kvStore, err := NewKeyValueStore(ctx, writer, rc)
	require.NoError(t, err)

	kvCnt := rand.Intn(10) + 10
	keys := make([][]byte, kvCnt)
	values := make([][]byte, kvCnt)
	for i := 0; i < kvCnt; i++ {
		randLen := rand.Intn(10) + 1
		keys[i] = make([]byte, randLen)
		rand.Read(keys[i])
		randLen = rand.Intn(10) + 1
		values[i] = make([]byte, randLen)
		rand.Read(values[i])
		err = kvStore.addEncodedData(getEncodedData(keys[i], values[i]))
		require.NoError(t, err)
	}
	kvStore.Close()
	err = writer.Close(ctx)
	require.NoError(t, err)

	bufSize := rand.Intn(100) + 1
	kvReader, err := newKVReader(ctx, "/test", memStore, 0, bufSize)
	require.NoError(t, err)
	for i := 0; i < kvCnt; i++ {
		key, value, err := kvReader.nextKV()
		require.NoError(t, err)
		require.Equal(t, keys[i], key)
		require.Equal(t, values[i], value)
	}
	_, _, err = kvReader.nextKV()
	require.Equal(t, io.EOF, err)

	require.NoError(t, kvReader.Close())
}
