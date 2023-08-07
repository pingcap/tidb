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
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAddKeyValueMaintainRangeProperty(t *testing.T) {
	ctx := context.Background()
	memStore := storage.NewMemStorage()
	writer, err := memStore.Create(ctx, "/test", nil)
	require.NoError(t, err)
	rc := &rangePropertiesCollector{
		propSizeIdxDistance: 100,
		propKeysIdxDistance: 2,
	}
	rc.reset()
	initRC := *rc
	kvStore, err := NewKeyValueStore(ctx, writer, rc, 1, 1)
	require.NoError(t, err)

	require.Equal(t, &initRC, rc)
	encoded := rc.encode()
	require.Len(t, encoded, 0)

	k1, v1 := []byte("key1"), []byte("value1")
	err = kvStore.AddKeyValue(k1, v1)
	require.NoError(t, err)
	// when not accumulated enough data, no range property will be added.
	require.Equal(t, &initRC, rc)

	// propKeysIdxDistance = 2, so after adding 2 keys, a new range property will be added.
	k2, v2 := []byte("key2"), []byte("value2")
	err = kvStore.AddKeyValue(k2, v2)
	require.NoError(t, err)
	require.Len(t, rc.props, 1)
	expected := &rangeProperty{
		key:      k1,
		offset:   0,
		writerID: 1,
		dataSeq:  1,
		size:     uint64(len(k1) + len(v1) + len(k2) + len(v2)),
		keys:     2,
	}
	require.Equal(t, expected, rc.props[0])
	encoded = rc.encode()
	require.Greater(t, len(encoded), 0)

	// when not accumulated enough data, no range property will be added.
	k3, v3 := []byte("key3"), []byte("value3")
	err = kvStore.AddKeyValue(k3, v3)
	require.NoError(t, err)
	require.Len(t, rc.props, 1)

	err = writer.Close(ctx)
	require.NoError(t, err)

	writer, err = memStore.Create(ctx, "/test2", nil)
	require.NoError(t, err)
	rc = &rangePropertiesCollector{
		propSizeIdxDistance: 1,
		propKeysIdxDistance: 100,
	}
	rc.reset()
	kvStore, err = NewKeyValueStore(ctx, writer, rc, 2, 2)
	require.NoError(t, err)
	err = kvStore.AddKeyValue(k1, v1)
	require.NoError(t, err)
	require.Len(t, rc.props, 1)
	expected = &rangeProperty{
		key:      k1,
		offset:   0,
		writerID: 2,
		dataSeq:  2,
		size:     uint64(len(k1) + len(v1)),
		keys:     1,
	}
	require.Equal(t, expected, rc.props[0])

	err = kvStore.AddKeyValue(k2, v2)
	require.NoError(t, err)
	require.Len(t, rc.props, 2)
	expected = &rangeProperty{
		key:      k2,
		offset:   uint64(len(k1) + len(v1) + 16),
		writerID: 2,
		dataSeq:  2,
		size:     uint64(len(k2) + len(v2)),
		keys:     1,
	}
	require.Equal(t, expected, rc.props[1])
	err = writer.Close(ctx)
	require.NoError(t, err)
}

// TODO(lance6716): add more tests when the usage of other functions are merged into master.
