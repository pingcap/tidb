// Copyright 2024 PingCAP, Inc.
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

package tikv

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var (
	sortedKVs = [][2][]byte{
		{[]byte("a"), []byte("1")},
	}
	ts uint64 = 1
)

func TestGRPCWriteToTiKV(t *testing.T) {
	t.Skip("this is a manual test")

	ctx := context.Background()
	pdAddrs := []string{"127.0.0.1:2379"}

	metas, err := write2ImportService4Test(ctx, pdAddrs, sortedKVs, ts)
	require.NoError(t, err)
	for _, meta := range metas {
		t.Logf("meta UUID: %v", uuid.UUID(meta.Uuid).String())
	}
}

type mockCollector struct {
	name string
}

func (m mockCollector) Add(key sstable.InternalKey, value []byte) error {
	return nil
}

func (m mockCollector) Finish(userProps map[string]string) error {
	return nil
}

func (m mockCollector) Name() string {
	return m.name
}

func TestPebbleWriteSST(t *testing.T) {
	t.Skip("this is a manual test")

	kvs := encodeKVs4Test(sortedKVs, ts)

	sstPath := "/tmp/test-write.sst"
	f, err := vfs.Default.Create(sstPath)
	require.NoError(t, err)
	writable := objstorageprovider.NewFileWritable(f)

	writer := sstable.NewWriter(writable, sstable.WriterOptions{
		Compression:  pebble.ZstdCompression, // should read TiKV config, and CF differs
		FilterPolicy: bloom.FilterPolicy(10),
		MergerName:   "",
		TablePropertyCollectors: []func() sstable.TablePropertyCollector{
			func() sstable.TablePropertyCollector {
				return mockCollector{name: "test-collector"}
			},
		},
	})
	for _, kv := range kvs {
		t.Logf("key: %X\nvalue: %X", kv[0], kv[1])
		err = writer.Set(kv[0], kv[1])
		require.NoError(t, err)
	}
	err = writer.Close()
	require.NoError(t, err)
}

func TestPebbleReadSST(t *testing.T) {
	t.Skip("this is a manual test")

	sstPath := "/tmp/test.sst"
	t.Logf("read sst: %s", sstPath)
	f, err := vfs.Default.Open(sstPath)
	require.NoError(t, err)
	readable, err := sstable.NewSimpleReadable(f)
	require.NoError(t, err)
	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	require.NoError(t, err)
	defer reader.Close()

	layout, err := reader.Layout()
	require.NoError(t, err)

	content := &strings.Builder{}
	layout.Describe(content, true, reader, nil)

	t.Logf("layout:\n %s", content.String())
	t.Logf("properties:\n %s", reader.Properties.String())

	iter, err := reader.NewIter(nil, nil)
	require.NoError(t, err)
	defer iter.Close()

	k, v := iter.First()
	if k == nil {
		return
	}
	getValue := func(v pebble.LazyValue) []byte {
		realV, _, err2 := v.Value(nil)
		require.NoError(t, err2)
		return realV
	}
	t.Logf("key: %X\nvalue: %X", k.UserKey, getValue(v))
	for {
		k, v = iter.Next()
		if k == nil {
			break
		}
		t.Logf("key: %X\nvalue: %X", k.UserKey, getValue(v))
	}
}
