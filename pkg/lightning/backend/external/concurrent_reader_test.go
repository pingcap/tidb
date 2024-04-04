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
	"io"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestConcurrentRead(t *testing.T) {
	seed := time.Now().Unix()
	t.Logf("seed: %d", seed)
	rand.Seed(uint64(seed))

	memStore := storage.NewMemStorage()
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}

	ctx := context.Background()
	filename := "/test"
	w, err := memStore.Create(ctx, filename, nil)
	require.NoError(t, err)
	_, err = w.Write(ctx, data)
	require.NoError(t, err)
	err = w.Close(ctx)
	require.NoError(t, err)

	fileSize := 256
	offset := rand.Intn(fileSize)
	concurrency := rand.Intn(4) + 1
	readBufferSize := rand.Intn(100) + 1

	bufs := make([][]byte, concurrency)
	for i := range bufs {
		bufs[i] = make([]byte, readBufferSize)
	}
	rd, err := newConcurrentFileReader(
		ctx,
		memStore,
		filename,
		int64(offset),
		int64(fileSize),
		concurrency,
		readBufferSize,
	)
	require.NoError(t, err)

	got := make([]byte, 0, 256)

	for {
		bs, err := rd.read(bufs)
		if err != nil {
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
		for _, b := range bs {
			got = append(got, b...)
		}
	}

	require.Equal(t, data[offset:], got)
}
