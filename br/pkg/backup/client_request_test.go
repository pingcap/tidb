// Copyright 2026 PingCAP, Inc.
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

package backup

import (
	"sync"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/stretchr/testify/require"
)

func TestBuildStoreBackupReqRewritesClonedBackend(t *testing.T) {
	baseBackend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_S3{S3: &backuppb.S3{Bucket: "bucket", Prefix: "root"}},
	}
	loop := &MainBackupLoop{
		BackupReq: backuppb.BackupRequest{
			StorageBackend: baseBackend,
		},
		RewriteStorageBackend: func(storeID uint64, backend *backuppb.StorageBackend) error {
			require.NotSame(t, baseBackend, backend)
			backend.GetS3().Prefix = "root/store-7"
			return nil
		},
	}

	storeReq, err := loop.buildStoreBackupReq(7)
	require.NoError(t, err)
	require.Equal(t, "root/store-7", storeReq.GetStorageBackend().GetS3().Prefix)
	require.Equal(t, "root", loop.BackupReq.GetStorageBackend().GetS3().Prefix)
}

func TestBuildStoreBackupReqRunsBeforeFirstRequestToStoreOncePerStore(t *testing.T) {
	baseBackend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_S3{S3: &backuppb.S3{Bucket: "bucket", Prefix: "root"}},
	}
	var (
		mu    sync.Mutex
		calls = make(map[uint64]int)
		seen  = make(map[uint64]string)
	)
	loop := &MainBackupLoop{
		BackupReq: backuppb.BackupRequest{
			StorageBackend: baseBackend,
		},
		RewriteStorageBackend: func(storeID uint64, backend *backuppb.StorageBackend) error {
			backend.GetS3().Prefix = "root/store"
			return nil
		},
		BeforeFirstRequestToStore: func(storeID uint64, request backuppb.BackupRequest) error {
			mu.Lock()
			defer mu.Unlock()
			calls[storeID]++
			seen[storeID] = request.GetStorageBackend().GetS3().Prefix
			return nil
		},
	}

	_, err := loop.buildStoreBackupReq(7)
	require.NoError(t, err)
	_, err = loop.buildStoreBackupReq(7)
	require.NoError(t, err)
	_, err = loop.buildStoreBackupReq(8)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, map[uint64]int{7: 1, 8: 1}, calls)
	require.Equal(t, map[uint64]string{7: "root/store", 8: "root/store"}, seen)
}
