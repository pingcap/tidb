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

package logclient

import (
	"context"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/domain"
)

var FilterFilesByRegion = filterFilesByRegion

// readStreamMetaByTS is used for streaming task. collect all meta file by TS, it is for test usage.
func (rc *LogFileManager) ReadStreamMeta(ctx context.Context) ([]Meta, error) {
	metas, err := rc.streamingMeta(ctx)
	if err != nil {
		return nil, err
	}
	r := iter.CollectAll(ctx, metas)
	if r.Err != nil {
		return nil, errors.Trace(r.Err)
	}
	return r.Item, nil
}

func TEST_NewLogClient(clusterID, startTS, restoreTS uint64, storage storage.ExternalStorage, dom *domain.Domain) *LogClient {
	return &LogClient{
		dom: dom,
		LogFileManager: &LogFileManager{
			startTS:   startTS,
			restoreTS: restoreTS,
		},
		clusterID: clusterID,
		storage:   storage,
	}
}

func TEST_NewLogFileManager(startTS, restoreTS, shiftStartTS uint64, helper streamMetadataHelper) *LogFileManager {
	return &LogFileManager{
		startTS:      startTS,
		restoreTS:    restoreTS,
		shiftStartTS: shiftStartTS,
		helper:       helper,
	}
}

type FakeStreamMetadataHelper struct {
	streamMetadataHelper

	Data []byte
}

func (helper *FakeStreamMetadataHelper) ReadFile(
	ctx context.Context,
	path string,
	offset uint64,
	length uint64,
	compressionType backuppb.CompressionType,
	storage storage.ExternalStorage,
) ([]byte, error) {
	return helper.Data[offset : offset+length], nil
}
