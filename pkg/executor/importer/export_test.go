// Copyright 2019 PingCAP, Inc.
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

package importer

import (
	"context"

	"github.com/pingcap/tidb/pkg/lightning/importdef"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

// NewDefaultLineFieldsInfoForTest exposes the default format to external tests.
func NewDefaultLineFieldsInfoForTest() importdef.LineFieldsInfo {
	return newDefaultLineFieldsInfo()
}

// SetLoggerForTest replaces the logger of the controller.
func (e *LoadDataController) SetLoggerForTest(logger *zap.Logger) {
	e.logger = logger
}

// SampleIndexSizeRatioForTest exposes sampleIndexSizeRatio to external tests.
func (e *LoadDataController) SampleIndexSizeRatioForTest(ctx context.Context, ksCodec []byte) (float64, error) {
	return e.sampleIndexSizeRatio(ctx, ksCodec)
}

// BuildKVSizeSampleConfigForTest exposes the sample config to external tests.
func (e *LoadDataController) BuildKVSizeSampleConfigForTest() *KVSizeSampleConfig {
	return e.buildKVSizeSampleConfig()
}

// DataStoreForTest returns the data store of the controller.
func (e *LoadDataController) DataStoreForTest() storeapi.Storage {
	return e.dataStore
}

// DataFilesForTest returns the data files of the controller.
func (e *LoadDataController) DataFilesForTest() []*mydump.SourceFileMeta {
	return e.dataFiles
}

// SetMaxSampleFileSizeForTest changes the sample size limit and returns a
// function that restores it.
func SetMaxSampleFileSizeForTest(size int64) (restore func()) {
	bak := maxSampleFileSize
	maxSampleFileSize = size
	return func() { maxSampleFileSize = bak }
}
