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

package stream

import (
	"context"
	"math"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

func (s *StreamBackupSearch) SearchFromDataFileForTest(
	ctx context.Context,
	dataFile *backuppb.DataFileInfo,
	ch chan<- *StreamKVInfo,
) error {
	return s.searchFromDataFile(ctx, dataFile, ch)
}

func (s *StreamBackupSearch) MergeCFEntriesForTest(
	defaultCFEntries, writeCFEntries map[string]*StreamKVInfo,
) []*StreamKVInfo {
	return s.mergeCFEntries(defaultCFEntries, writeCFEntries)
}

// LoadFrom loads data from an external storage into the stream metadata set. (Now only for test)
func (ms *StreamMetadataSet) LoadFrom(ctx context.Context, s storeapi.Storage) error {
	_, err := ms.LoadUntilAndCalculateShiftTS(ctx, s, math.MaxUint64)
	return err
}
