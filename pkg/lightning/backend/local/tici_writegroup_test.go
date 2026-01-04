// Copyright 2025 PingCAP, Inc.
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

package local

import (
	"context"

	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/pkg/tici"
)

type mockTiCIWriteGroup struct {
	createCount          int
	writePairsCount      int
	finishPartitionCount int
}

func (m *mockTiCIWriteGroup) CreateFileWriter(_ context.Context) (*tici.FileWriter, error) {
	m.createCount++
	return nil, nil
}

func (*mockTiCIWriteGroup) WriteHeader(_ context.Context, _ *tici.FileWriter, _ uint64) error {
	return nil
}

func (m *mockTiCIWriteGroup) WritePairs(_ context.Context, _ *tici.FileWriter, _ []*sst.Pair, count int) error {
	m.writePairsCount += count
	return nil
}

func (*mockTiCIWriteGroup) CloseFileWriters(_ context.Context, _ *tici.FileWriter) error {
	return nil
}

func (m *mockTiCIWriteGroup) FinishPartitionUpload(_ context.Context, _ *tici.FileWriter, _, _ []byte) error {
	m.finishPartitionCount++
	return nil
}

func (*mockTiCIWriteGroup) FinishIndexUpload(_ context.Context) error {
	return nil
}

func (*mockTiCIWriteGroup) Close() error {
	return nil
}
