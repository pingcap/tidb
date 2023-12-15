// Copyright 2023 PingCAP, Inc.
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

package importer_test

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/mock/gomock"
)

func TestLocalSortChunkProcess(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	tempDir := t.TempDir()
	fileName := path.Join(tempDir, "test.csv")
	sourceData := []byte("1,2,3\n4,5,6\n7,8,9\n")
	require.NoError(t, os.WriteFile(fileName, sourceData, 0o644))
	file, err := os.Open(fileName)
	require.NoError(t, err)

	stmt := "create table test.t(a int, b int, c int, key(a), key(b,c))"
	tk.MustExec(stmt)
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	table, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	csvParser, err := mydump.NewCSVParser(
		ctx,
		&config.CSVConfig{
			Separator: `,`,
			Delimiter: `"`,
		},
		file,
		importer.LoadDataReadBlockSize,
		nil,
		false,
		nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, csvParser.Close())
	})

	fieldMappings := make([]*importer.FieldMapping, 0, len(table.VisibleCols()))
	for _, v := range table.VisibleCols() {
		fieldMapping := &importer.FieldMapping{
			Column: v,
		}
		fieldMappings = append(fieldMappings, fieldMapping)
	}
	logger := log.L()
	encoder, err := importer.NewTableKVEncoder(
		&encode.EncodingConfig{
			Path:   fileName,
			Table:  table,
			Logger: logger,
		},
		&importer.TableImporter{
			LoadDataController: &importer.LoadDataController{
				ASTArgs:       &importer.ASTArgs{},
				InsertColumns: table.VisibleCols(),
				FieldMappings: fieldMappings,
			},
		},
	)
	require.NoError(t, err)
	chunkInfo := &checkpoints.ChunkCheckpoint{
		FileMeta: mydump.SourceFileMeta{
			Path:     fileName,
			FileSize: int64(len(sourceData)),
		},
		Chunk: mydump.Chunk{
			EndOffset: int64(len(sourceData)),
			RowIDMax:  10000,
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	engineWriter := mock.NewMockEngineWriter(ctrl)
	engineWriter.EXPECT().AppendRows(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	diskQuotaLock := &syncutil.RWMutex{}
	codec := tikv.NewCodecV1(tikv.ModeRaw)
	processor := importer.NewLocalSortChunkProcessor(
		csvParser, encoder, codec,
		chunkInfo, logger.Logger, diskQuotaLock, engineWriter, engineWriter,
	)
	require.NoError(t, processor.Process(ctx))
	require.Equal(t, uint64(9), chunkInfo.Checksum.SumKVS())
	require.Equal(t, uint64(348), chunkInfo.Checksum.SumSize())
}
