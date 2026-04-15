// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table/tables"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type trackingReader struct {
	*bytes.Reader
	size   int64
	closed bool
}

func newTrackingReader(content string) *trackingReader {
	return &trackingReader{
		Reader: bytes.NewReader([]byte(content)),
		size:   int64(len(content)),
	}
}

func (r *trackingReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart && offset > 0 {
		return 0, errors.New("mock seek error")
	}
	return r.Reader.Seek(offset, whence)
}

func (r *trackingReader) Close() error {
	r.closed = true
	return nil
}

func (r *trackingReader) GetFileSize() (int64, error) {
	return r.size, nil
}

type trackingStorage struct {
	reader *trackingReader
}

func (*trackingStorage) WriteFile(context.Context, string, []byte) error  { panic("not implemented") }
func (*trackingStorage) ReadFile(context.Context, string) ([]byte, error) { panic("not implemented") }
func (*trackingStorage) FileExists(context.Context, string) (bool, error) { panic("not implemented") }
func (*trackingStorage) DeleteFile(context.Context, string) error         { panic("not implemented") }
func (s *trackingStorage) Open(context.Context, string, *storeapi.ReaderOption) (objectio.Reader, error) {
	return s.reader, nil
}
func (*trackingStorage) DeleteFiles(context.Context, []string) error { panic("not implemented") }
func (*trackingStorage) WalkDir(context.Context, *storeapi.WalkOption, func(string, int64) error) error {
	panic("not implemented")
}
func (*trackingStorage) URI() string { return "mock://tracking" }
func (*trackingStorage) Create(context.Context, string, *storeapi.WriterOption) (objectio.Writer, error) {
	panic("not implemented")
}
func (*trackingStorage) Rename(context.Context, string, string) error { panic("not implemented") }
func (*trackingStorage) PresignFile(context.Context, string, time.Duration) (string, error) {
	panic("not implemented")
}
func (*trackingStorage) Close() {}

func createDataFiles(t *testing.T, dir string, fileCount, rowsPerFile, rowLen int) {
	t.Helper()
	require.NoError(t, os.Mkdir(dir, 0o755))
	padLen := rowLen - 4
	require.GreaterOrEqual(t, padLen, 3)
	padding := strings.Repeat("a", padLen/3)
	var rowSB strings.Builder
	rowSB.WriteString("1111")
	for range 3 {
		rowSB.WriteString(",")
		rowSB.WriteString(padding)
	}
	rowSB.WriteString("\n")
	rowData := rowSB.String()
	var fileSB strings.Builder
	for j := 0; j < rowsPerFile; j++ {
		fileSB.WriteString(rowData)
	}
	for i := 0; i < fileCount; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("%03d.csv", i)), []byte(fileSB.String()), 0o644))
	}
}

type caseTp struct {
	fileCount, rowsPerFile, rowLen int
	ksCodec                        []byte
	sql                            string
	ratio                          float64
}

func runCaseFn(t *testing.T, i int, c caseTp) {
	dir := filepath.Join(t.TempDir(), fmt.Sprintf("case-%d", i))
	createDataFiles(t, dir, c.fileCount, c.rowsPerFile, c.rowLen)
	p := parser.New()
	node, err := p.ParseOneStmt(c.sql, "", "")
	require.NoError(t, err)
	sctx := utilmock.NewContext()
	tblInfo, err := ddl.MockTableInfo(sctx, node.(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	tblInfo.State = model.StatePublic
	table := tables.MockTableFromMeta(tblInfo)
	ctrl, err := NewLoadDataController(&Plan{
		Path:           filepath.Join(dir, "*.csv"),
		Format:         DataFormatCSV,
		LineFieldsInfo: newDefaultLineFieldsInfo(),
		InImportInto:   true,
	}, table, &ASTArgs{})
	require.NoError(t, err)
	ctrl.logger = zap.Must(zap.NewDevelopment())
	ctx := context.Background()
	require.NoError(t, ctrl.InitDataFiles(ctx))
	ratio, err := ctrl.sampleIndexSizeRatio(ctx, c.ksCodec)
	require.NoError(t, err)
	require.InDelta(t, c.ratio, ratio, 0.001)

	sampled, err := SampleFileImportKVSize(
		ctx,
		ctrl.buildKVSizeSampleConfig(),
		table,
		ctrl.dataStore,
		ctrl.dataFiles,
		c.ksCodec,
		ctrl.logger,
	)
	require.NoError(t, err)
	var sampledRatio float64
	if sampled.DataKVSize > 0 {
		sampledRatio = float64(sampled.IndexKVSize) / float64(sampled.DataKVSize)
	}
	require.InDelta(t, c.ratio, sampledRatio, 0.001)
}

func TestSampleIndexSizeRatio(t *testing.T) {
	ksCodec := []byte{'x', 0x00, 0x00, 0x01}
	simpleTbl := `create table t (a int, b text, c text, d text, index idx(a));`
	cases := []caseTp{
		// without ks codec
		// no file
		{0, 20, 100, nil, simpleTbl, 0},
		// < 3 files
		{1, 20, 100, nil, simpleTbl, 0.287},
		{2, 20, 100, nil, simpleTbl, 0.287},
		// < 3 files, not enough rows
		{2, 8, 100, nil, simpleTbl, 0.287},
		// enough files
		{10, 20, 100, nil, simpleTbl, 0.287},
		{10, 20, 100, nil,
			`create table t (a int, b text, c text, d text, index idx(b(1024)));`, 0.568},
		{10, 20, 100, nil,
			`create table t (a int, b text, c text, d text, index idx1(a), index idx2(a), index idx3(a), index idx4(a));`, 1.151},
		// enough files, not enough rows
		{10, 5, 100, nil, simpleTbl, 0.287},
		// longer rows
		{10, 20, 400, nil, simpleTbl, 0.087},
		{10, 12, 2000, nil, simpleTbl, 0.018},
		{10, 12, 2000, nil,
			`create table t (a int, b text, c text, d text, index idx1(a), index idx2(a), index idx3(a), index idx4(a));`, 0.074},

		// with ks codec
		{10, 20, 100, ksCodec, simpleTbl, 0.308},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			runCaseFn(t, i, c)
		})
	}

	t.Run("parser_close_on_error", func(t *testing.T) {
		newChunk := func() *checkpoints.ChunkCheckpoint {
			return &checkpoints.ChunkCheckpoint{
				FileMeta: mydump.SourceFileMeta{
					Path:     "test.sql",
					FileSize: 16,
				},
				Chunk: mydump.Chunk{
					Offset:       1,
					PrevRowIDMax: 1,
				},
			}
		}

		reader := newTrackingReader("INSERT INTO t VALUES (1);\n")
		sampler := &kvSizeSampler{
			cfg: &KVSizeSampleConfig{
				Format: DataFormatSQL,
			},
			dataStore: &trackingStorage{reader: reader},
			logger:    zap.NewNop(),
		}
		_, err := sampler.getParser(context.Background(), newChunk())
		require.Error(t, err)
		require.True(t, reader.closed)

		reader = newTrackingReader("INSERT INTO t VALUES (1);\n")
		ctrl := &LoadDataController{
			Plan:      &Plan{Format: DataFormatSQL},
			dataStore: &trackingStorage{reader: reader},
			logger:    zap.NewNop(),
		}
		_, err = ctrl.getParser(context.Background(), newChunk())
		require.Error(t, err)
		require.True(t, reader.closed)
	})

	t.Run("sql_source_size_uses_consumed_bytes_not_buffered_progress", func(t *testing.T) {
		dir := t.TempDir()
		var fileSB strings.Builder
		fileSB.WriteString("INSERT INTO t VALUES\n")
		for i := 0; i < 20; i++ {
			_, err := fmt.Fprintf(&fileSB, "(%d,'v%d','w%d','x%d')", i, i, i, i)
			require.NoError(t, err)
			if i < 19 {
				fileSB.WriteString(",\n")
				continue
			}
			fileSB.WriteString(";\n")
		}
		content := fileSB.String()
		require.NoError(t, os.WriteFile(filepath.Join(dir, "001.sql"), []byte(content), 0o644))

		p := parser.New()
		node, err := p.ParseOneStmt(`create table t (a int, b text, c text, d text, index idx(a));`, "", "")
		require.NoError(t, err)
		sctx := utilmock.NewContext()
		tblInfo, err := ddl.MockTableInfo(sctx, node.(*ast.CreateTableStmt), 1)
		require.NoError(t, err)
		tblInfo.State = model.StatePublic
		table := tables.MockTableFromMeta(tblInfo)

		ctrl, err := NewLoadDataController(&Plan{
			Path:         filepath.Join(dir, "*.sql"),
			Format:       DataFormatSQL,
			InImportInto: true,
		}, table, &ASTArgs{})
		require.NoError(t, err)
		ctrl.logger = zap.Must(zap.NewDevelopment())
		ctx := context.Background()
		require.NoError(t, ctrl.InitDataFiles(ctx))

		sampled, err := SampleFileImportKVSize(
			ctx,
			ctrl.buildKVSizeSampleConfig(),
			table,
			ctrl.dataStore,
			ctrl.dataFiles,
			nil,
			ctrl.logger,
		)
		require.NoError(t, err)
		require.Positive(t, sampled.SourceSize)
		require.Positive(t, sampled.TotalKVSize())
		require.Greater(t, sampled.SourceSize, int64(len(content)/2))
		require.Less(t, sampled.SourceSize, int64(len(content)*2))
	})
}
func TestSampleIndexSizeRatioVeryLongRows(t *testing.T) {
	simpleTbl := `create table t (a int, b text, c text, d text, index idx(a));`
	bak := maxSampleFileSize
	maxSampleFileSize = 2 * units.MiB
	t.Cleanup(func() {
		maxSampleFileSize = bak
	})
	// early return when reach maxSampleFileSize
	longRowCase := caseTp{10, 10, units.MiB + 100*units.KiB, nil, simpleTbl, 0}
	runCaseFn(t, -1, longRowCase)
}
