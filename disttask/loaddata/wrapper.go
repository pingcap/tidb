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

package loaddata

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/util/intest"
)

func makeTableRegions(ctx context.Context, task *TaskMeta, concurrency int) ([]*mydump.TableRegion, error) {
	if concurrency <= 0 {
		return nil, errors.Errorf("concurrency must be greater than 0, but got %d", concurrency)
	}

	b, err := storage.ParseBackend(task.Dir, nil)
	if err != nil {
		return nil, err
	}

	opt := &storage.ExternalStorageOptions{}
	if intest.InTest {
		opt.NoCredentials = true
	}
	store, err := storage.New(ctx, b, opt)
	if err != nil {
		return nil, err
	}

	meta := &mydump.MDTableMeta{
		DB:           task.Table.DBName,
		Name:         task.Table.Info.Name.String(),
		IsRowOrdered: task.Table.IsRowOrdered,
	}

	sourceType, err := transformSourceType(task.Format.Type)
	if err != nil {
		return nil, err
	}
	for _, file := range task.FileInfos {
		meta.DataFiles = append(meta.DataFiles, mydump.FileInfo{
			FileMeta: mydump.SourceFileMeta{
				Path:        file.Path,
				Type:        sourceType,
				FileSize:    file.Size,
				RealSize:    file.RealSize,
				Compression: task.Format.Compression,
			},
		})
	}
	cfg := &config.Config{
		App: config.Lightning{
			RegionConcurrency: concurrency,
			TableConcurrency:  concurrency,
		},
		Mydumper: config.MydumperRuntime{
			CSV:           task.Format.CSV.Config,
			StrictFormat:  task.Format.CSV.Strict,
			MaxRegionSize: config.MaxRegionSize,
			ReadBlockSize: config.ReadBlockSize,
			// uniform distribution
			BatchImportRatio: 0,
		},
	}

	return mydump.MakeTableRegions(ctx, meta, len(task.Table.TargetColumns), cfg, nil, store)
}

func transformSourceType(tp string) (mydump.SourceType, error) {
	switch tp {
	case importer.LoadDataFormatParquet:
		return mydump.SourceTypeParquet, nil
	case importer.LoadDataFormatDelimitedData:
		return mydump.SourceTypeCSV, nil
	case importer.LoadDataFormatSQLDump:
		return mydump.SourceTypeSQL, nil
	default:
		return mydump.SourceTypeIgnore, errors.Errorf("unknown source type: %s", tp)
	}
}
