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

// Restorer is the minimal methods required for restoring.
// It contains the primitive APIs extract from `restore.Client`, so some of arguments may seem redundant.
// Maybe TODO: make a better abstraction?
package sstfiles

import (
	"context"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/internal/utils"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/rtree"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"golang.org/x/sync/errgroup"
)

type SimpleFileRestorer struct {
	workerPool   *tidbutil.WorkerPool
	splitter     split.SplitClient
	fileImporter *SnapFileImporter
}

func NewSimpleFileRestorer(
	fileImporter *SnapFileImporter,
	splitter split.SplitClient,
	workerPool *tidbutil.WorkerPool,
) FileRestorer {
	return &SimpleFileRestorer{
		fileImporter: fileImporter,
		splitter:     splitter,
		workerPool:   workerPool,
	}
}

func (s *SimpleFileRestorer) Close() error {
	return s.fileImporter.Close()
}

// SplitRanges implements FileRestorer. It splits region by
// data range after rewrite.
// updateCh is used to record progress.
func (s *SimpleFileRestorer) SplitRanges(ctx context.Context, ranges []rtree.Range, updateCh glue.Progress) error {
	var splitClientOpt split.ClientOptionalParameter
	splitClientOpt = split.WithOnSplit(func(keys [][]byte) {
		for range keys {
			updateCh.Inc()
		}
	})
	s.splitter.ApplyOptions(splitClientOpt)
	splitter := utils.NewRegionSplitter(s.splitter)
	return splitter.ExecuteSplit(ctx, ranges)
}

func (r *SimpleFileRestorer) RestoreFiles(ctx context.Context, files []SstFilesInfo, updateCh glue.Progress) error {
	errCh := make(chan error, len(files))
	eg, ectx := errgroup.WithContext(ctx)
	defer close(errCh)

	for _, file := range files {
		fileReplica := file
		r.workerPool.ApplyOnErrorGroup(eg,
			func() error {
				defer func() {
					log.Info("import sst files done", logutil.Files(fileReplica.Files))
					updateCh.Inc()
				}()
				return r.fileImporter.ImportSSTFiles(ectx, fileReplica.Files, fileReplica.RewriteRules)
			})
	}
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	return nil
}
