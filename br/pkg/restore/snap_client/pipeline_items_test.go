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

package snapclient_test

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/rtree"
)

type fakeRestorer struct {
	mu                  sync.Mutex
	errorInSplit        bool
	splitRanges         []rtree.Range
	restoredFiles       []*backuppb.File
	tableIDIsInsequence bool
}

func (f *fakeRestorer) SplitRanges(ctx context.Context, ranges []rtree.Range, updateCh glue.Progress, isRawKv bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	f.splitRanges = append(f.splitRanges, ranges...)
	if f.errorInSplit {
		err := errors.Annotatef(berrors.ErrRestoreSplitFailed,
			"the key space takes many efforts and finally get together, how dare you split them again... :<")
		log.Error("error happens :3", logutil.ShortError(err))
		return err
	}
	return nil
}

func (f *fakeRestorer) RestoreSSTFiles(ctx context.Context, tableIDWithFiles []snapclient.TableIDWithFiles, updateCh glue.Progress) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	for i, tableIDWithFile := range tableIDWithFiles {
		if int64(i) != tableIDWithFile.TableID {
			f.tableIDIsInsequence = false
		}
		f.restoredFiles = append(f.restoredFiles, tableIDWithFile.Files...)
	}
	err := errors.Annotatef(berrors.ErrRestoreWriteAndIngest, "the files to restore are taken by a hijacker, meow :3")
	log.Error("error happens :3", logutil.ShortError(err))
	return err
}
