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
	"testing"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
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

func fakeRanges(keys ...string) (r snapclient.DrainResult) {
	for i := range keys {
		if i+1 == len(keys) {
			return
		}
		r.Ranges = append(r.Ranges, rtree.Range{
			StartKey: []byte(keys[i]),
			EndKey:   []byte(keys[i+1]),
			Files:    []*backuppb.File{{Name: "fake.sst"}},
		})
		r.TableEndOffsetInRanges = append(r.TableEndOffsetInRanges, len(r.Ranges))
		r.TablesToSend = append(r.TablesToSend, snapclient.CreatedTable{
			Table: &model.TableInfo{
				ID: int64(i),
			},
		})
	}
	return
}

type errorInTimeSink struct {
	ctx   context.Context
	errCh chan error
	t     *testing.T
}

func (e errorInTimeSink) EmitTables(tables ...snapclient.CreatedTable) {}

func (e errorInTimeSink) EmitError(err error) {
	e.errCh <- err
}

func (e errorInTimeSink) Close() {}

func (e errorInTimeSink) Wait() {
	select {
	case <-e.ctx.Done():
		e.t.Logf("The context is canceled but no error happen")
		e.t.FailNow()
	case <-e.errCh:
	}
}

func assertErrorEmitInTime(ctx context.Context, t *testing.T) errorInTimeSink {
	errCh := make(chan error, 1)
	return errorInTimeSink{
		ctx:   ctx,
		errCh: errCh,
		t:     t,
	}
}

func TestSplitFailed(t *testing.T) {
	ranges := []snapclient.DrainResult{
		fakeRanges("aax", "abx", "abz"),
		fakeRanges("abz", "bbz", "bcy"),
		fakeRanges("bcy", "cad", "xxy"),
	}
	r := &fakeRestorer{errorInSplit: true, tableIDIsInsequence: true}
	sender, err := snapclient.NewTiKVSender(context.TODO(), r, nil, 1)
	require.NoError(t, err)
	dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sink := assertErrorEmitInTime(dctx, t)
	sender.PutSink(sink)
	for _, r := range ranges {
		sender.RestoreBatch(r)
	}
	sink.Wait()
	sender.Close()
	require.GreaterOrEqual(t, len(r.splitRanges), 2)
	require.Len(t, r.restoredFiles, 0)
	require.True(t, r.tableIDIsInsequence)
}

func TestRestoreFailed(t *testing.T) {
	ranges := []snapclient.DrainResult{
		fakeRanges("aax", "abx", "abz"),
		fakeRanges("abz", "bbz", "bcy"),
		fakeRanges("bcy", "cad", "xxy"),
	}
	r := &fakeRestorer{
		tableIDIsInsequence: true,
	}
	sender, err := snapclient.NewTiKVSender(context.TODO(), r, nil, 1)
	require.NoError(t, err)
	dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sink := assertErrorEmitInTime(dctx, t)
	sender.PutSink(sink)
	for _, r := range ranges {
		sender.RestoreBatch(r)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sink.Wait()
	}()
	sink.Close()
	sender.Close()
	wg.Wait()
	require.GreaterOrEqual(t, len(r.restoredFiles), 1)
	require.True(t, r.tableIDIsInsequence)
}
