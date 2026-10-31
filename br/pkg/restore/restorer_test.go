// Copyright 2024 PingCAP, Inc.
package restore_test

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
)

// Helper function to create test files
func createTestFiles() []*backuppb.File {
	return []*backuppb.File{
		{Name: "file1.sst", TotalKvs: 10},
		{Name: "file2.sst", TotalKvs: 20},
	}
}

type fakeImporter struct {
	restore.FileImporter
	hasError bool
}

func (f *fakeImporter) Import(ctx context.Context, fileSets ...restore.BackupFileSet) error {
	if f.hasError {
		return errors.New("import error")
	}
	return nil
}

func (f *fakeImporter) Close() error {
	return nil
}

func TestSimpleRestorerImportAndProgress(t *testing.T) {
	ctx := context.Background()
	files := createTestFiles()
	progressCount := int64(0)

	workerPool := util.NewWorkerPool(2, "simple-restorer")
	restorer := restore.NewSimpleSstRestorer(ctx, &fakeImporter{}, workerPool, nil)

	fileSet := restore.BatchBackupFileSet{
		{SSTFiles: files},
	}
	err := restorer.GoRestore(func(progress int64) {
		progressCount += progress
	}, fileSet)
	require.NoError(t, err)
	err = restorer.WaitUntilFinish()
	require.Equal(t, int64(30), progressCount)
	require.NoError(t, err)

	batchFileSet := restore.BatchBackupFileSet{
		{SSTFiles: files},
		{SSTFiles: files},
	}
	progressCount = int64(0)
	var mu sync.Mutex
	err = restorer.GoRestore(func(progress int64) {
		mu.Lock()
		progressCount += progress
		mu.Unlock()
	}, batchFileSet)
	require.NoError(t, err)
	err = restorer.WaitUntilFinish()
	require.NoError(t, err)
	require.Equal(t, int64(60), progressCount)
}

func TestSimpleRestorerWithErrorInImport(t *testing.T) {
	ctx := context.Background()

	workerPool := util.NewWorkerPool(2, "simple-restorer")
	restorer := restore.NewSimpleSstRestorer(ctx, &fakeImporter{hasError: true}, workerPool, nil)

	files := []*backuppb.File{
		{Name: "file_with_error.sst", TotalKvs: 15},
	}
	fileSet := restore.BatchBackupFileSet{
		{SSTFiles: files},
	}

	// Run restore and expect an error
	progressCount := int64(0)
	restorer.GoRestore(func(progress int64) {}, fileSet)
	err := restorer.WaitUntilFinish()
	require.Error(t, err)
	require.Contains(t, err.Error(), "import error")
	require.Equal(t, int64(0), progressCount)
}

func createSampleBatchFileSets() restore.BatchBackupFileSet {
	return restore.BatchBackupFileSet{
		{
			TableID: 1001,
			SSTFiles: []*backuppb.File{
				{Name: "file1.sst", TotalKvs: 10},
				{Name: "file2.sst", TotalKvs: 20},
			},
		},
		{
			TableID: 1002,
			SSTFiles: []*backuppb.File{
				{Name: "file3.sst", TotalKvs: 15},
			},
		},
	}
}

// FakeBalancedFileImporteris a minimal implementation for testing
type FakeBalancedFileImporter struct {
	hasError     bool
	unblockCount int
}

func (f *FakeBalancedFileImporter) Import(ctx context.Context, fileSets ...restore.BackupFileSet) error {
	if f.hasError {
		return errors.New("import error")
	}
	return nil
}

func (f *FakeBalancedFileImporter) PauseForBackpressure() {
	f.unblockCount++
}

func (f *FakeBalancedFileImporter) Close() error {
	return nil
}

func TestMultiTablesRestorerRestoreSuccess(t *testing.T) {
	ctx := context.Background()
	importer := &FakeBalancedFileImporter{}
	workerPool := util.NewWorkerPool(2, "multi-tables-restorer")

	restorer := restore.NewMultiTablesRestorer(ctx, importer, workerPool, nil)

	var progress int64
	fileSets := createSampleBatchFileSets()

	var mu sync.Mutex
	restorer.GoRestore(func(p int64) {
		mu.Lock()
		progress += p
		mu.Unlock()
	}, fileSets)
	err := restorer.WaitUntilFinish()
	require.NoError(t, err)

	// Ensure progress was tracked correctly
	require.Equal(t, int64(2), progress) // Total files group: 2
	require.Equal(t, 1, importer.unblockCount)
}

func TestMultiTablesRestorerRestoreWithImportError(t *testing.T) {
	ctx := context.Background()
	importer := &FakeBalancedFileImporter{hasError: true}
	workerPool := util.NewWorkerPool(2, "multi-tables-restorer")

	restorer := restore.NewMultiTablesRestorer(ctx, importer, workerPool, nil)
	fileSets := createSampleBatchFileSets()

	restorer.GoRestore(func(int64) {}, fileSets)
	err := restorer.WaitUntilFinish()
	require.Error(t, err)
	require.Contains(t, err.Error(), "import error")
}

func TestMultiTablesRestorerRestoreWithContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	importer := &FakeBalancedFileImporter{}
	workerPool := util.NewWorkerPool(2, "multi-tables-restorer")

	restorer := restore.NewMultiTablesRestorer(ctx, importer, workerPool, nil)

	fileSets := createSampleBatchFileSets()

	// Cancel context before restore completes
	cancel()
	err := restorer.GoRestore(func(int64) {}, fileSets)
	require.ErrorIs(t, err, context.Canceled)
}

// FakeSplitStrategy implements split.SplitStrategy for testing purposes
type FakeSplitStrategy[T any] struct {
	shouldSplit bool
	accumulated []T
}

// ShouldSkip determines if a given item should be skipped. For testing, this is hardcoded to `false`.
func (f *FakeSplitStrategy[T]) ShouldSkip(item T) bool {
	return false
}

// Accumulate adds a new item to the accumulated list.
func (f *FakeSplitStrategy[T]) Accumulate(item T) {
	f.accumulated = append(f.accumulated, item)
}

// ShouldSplit returns whether the accumulated items meet the condition for splitting.
func (f *FakeSplitStrategy[T]) ShouldSplit() bool {
	return f.shouldSplit
}

// ResetAccumulations clears the accumulated items.
func (f *FakeSplitStrategy[T]) ResetAccumulations() {
	f.accumulated = []T{}
}

// GetAccumulations returns an iterator for the accumulated items.
func (f *FakeSplitStrategy[T]) GetAccumulations() *split.SplitHelperIterator {
	rewrites, ok := any(f.accumulated).([]*split.RewriteSplitter)
	if !ok {
		panic("GetAccumulations called with non-*split.RewriteSplitter type")
	}
	return split.NewSplitHelperIterator(rewrites)
}

// FakeRegionsSplitter is a mock of the RegionsSplitter that records calls to ExecuteRegions
type FakeRegionsSplitter struct {
	split.Splitter
	executedSplitsCount int
	expectedEndKeys     [][]byte
}

func (f *FakeRegionsSplitter) ExecuteRegions(ctx context.Context, items *split.SplitHelperIterator) error {
	items.Traverse(func(v split.Valued, endKey []byte, rule *restoreutils.RewriteRules) bool {
		f.expectedEndKeys = append(f.expectedEndKeys, endKey)
		return true
	})
	f.executedSplitsCount += 1
	return nil
}

func TestWithSplitWithoutTriggersSplit(t *testing.T) {
	ctx := context.Background()
	fakeSplitter := &FakeRegionsSplitter{
		executedSplitsCount: 0,
	}
	strategy := &FakeSplitStrategy[string]{shouldSplit: false}
	wrapper := &restore.PipelineRestorerWrapper[string]{PipelineRegionsSplitter: fakeSplitter}

	items := iter.FromSlice([]string{"item1", "item2", "item3"})
	splitIter := wrapper.WithSplit(ctx, items, strategy)

	for i := splitIter.TryNext(ctx); !i.Finished; i = splitIter.TryNext(ctx) {
	}

	require.Equal(t, fakeSplitter.executedSplitsCount, 0)
}
func TestWithSplitAccumulateAndReset(t *testing.T) {
	ctx := context.Background()
	fakeSplitter := &FakeRegionsSplitter{}
	strategy := &FakeSplitStrategy[*split.RewriteSplitter]{shouldSplit: true}
	wrapper := &restore.PipelineRestorerWrapper[*split.RewriteSplitter]{PipelineRegionsSplitter: fakeSplitter}

	// Create RewriteSplitter items
	items := iter.FromSlice([]*split.RewriteSplitter{
		split.NewRewriteSpliter([]byte("t_1"), 1, nil, split.NewSplitHelper()),
		split.NewRewriteSpliter([]byte("t_2"), 2, nil, split.NewSplitHelper()),
	})
	splitIter := wrapper.WithSplit(ctx, items, strategy)

	// Traverse through the split iterator
	for i := splitIter.TryNext(ctx); !i.Finished; i = splitIter.TryNext(ctx) {
	}

	endKeys := [][]byte{
		codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(2)),
		codec.EncodeBytes([]byte{}, tablecodec.EncodeTablePrefix(3)),
	}

	// Verify that the split happened and the accumulation was reset
	require.ElementsMatch(t, endKeys, fakeSplitter.expectedEndKeys)
	require.Equal(t, 2, fakeSplitter.executedSplitsCount)
	require.Empty(t, strategy.accumulated)
}
