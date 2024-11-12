// Copyright 2024 PingCAP, Inc.
package restore_test

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/pkg/util"
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
	err := restorer.Restore(func(progress int64) {
		progressCount += progress
	}, fileSet)
	require.NoError(t, err)
	err = restorer.WaitUnitilFinish()
	require.Equal(t, int64(30), progressCount)
	require.NoError(t, err)

	batchFileSet := restore.BatchBackupFileSet{
		{SSTFiles: files},
		{SSTFiles: files},
	}
	progressCount = int64(0)
	err = restorer.Restore(func(progress int64) {
		progressCount += progress
	}, batchFileSet)
	require.NoError(t, err)
	err = restorer.WaitUnitilFinish()
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
	err := restorer.Restore(func(progress int64) {}, fileSet)
	require.Error(t, err)
	require.Contains(t, err.Error(), "import error")
}
