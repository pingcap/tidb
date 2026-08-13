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

package conflictrows

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// Conflict-row files are retained for one week. Keep this hardcoded until
	// customer feedback shows that a configurable retention period is needed.
	retention                = 7 * 24 * time.Hour
	maxTaskIDsPerFlush       = 128
	maxObjectsPerFlush       = 1000
	maxLoggedMissingTaskIDs  = 16
	maxLoggedUnexpectedFiles = 16
)

// TaskInfoGetter provides task metadata needed to decide conflict-row retention.
type TaskInfoGetter interface {
	GetTaskCleanupInfoByIDs(context.Context, []int64) (map[int64]storage.TaskCleanupInfo, error)
}

type cleanupStats struct {
	candidateTasks              int64
	retainedTasks               int64
	deletedTasks                int64
	deletedFiles                int64
	missingMetadata             int64
	filesWithoutTask            int64
	nonImportIntoTaskFiles      int64
	unparsedTaskIDFiles         int64
	failures                    int64
	firstMissingTaskIDs         []int64
	firstFilesWithoutTask       []string
	firstNonImportIntoTaskFiles []string
	firstUnparsedTaskIDFiles    []string
}

func parseTaskID(name string) (int64, bool) {
	relativeName, ok := strings.CutPrefix(name, storagePrefix)
	if !ok {
		return 0, false
	}
	taskIDComponent, descendant, ok := strings.Cut(relativeName, "/")
	if !ok || strings.Trim(descendant, "/") == "" || taskIDComponent == "" {
		return 0, false
	}
	for _, char := range taskIDComponent {
		if char < '0' || char > '9' {
			return 0, false
		}
	}
	taskID, err := strconv.ParseInt(taskIDComponent, 10, 64)
	if err != nil || taskID <= 0 {
		return 0, false
	}
	return taskID, true
}

func shouldDelete(info storage.TaskCleanupInfo, now time.Time) bool {
	if info.Type != proto.ImportInto {
		return false
	}
	switch info.State {
	case proto.TaskStateFailed, proto.TaskStateReverted:
		return true
	case proto.TaskStateSucceed:
		return info.EndTime != nil && !now.Before(info.EndTime.Add(retention))
	default:
		return false
	}
}

func reconcileFiles(
	ctx context.Context,
	store storeapi.Storage,
	infoGetter TaskInfoGetter,
	now time.Time,
) (stats cleanupStats, err error) {
	defer func() {
		if err != nil {
			stats.failures++
		}
		logCleanupResult(stats)
	}()

	taskFiles := make(map[int64][]string, maxTaskIDsPerFlush)
	unparsedTaskIDFiles := make([]string, 0)
	fileCount := 0
	flush := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if fileCount == 0 {
			return nil
		}
		taskIDs := make([]int64, 0, len(taskFiles))
		for taskID := range taskFiles {
			taskIDs = append(taskIDs, taskID)
		}
		sort.Slice(taskIDs, func(i, j int) bool { return taskIDs[i] < taskIDs[j] })

		infosByTaskID := make(map[int64]storage.TaskCleanupInfo, len(taskIDs))
		if len(taskIDs) > 0 {
			var err error
			infosByTaskID, err = infoGetter.GetTaskCleanupInfoByIDs(ctx, taskIDs)
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		flushStats := cleanupStats{
			unparsedTaskIDFiles:      int64(len(unparsedTaskIDFiles)),
			firstUnparsedTaskIDFiles: appendFirstFiles(nil, unparsedTaskIDFiles),
		}
		filesToDelete := make([]string, 0, fileCount)
		filesToDelete = append(filesToDelete, unparsedTaskIDFiles...)
		for _, taskID := range taskIDs {
			flushStats.candidateTasks++
			info, ok := infosByTaskID[taskID]
			switch {
			case !ok:
				// Conflict-row files should not outlive their task metadata. This is
				// rare, so retain bounded file samples to make the cleanup observable.
				flushStats.missingMetadata++
				if len(flushStats.firstMissingTaskIDs) < maxLoggedMissingTaskIDs {
					flushStats.firstMissingTaskIDs = append(flushStats.firstMissingTaskIDs, taskID)
				}
				files := taskFiles[taskID]
				flushStats.filesWithoutTask += int64(len(files))
				flushStats.firstFilesWithoutTask = appendFirstFiles(
					flushStats.firstFilesWithoutTask, files)
				flushStats.deletedTasks++
				filesToDelete = append(filesToDelete, files...)
			case info.Type != proto.ImportInto:
				// A task ID collision with another task type should also be rare. These
				// files cannot belong to IMPORT INTO, so record samples before deletion.
				files := taskFiles[taskID]
				flushStats.nonImportIntoTaskFiles += int64(len(files))
				flushStats.firstNonImportIntoTaskFiles = appendFirstFiles(
					flushStats.firstNonImportIntoTaskFiles, files)
				flushStats.deletedTasks++
				filesToDelete = append(filesToDelete, files...)
			case shouldDelete(info, now):
				flushStats.deletedTasks++
				filesToDelete = append(filesToDelete, taskFiles[taskID]...)
			default:
				flushStats.retainedTasks++
			}
		}
		if len(filesToDelete) > 0 {
			if err := store.DeleteFiles(ctx, filesToDelete); err != nil {
				return err
			}
			flushStats.deletedFiles = int64(len(filesToDelete))
		}
		stats.mergeCompletedFlush(flushStats)

		clear(taskFiles)
		unparsedTaskIDFiles = unparsedTaskIDFiles[:0]
		fileCount = 0
		return nil
	}

	err = store.WalkDir(ctx, &storeapi.WalkOption{SubDir: storagePrefix}, func(name string, _ int64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		taskID, ok := parseTaskID(name)
		if !ok {
			if fileCount == maxObjectsPerFlush {
				if err := flush(); err != nil {
					return err
				}
			}
			// IMPORT INTO always writes a positive task ID in this path. Malformed
			// entries are unexpected and rare, so delete them and retain samples.
			unparsedTaskIDFiles = append(unparsedTaskIDFiles, name)
			fileCount++
			return nil
		}
		_, taskAlreadyPresent := taskFiles[taskID]
		if fileCount == maxObjectsPerFlush ||
			(!taskAlreadyPresent && len(taskFiles) == maxTaskIDsPerFlush) {
			if err := flush(); err != nil {
				return err
			}
		}
		taskFiles[taskID] = append(taskFiles[taskID], name)
		fileCount++
		return nil
	})
	if err != nil {
		return stats, err
	}
	if err = flush(); err != nil {
		return stats, err
	}
	return stats, nil
}

func (stats *cleanupStats) mergeCompletedFlush(completed cleanupStats) {
	stats.candidateTasks += completed.candidateTasks
	stats.retainedTasks += completed.retainedTasks
	stats.deletedTasks += completed.deletedTasks
	stats.deletedFiles += completed.deletedFiles
	stats.missingMetadata += completed.missingMetadata
	stats.filesWithoutTask += completed.filesWithoutTask
	stats.nonImportIntoTaskFiles += completed.nonImportIntoTaskFiles
	stats.unparsedTaskIDFiles += completed.unparsedTaskIDFiles
	missingIDCapacity := maxLoggedMissingTaskIDs - len(stats.firstMissingTaskIDs)
	stats.firstMissingTaskIDs = append(
		stats.firstMissingTaskIDs,
		completed.firstMissingTaskIDs[:min(missingIDCapacity, len(completed.firstMissingTaskIDs))]...,
	)
	stats.firstFilesWithoutTask = appendFirstFiles(
		stats.firstFilesWithoutTask, completed.firstFilesWithoutTask)
	stats.firstNonImportIntoTaskFiles = appendFirstFiles(
		stats.firstNonImportIntoTaskFiles, completed.firstNonImportIntoTaskFiles)
	stats.firstUnparsedTaskIDFiles = appendFirstFiles(
		stats.firstUnparsedTaskIDFiles, completed.firstUnparsedTaskIDFiles)
}

func appendFirstFiles(firstFiles, files []string) []string {
	remaining := maxLoggedUnexpectedFiles - len(firstFiles)
	if remaining <= 0 {
		return firstFiles
	}
	return append(firstFiles, files[:min(remaining, len(files))]...)
}

func logCleanupResult(stats cleanupStats) {
	logger := logutil.BgLogger()
	if stats.missingMetadata > 0 {
		logger.Warn("conflict-row cleanup deleted objects without task metadata",
			zap.Int64("missing-metadata", stats.missingMetadata),
			zap.Int64("file-count", stats.filesWithoutTask),
			zap.Int64s("task-ids", stats.firstMissingTaskIDs),
			zap.Strings("file-samples", stats.firstFilesWithoutTask))
	}
	if stats.nonImportIntoTaskFiles > 0 {
		logger.Warn("conflict-row cleanup deleted objects for non-import-into tasks",
			zap.Int64("file-count", stats.nonImportIntoTaskFiles),
			zap.Strings("file-samples", stats.firstNonImportIntoTaskFiles))
	}
	if stats.unparsedTaskIDFiles > 0 {
		logger.Warn("conflict-row cleanup deleted objects with unparsed task IDs",
			zap.Int64("file-count", stats.unparsedTaskIDFiles),
			zap.Strings("file-samples", stats.firstUnparsedTaskIDFiles))
	}
	logger.Info("finished conflict-row cleanup reconciliation",
		zap.Int64("candidate-tasks", stats.candidateTasks),
		zap.Int64("retained-tasks", stats.retainedTasks),
		zap.Int64("deleted-tasks", stats.deletedTasks),
		zap.Int64("deleted-files", stats.deletedFiles),
		zap.Int64("missing-metadata", stats.missingMetadata),
		zap.Int64("files-without-task", stats.filesWithoutTask),
		zap.Int64("non-import-into-task-files", stats.nonImportIntoTaskFiles),
		zap.Int64("unparsed-task-id-files", stats.unparsedTaskIDFiles),
		zap.Int64("failures", stats.failures))
}

// CleanExpiredFiles reconciles conflict-row files with current task metadata.
func CleanExpiredFiles(ctx context.Context, infoGetter TaskInfoGetter, cloudStorageURI string) error {
	if cloudStorageURI == "" {
		return nil
	}
	sortStore, err := importer.GetSortStore(ctx, cloudStorageURI)
	if err != nil {
		logCleanupResult(cleanupStats{failures: 1})
		return err
	}
	defer sortStore.Close()
	_, err = reconcileFiles(ctx, sortStore, infoGetter, time.Now())
	return err
}
