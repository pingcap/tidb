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
	"slices"
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
	// Conflict-row output from successful tasks is retained for one week. Output
	// from failed or reverted tasks, files without matching IMPORT INTO task
	// metadata, and invalid file names are deleted immediately. Keep the retention
	// period hardcoded until customer feedback shows that it should be configurable.
	retention          = 7 * 24 * time.Hour
	maxTaskIDsPerFlush = 128
	maxObjectsPerFlush = 1000
	maxLoggedSamples   = 16
)

// TaskInfoGetter provides task metadata needed to decide conflict-row retention.
type TaskInfoGetter interface {
	GetTaskCleanupInfoByIDs(context.Context, []int64) (map[int64]*storage.TaskCleanupInfo, error)
}

// countWithSamples uses exported fields so zap.Any can encode them using their JSON tags.
type countWithSamples struct {
	Count   int64    `json:"count,omitempty"`
	Samples []string `json:"samples,omitempty"`
}

func (cs *countWithSamples) appendSamples(samples []string) {
	remaining := maxLoggedSamples - len(cs.Samples)
	if remaining <= 0 {
		return
	}
	cs.Samples = append(cs.Samples, samples[:min(remaining, len(samples))]...)
}

func recordCountWithSamples(cs **countWithSamples, samples ...string) {
	if len(samples) == 0 {
		return
	}
	if *cs == nil {
		*cs = &countWithSamples{}
	}
	(*cs).Count += int64(len(samples))
	(*cs).appendSamples(samples)
}

func mergeCountWithSamples(cs **countWithSamples, completed *countWithSamples) {
	if completed == nil {
		return
	}
	if *cs == nil {
		*cs = &countWithSamples{}
	}
	(*cs).Count += completed.Count
	(*cs).appendSamples(completed.Samples)
}

// cleanupStats uses exported fields so zap.Any can encode them using their JSON tags.
type cleanupStats struct {
	DeletedFiles           int64             `json:"deleted-files,omitempty"`
	MissingTasks           *countWithSamples `json:"missing-tasks,omitempty"`
	MissingTaskFiles       *countWithSamples `json:"missing-task-files,omitempty"`
	NonImportIntoTaskFiles *countWithSamples `json:"non-import-into-task-files,omitempty"`
	UnparsedTaskIDFiles    *countWithSamples `json:"unparsed-task-id-files,omitempty"`
	Failures               int64             `json:"failures,omitempty"`
}

func (stats *cleanupStats) recordUnparsedTaskIDFiles(files []string) {
	recordCountWithSamples(&stats.UnparsedTaskIDFiles, files...)
}

func (stats *cleanupStats) recordTaskDiagnostics(
	taskID int64,
	files []string,
	info *storage.TaskCleanupInfo,
) {
	switch {
	case info == nil:
		// Conflict-row files should not outlive their task metadata. This is
		// rare, so retain bounded file samples to make the cleanup observable.
		stats.recordMissingTask(taskID, files)
	case info.Type != proto.ImportInto:
		// A task ID collision with another task type should also be rare. These
		// files cannot belong to IMPORT INTO, so record samples before deletion.
		stats.recordNonImportIntoTask(files)
	}
}

func (stats *cleanupStats) recordMissingTask(taskID int64, files []string) {
	recordCountWithSamples(&stats.MissingTasks, strconv.FormatInt(taskID, 10))
	recordCountWithSamples(&stats.MissingTaskFiles, files...)
}

func (stats *cleanupStats) recordNonImportIntoTask(files []string) {
	recordCountWithSamples(&stats.NonImportIntoTaskFiles, files...)
}

func (stats *cleanupStats) mergeCompletedFlush(completed cleanupStats) {
	stats.DeletedFiles += completed.DeletedFiles
	mergeCountWithSamples(&stats.MissingTasks, completed.MissingTasks)
	mergeCountWithSamples(&stats.MissingTaskFiles, completed.MissingTaskFiles)
	mergeCountWithSamples(&stats.NonImportIntoTaskFiles, completed.NonImportIntoTaskFiles)
	mergeCountWithSamples(&stats.UnparsedTaskIDFiles, completed.UnparsedTaskIDFiles)
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
		// Only successful task output is retained for user inspection. Failed or
		// reverted tasks may leave incomplete conflict-row files, so remove them
		// immediately with the task's other external artifacts.
		return true
	case proto.TaskStateSucceed:
		return info.EndTime != nil && !now.Before(info.EndTime.Add(retention))
	default:
		return false
	}
}

func cleanFiles(
	ctx context.Context,
	store storeapi.Storage,
	infoGetter TaskInfoGetter,
	now time.Time,
) (stats cleanupStats, err error) {
	defer func() {
		if err != nil {
			stats.Failures++
		}
		logutil.BgLogger().Info("finished conflict-row file cleanup", zap.Any("stats", stats))
	}()

	taskFiles := make(map[int64][]string, maxTaskIDsPerFlush)
	unparsedTaskIDFiles := make([]string, 0)
	fileCount := 0
	flush := func() error {
		if fileCount == 0 {
			return nil
		}
		taskIDs := make([]int64, 0, len(taskFiles))
		for taskID := range taskFiles {
			taskIDs = append(taskIDs, taskID)
		}
		slices.Sort(taskIDs)

		var infosByTaskID map[int64]*storage.TaskCleanupInfo
		if len(taskIDs) > 0 {
			infosByTaskID, err = infoGetter.GetTaskCleanupInfoByIDs(ctx, taskIDs)
			if err != nil {
				return err
			}
		}
		flushStats := cleanupStats{}
		flushStats.recordUnparsedTaskIDFiles(unparsedTaskIDFiles)
		filesToDelete := make([]string, 0, fileCount)
		filesToDelete = append(filesToDelete, unparsedTaskIDFiles...)
		for _, taskID := range taskIDs {
			files := taskFiles[taskID]
			info := infosByTaskID[taskID]
			shouldDeleteFiles := info == nil || info.Type != proto.ImportInto || shouldDelete(*info, now)
			flushStats.recordTaskDiagnostics(taskID, files, info)
			if shouldDeleteFiles {
				filesToDelete = append(filesToDelete, files...)
			}
		}
		if len(filesToDelete) > 0 {
			if err := store.DeleteFiles(ctx, filesToDelete); err != nil {
				return err
			}
			flushStats.DeletedFiles = int64(len(filesToDelete))
		}
		stats.mergeCompletedFlush(flushStats)

		clear(taskFiles)
		unparsedTaskIDFiles = unparsedTaskIDFiles[:0]
		fileCount = 0
		return nil
	}

	err = store.WalkDir(ctx, &storeapi.WalkOption{SubDir: storagePrefix}, func(name string, _ int64) error {
		taskID, ok := parseTaskID(name)
		if !ok {
			// IMPORT INTO always writes a positive task ID in this path. Malformed
			// entries are unexpected and rare, so delete them and retain samples.
			unparsedTaskIDFiles = append(unparsedTaskIDFiles, name)
		} else {
			taskFiles[taskID] = append(taskFiles[taskID], name)
		}
		fileCount++
		// Checking after insertion keeps one flush path and lets a batch exceed
		// either limit by at most one object or task.
		if fileCount > maxObjectsPerFlush || len(taskFiles) > maxTaskIDsPerFlush {
			return flush()
		}
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

// CleanConflictRowFiles applies the conflict-row cleanup policy. Files for failed
// or reverted tasks, files without matching IMPORT INTO task metadata, and file
// names without a valid positive task ID are deleted immediately. Files for
// successful tasks are deleted after the retention period; other states are retained.
func CleanConflictRowFiles(ctx context.Context, infoGetter TaskInfoGetter, cloudStorageURI string) error {
	if cloudStorageURI == "" {
		return nil
	}
	sortStore, err := importer.GetSortStore(ctx, cloudStorageURI)
	if err != nil {
		return err
	}
	defer sortStore.Close()
	_, err = cleanFiles(ctx, sortStore, infoGetter, time.Now())
	return err
}
