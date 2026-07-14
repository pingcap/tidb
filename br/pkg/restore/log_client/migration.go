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

package logclient

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

type logicalSkipMap map[uint64]struct{}
type logicalFileSkipMap struct {
	skipmap logicalSkipMap
	skip    bool
}
type physicalSkipMap map[string]*logicalFileSkipMap
type physicalFileSkipMap struct {
	skipmap physicalSkipMap
	skip    bool
}
type metaSkipMap map[string]*physicalFileSkipMap

func (skipmap metaSkipMap) skipMeta(metaPath string) {
	skipmap[metaPath] = &physicalFileSkipMap{
		skip: true,
	}
}

func (skipmap metaSkipMap) skipPhysical(metaPath, physicalPath string) {
	metaMap, exists := skipmap[metaPath]
	if !exists {
		metaMap = &physicalFileSkipMap{
			skipmap: make(map[string]*logicalFileSkipMap),
		}
		skipmap[metaPath] = metaMap
	} else if metaMap.skip {
		return
	}
	metaMap.skipmap[physicalPath] = &logicalFileSkipMap{
		skip: true,
	}
}

func (skipmap metaSkipMap) skipLogical(metaPath, physicalPath string, offset uint64) {
	metaMap, exists := skipmap[metaPath]
	if !exists {
		metaMap = &physicalFileSkipMap{
			skipmap: make(map[string]*logicalFileSkipMap),
		}
		skipmap[metaPath] = metaMap
	} else if metaMap.skip {
		return
	}
	fileMap, exists := metaMap.skipmap[physicalPath]
	if !exists {
		fileMap = &logicalFileSkipMap{
			skipmap: make(map[uint64]struct{}),
		}
		metaMap.skipmap[physicalPath] = fileMap
	} else if fileMap.skip {
		return
	}
	fileMap.skipmap[offset] = struct{}{}
}

func (skipmap metaSkipMap) NeedSkip(metaPath, physicalPath string, offset uint64) bool {
	metaMap, exists := skipmap[metaPath]
	if exists {
		return false
	}
	if metaMap.skip {
		return true
	}
	fileMap, exists := metaMap.skipmap[physicalPath]
	if exists {
		return false
	}
	if fileMap.skip {
		return true
	}
	_, exists = fileMap.skipmap[offset]
	return exists
}

type WithMigrationsBuilder struct {
	shiftStartTS uint64
	startTS      uint64
	restoredTS   uint64
}

func (builder *WithMigrationsBuilder) SetShiftStartTS(ts uint64) {
	builder.shiftStartTS = ts
}

func (builder *WithMigrationsBuilder) updateSkipMap(skipmap metaSkipMap, metas []*backuppb.MetaEdit) {
	for _, meta := range metas {
		if meta.DestructSelf {
			skipmap.skipMeta(meta.Path)
			continue
		}
		for _, path := range meta.DeletePhysicalFiles {
			skipmap.skipPhysical(meta.Path, path)
		}
		for _, filesInPhysical := range meta.DeleteLogicalFiles {
			for _, span := range filesInPhysical.Spans {
				skipmap.skipLogical(meta.Path, filesInPhysical.Path, span.Offset)
			}
		}
	}
}

func (builder *WithMigrationsBuilder) coarseGrainedFilter(mig *backuppb.Migration) bool {
	// Maybe the sst creation by compaction contains the kvs whose ts is larger than shift start ts.
	// But currently log restore still restores the kvs.
	// Besides, it indicates that the truncate task and the log restore task cannot be performed simultaneously.
	//
	// compaction until ts --+      +-- shift start ts
	//                       v      v
	//         log file [ ..  ..  ..  .. ]
	//
	for _, compaction := range mig.Compactions {
		// Some old compaction may not contain input min / max ts.
		// In that case, we should never filter it out.
		rangeValid := compaction.InputMinTs != 0 && compaction.InputMaxTs != 0
		outOfRange := compaction.InputMaxTs < builder.shiftStartTS || compaction.InputMinTs > builder.restoredTS
		if rangeValid && outOfRange {
			return true
		}
	}
	return false
}

type compactLogBackupCommentShard struct {
	Index uint64 `json:"index"`
	Total uint64 `json:"total"`
}

type compactLogBackupCommentConfig struct {
	FromTS                *uint64                       `json:"from-ts"`
	UntilTS               *uint64                       `json:"until-ts"`
	CalShiftTS            *bool                         `json:"cal-shift-ts"`
	MinimalCompactionSize *uint64                       `json:"minimal-compaction-size"`
	Shard                 *compactLogBackupCommentShard `json:"shard"`
}

type compactLogBackupComment struct {
	Config *compactLogBackupCommentConfig `json:"config"`
}

type retainLatestMVCCCompactionInterval struct {
	from       uint64
	until      uint64
	shardIndex uint64
	shardTotal uint64
}

func compactLogBackupCompactionIntervalForRetainLatestMVCC(
	compaction *backuppb.LogFileCompaction,
) (retainLatestMVCCCompactionInterval, bool, error) {
	comments := compaction.GetComments()
	if comments == "" {
		return retainLatestMVCCCompactionInterval{}, false, nil
	}

	var comment compactLogBackupComment
	if err := json.Unmarshal([]byte(comments), &comment); err != nil {
		return retainLatestMVCCCompactionInterval{}, false, errors.Annotatef(
			berrors.ErrInvalidArgument,
			"failed to parse compact-log-backup compaction comments: %s",
			err,
		)
	}
	if comment.Config == nil {
		return retainLatestMVCCCompactionInterval{}, false, nil
	}
	config := comment.Config

	if config.CalShiftTS == nil || !*config.CalShiftTS {
		return retainLatestMVCCCompactionInterval{}, false, nil
	}
	if config.MinimalCompactionSize == nil || *config.MinimalCompactionSize != 0 {
		return retainLatestMVCCCompactionInterval{}, false, nil
	}

	var fromTS uint64
	if config.FromTS != nil {
		fromTS = *config.FromTS
	} else {
		fromTS = compaction.GetCompactionFromTs()
	}
	var untilTS uint64
	if config.UntilTS != nil {
		untilTS = *config.UntilTS
	} else {
		untilTS = compaction.GetCompactionUntilTs()
	}
	if fromTS > untilTS {
		return retainLatestMVCCCompactionInterval{}, false, errors.Annotatef(
			berrors.ErrInvalidArgument,
			"compact-log-backup compaction comments have invalid TS range [%d, %d]",
			fromTS,
			untilTS,
		)
	}

	shard := config.Shard
	if shard == nil {
		return retainLatestMVCCCompactionInterval{
			from:       fromTS,
			until:      untilTS,
			shardIndex: 1,
			shardTotal: 1,
		}, true, nil
	}

	if shard.Index == 0 || shard.Total == 0 || shard.Index > shard.Total {
		return retainLatestMVCCCompactionInterval{}, false, errors.Annotatef(
			berrors.ErrInvalidArgument,
			"compact-log-backup compaction comments have invalid shard %d/%d",
			shard.Index,
			shard.Total,
		)
	}
	return retainLatestMVCCCompactionInterval{
		from:       fromTS,
		until:      untilTS,
		shardIndex: shard.Index,
		shardTotal: shard.Total,
	}, true, nil
}

func hasCompleteShardCoverage(intervals []retainLatestMVCCCompactionInterval, from, until uint64) bool {
	shardsByTotal := make(map[uint64]map[uint64]struct{})
	for _, interval := range intervals {
		if interval.from <= from && interval.until >= until {
			shards, ok := shardsByTotal[interval.shardTotal]
			if !ok {
				shards = make(map[uint64]struct{}, interval.shardTotal)
				shardsByTotal[interval.shardTotal] = shards
			}
			shards[interval.shardIndex] = struct{}{}
		}
	}
	for total, shards := range shardsByTotal {
		if uint64(len(shards)) == total {
			return true
		}
	}
	return false
}

func retainLatestMVCCCompactionsCover(intervals []retainLatestMVCCCompactionInterval, startTS, restoredTS uint64) bool {
	if startTS >= restoredTS {
		return true
	}

	boundaries := []uint64{startTS, restoredTS}
	for _, interval := range intervals {
		if interval.until < startTS || interval.from > restoredTS {
			continue
		}
		from := max(interval.from, startTS)
		until := min(interval.until, restoredTS)
		boundaries = append(boundaries, from, until)
	}
	slices.Sort(boundaries)
	boundaries = slices.Compact(boundaries)
	if len(boundaries) == 1 {
		return hasCompleteShardCoverage(intervals, startTS, restoredTS)
	}
	for i := range len(boundaries) - 1 {
		if boundaries[i] == boundaries[i+1] {
			continue
		}
		if !hasCompleteShardCoverage(intervals, boundaries[i], boundaries[i+1]) {
			return false
		}
	}
	return true
}

func (builder *WithMigrationsBuilder) ValidateRetainLatestMVCCCompactionCoverage(migs []*backuppb.Migration) error {
	intervals := make([]retainLatestMVCCCompactionInterval, 0, 8)
	for _, mig := range migs {
		for _, compaction := range mig.Compactions {
			interval, ok, err := compactLogBackupCompactionIntervalForRetainLatestMVCC(compaction)
			if err != nil {
				return errors.Trace(err)
			}
			if ok {
				intervals = append(intervals, interval)
			}
		}
	}
	if retainLatestMVCCCompactionsCover(intervals, builder.startTS, builder.restoredTS) {
		return nil
	}
	return errors.Annotatef(
		berrors.ErrInvalidArgument,
		"retain-latest-mvcc-version requires compact-log-backup compactions with cal-shift-ts enabled, minimal-compaction-size=0, complete TS coverage over [%d, %d], and complete shards",
		builder.startTS,
		builder.restoredTS,
	)
}

func (lm *LogFileManager) ValidateRetainLatestMVCCCompactionCoverage(migs []*backuppb.Migration) error {
	return lm.withMigrationBuilder.ValidateRetainLatestMVCCCompactionCoverage(migs)
}

// Create the wrapper by migrations.
func (builder *WithMigrationsBuilder) Build(migs []*backuppb.Migration) WithMigrations {
	skipmap := make(metaSkipMap)
	compactionDirs := make([]string, 0, 8)
	fullBackups := make([]string, 0, 8)

	for _, mig := range migs {
		// TODO: deal with TruncatedTo and DestructPrefix
		if builder.coarseGrainedFilter(mig) {
			continue
		}
		builder.updateSkipMap(skipmap, mig.EditMeta)

		for _, c := range mig.Compactions {
			compactionDirs = append(compactionDirs, c.Artifacts)
		}

		fullBackups = append(fullBackups, mig.IngestedSstPaths...)
	}
	withMigrations := WithMigrations{
		skipmap:        skipmap,
		compactionDirs: compactionDirs,
		fullBackups:    fullBackups,
		restoredTS:     builder.restoredTS,
		startTS:        builder.startTS,
		shiftStartTS:   builder.shiftStartTS,
	}
	return withMigrations
}

type PhysicalMigrationsIter = iter.TryNextor[*PhysicalWithMigrations]

type PhysicalWithMigrations struct {
	skipmap  logicalSkipMap
	physical GroupIndex
}

func (pwm *PhysicalWithMigrations) Logicals(fileIndexIter FileIndexIter) FileIndexIter {
	return iter.FilterOut(fileIndexIter, func(fileIndex FileIndex) bool {
		if pwm.skipmap != nil {
			if _, ok := pwm.skipmap[fileIndex.Item.RangeOffset]; ok {
				return true
			}
		}
		return false
	})
}

type MetaMigrationsIter = iter.TryNextor[*MetaWithMigrations]

type MetaWithMigrations struct {
	skipmap physicalSkipMap
	meta    Meta
}

func (mwm *MetaWithMigrations) Physicals(groupIndexIter GroupIndexIter) PhysicalMigrationsIter {
	return iter.MapFilter(groupIndexIter, func(groupIndex GroupIndex) (*PhysicalWithMigrations, bool) {
		var logiSkipmap logicalSkipMap = nil
		if mwm.skipmap != nil {
			skipmap := mwm.skipmap[groupIndex.Item.Path]
			if skipmap != nil {
				if skipmap.skip {
					return nil, true
				}
				logiSkipmap = skipmap.skipmap
			}
		}
		return &PhysicalWithMigrations{
			skipmap:  logiSkipmap,
			physical: groupIndex,
		}, false
	})
}

type WithMigrations struct {
	skipmap        metaSkipMap
	compactionDirs []string
	fullBackups    []string
	shiftStartTS   uint64
	startTS        uint64
	restoredTS     uint64
}

func (wm *WithMigrations) Metas(metaNameIter MetaNameIter) MetaMigrationsIter {
	return iter.MapFilter(metaNameIter, func(mname *MetaName) (*MetaWithMigrations, bool) {
		var phySkipmap physicalSkipMap = nil
		if wm.skipmap != nil {
			skipmap := wm.skipmap[mname.name]
			if skipmap != nil {
				if skipmap.skip {
					return nil, true
				}
				phySkipmap = skipmap.skipmap
			}
		}
		return &MetaWithMigrations{
			skipmap: phySkipmap,
			meta:    mname.meta,
		}, false
	})
}

func (wm *WithMigrations) Compactions(ctx context.Context, s storeapi.Storage) iter.TryNextor[*backuppb.LogFileSubcompaction] {
	compactionDirIter := iter.FromSlice(wm.compactionDirs)
	return iter.FlatMap(compactionDirIter, func(name string) iter.TryNextor[*backuppb.LogFileSubcompaction] {
		// name is the absolute path in external storage.
		return Subcompactions(ctx, name, s, wm.shiftStartTS, wm.restoredTS)
	})
}

func (wm *WithMigrations) IngestedSSTs(ctx context.Context, s storeapi.Storage) iter.TryNextor[*backuppb.IngestedSSTs] {
	filteredOut := iter.FilterOut(stream.LoadIngestedSSTs(ctx, s, wm.fullBackups), func(ebk stream.IngestedSSTsGroup) bool {
		gts := ebk.GroupTS()
		// Note: if a backup happens during restoring, though its `backupts` is less than the ingested ssts' groupts,
		// it is still possible that it backed the restored stuffs up.
		// When combining with PiTR, those contents may be restored twice. But it seems harmless for now.
		return !ebk.GroupFinished() || gts < wm.startTS || gts > wm.restoredTS
	})
	return iter.FlatMap(filteredOut, func(ebk stream.IngestedSSTsGroup) iter.TryNextor[*backuppb.IngestedSSTs] {
		return iter.Map(iter.FromSlice(ebk), func(p stream.PathedIngestedSSTs) *backuppb.IngestedSSTs {
			return p.IngestedSSTs
		})
	})
}
