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

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
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

func (wm *WithMigrations) Compactions(ctx context.Context, s storage.ExternalStorage) iter.TryNextor[*backuppb.LogFileSubcompaction] {
	compactionDirIter := iter.FromSlice(wm.compactionDirs)
	return iter.FlatMap(compactionDirIter, func(name string) iter.TryNextor[*backuppb.LogFileSubcompaction] {
		// name is the absolute path in external storage.
		return Subcompactions(ctx, name, s, wm.shiftStartTS, wm.restoredTS)
	})
}

func (wm *WithMigrations) IngestedSSTs(ctx context.Context, s storage.ExternalStorage) iter.TryNextor[*backuppb.IngestedSSTs] {
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
