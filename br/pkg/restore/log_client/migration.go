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
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
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
		if compaction.CompactionUntilTs < builder.shiftStartTS || compaction.CompactionFromTs > builder.restoredTS {
			return true
		}
	}
	return false
}

// Create the wrapper by migrations.
func (builder *WithMigrationsBuilder) Build(migs []*backuppb.Migration) WithMigrations {
	skipmap := make(metaSkipMap)
	for _, mig := range migs {
		// TODO: deal with TruncatedTo and DestructPrefix
		if builder.coarseGrainedFilter(mig) {
			continue
		}
		builder.updateSkipMap(skipmap, mig.EditMeta)
	}
	return WithMigrations(skipmap)
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

type WithMigrations metaSkipMap

func (wm WithMigrations) Metas(metaNameIter MetaNameIter) MetaMigrationsIter {
	return iter.MapFilter(metaNameIter, func(mname *MetaName) (*MetaWithMigrations, bool) {
		var phySkipmap physicalSkipMap = nil
		if wm != nil {
			skipmap := wm[mname.name]
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
