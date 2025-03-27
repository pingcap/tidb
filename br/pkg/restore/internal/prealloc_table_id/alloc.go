// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid

import (
	"fmt"
	"math"

	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pkg/errors"
)

const (
	// insaneTableIDThreshold is the threshold for "normal" table ID.
	// Sometimes there might be some tables with huge table ID.
	// For example, DDL metadata relative tables may have table ID up to 1 << 48.
	// When calculating the max table ID, we would ignore tables with table ID greater than this.
	// NOTE: In fact this could be just `1 << 48 - 1000` (the max available global ID),
	// however we are going to keep some gap here for some not-yet-known scenario, which means
	// at least, BR won't exhaust all global IDs.
	insaneTableIDThreshold = math.MaxUint32
)

// Allocator is the interface needed to allocate table IDs.
type Allocator interface {
	GetGlobalID() (int64, error)
	AdvanceGlobalIDs(n int) (int64, error)
}

// PreallocIDs mantains the state of preallocated table IDs.
// Not thread safe.
type PreallocIDs struct {
	start int64

	end int64

	used map[int64]struct{}

	next int64 //new added
}

// New collects the requirement of prealloc IDs and return a
// not-yet-allocated PreallocIDs.
func New(tables []*metautil.Table) *PreallocIDs {
	if len(tables) == 0 {
		return &PreallocIDs{
			start: math.MaxInt64,
		}
	}

	maxv := int64(0)

	for _, t := range tables {
		maxv += 1

		if t.Info.Partition != nil && t.Info.Partition.Definitions != nil {
			maxv += int64(len(t.Info.Partition.Definitions))
		}
	}
	return &PreallocIDs{
		start: math.MaxInt64,
		end: maxv,
		used:     make(map[int64]struct{}),
		next: math.MaxInt64,
	}
}

// String implements fmt.Stringer.
func (p *PreallocIDs) String() string {
	if p.start >= p.end {
		return fmt.Sprintf("ID:empty(end=%d)", p.end)
	}
	return fmt.Sprintf("ID:[%d,%d)", p.start, p.end)
}

// preallocTableIDs peralloc the id for [start, end)
func (p *PreallocIDs) Alloc(m Allocator) error {
	if p.end == 0 {
		return nil
	}

	alloced, err := m.AdvanceGlobalIDs(int(p.end))
	if err != nil {
		return err
	}
	p.start = alloced + 1
	p.end += p.start
	p.next = p.start
	return nil
}

func (p *PreallocIDs) allocID(originalID int64) (int64, error) {
    if int64(len(p.used)) >= p.end - p.start {
        return 0, errors.Errorf("no available IDs")
    }

    if originalID >= p.start && originalID < p.end {
        if _, exists := p.used[originalID]; !exists {
            p.used[originalID] = struct{}{}
            return originalID, nil
        }
    }

    start := p.next
    for {
        current := p.next
        p.next = (current + 1 - p.start) % (p.end - p.start) + p.start
        
        if _, exists := p.used[current]; !exists {
            p.used[current] = struct{}{}
            return current, nil
        }
        if p.next == start {
            break
        }
	}

    return 0, errors.Errorf("no available IDs")
}

func (p *PreallocIDs) RewriteTableInfo(info *model.TableInfo) (*model.TableInfo, error) {
	if info == nil {
		return nil, nil
	}
    infoCopy := info.Clone()

    newID, err := p.allocID(info.ID)
    if err != nil {
        return nil, errors.Wrapf(err, "failed to allocate table ID for %d", info.ID)
    }
    infoCopy.ID = newID

    if infoCopy.Partition != nil {
        for i := range infoCopy.Partition.Definitions {
            def := &infoCopy.Partition.Definitions[i]
            newPartID, err := p.allocID(def.ID)
            if err != nil {
                return nil, errors.Wrapf(err, "failed to allocate partition ID for %d", def.ID)
            }
            def.ID = newPartID
        }
    }

    return infoCopy, nil
}

// Only used in test, mock the behavior of Batch create tables.
func (p *PreallocIDs) BatchAlloc(tables []*metautil.Table) (map[string][]*model.TableInfo, error) {
	clonedInfos := make(map[string][]*model.TableInfo, len(tables))
	if len(tables) == 0 {
		return clonedInfos, nil
	}

	for _, t := range tables {
		infoClone, err := p.RewriteTableInfo(t.Info)
		if err != nil {
			return nil, err
		}
		clonedInfos[t.DB.Name.L] = append(clonedInfos[t.DB.Name.L], infoClone)
	}

	return clonedInfos, nil
}
