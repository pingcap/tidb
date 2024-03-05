// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid

import (
	"fmt"
	"math"

	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/parser/model"
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
type PreallocIDs struct {
	end int64

	allocedFrom int64
}

// New collects the requirement of prealloc IDs and return a
// not-yet-allocated PreallocIDs.
func New(tables []*metautil.Table) *PreallocIDs {
	if len(tables) == 0 {
		return &PreallocIDs{
			allocedFrom: math.MaxInt64,
		}
	}

	max := int64(0)

	for _, t := range tables {
		if t.Info.ID > max && t.Info.ID < insaneTableIDThreshold {
			max = t.Info.ID
		}

		if t.Info.Partition != nil && t.Info.Partition.Definitions != nil {
			for _, part := range t.Info.Partition.Definitions {
				if part.ID > max && part.ID < insaneTableIDThreshold {
					max = part.ID
				}
			}
		}
	}
	return &PreallocIDs{
		end: max + 1,

		allocedFrom: math.MaxInt64,
	}
}

// String implements fmt.Stringer.
func (p *PreallocIDs) String() string {
	if p.allocedFrom >= p.end {
		return fmt.Sprintf("ID:empty(end=%d)", p.end)
	}
	return fmt.Sprintf("ID:[%d,%d)", p.allocedFrom, p.end)
}

// preallocTableIDs peralloc the id for [start, end)
func (p *PreallocIDs) Alloc(m Allocator) error {
	currentId, err := m.GetGlobalID()
	if err != nil {
		return err
	}
	if currentId > p.end {
		return nil
	}

	alloced, err := m.AdvanceGlobalIDs(int(p.end - currentId))
	if err != nil {
		return err
	}
	p.allocedFrom = alloced
	return nil
}

// Prealloced checks whether a table ID has been successfully allocated.
func (p *PreallocIDs) Prealloced(tid int64) bool {
	return p.allocedFrom <= tid && tid < p.end
}

func (p *PreallocIDs) PreallocedFor(ti *model.TableInfo) bool {
	if !p.Prealloced(ti.ID) {
		return false
	}
	if ti.Partition != nil && ti.Partition.Definitions != nil {
		for _, part := range ti.Partition.Definitions {
			if !p.Prealloced(part.ID) {
				return false
			}
		}
	}
	return true
}
