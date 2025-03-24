// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid

import (
	"fmt"
	"math"

	"github.com/pingcap/tidb/br/pkg/metautil"
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
	end int64

	allocedFrom int64

	alloced map[int64]struct{}
}

// New collects the requirement of prealloc IDs and return a
// not-yet-allocated PreallocIDs.
func New(tables []*metautil.Table) *PreallocIDs {
	if len(tables) == 0 {
		return &PreallocIDs{
			allocedFrom: math.MaxInt64,
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
		end: maxv,

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
	if p.end == 0 {
		return nil
	}

	alloced, err := m.AdvanceGlobalIDs(int(p.end))
	if err != nil {
		return err
	}
	p.allocedFrom = alloced + 1
	p.end += p.allocedFrom
	p.alloced = make(map[int64]struct{})
	return nil
}

// Prealloced checks whether a table ID has been successfully allocated.
func (p *PreallocIDs) Prealloced(tid int64) bool {
	return p.allocedFrom <= tid && tid < p.end
}

// func (p *PreallocIDs) PreallocedFor(ti *model.TableInfo) {}

func (p *PreallocIDs) BatchAlloc(ids []int64) ([]int64,error) {
	if len(ids) > int(p.end-p.allocedFrom) {
		return []int64{}, fmt.Errorf("batch alloc too many IDs")
	}

	result := make([]int64, len(ids))
    fillIndices := make([]int, 0, len(ids))

    for i, id := range ids {
        if id >= p.allocedFrom && id < p.end {
            if _, exists := p.alloced[id]; !exists {
                p.alloced[id] = struct{}{}
                result[i] = id
                continue
            }
        }
        fillIndices = append(fillIndices, i)
	}

	fillCount := len(fillIndices)
    if fillCount == 0 {
        return result, nil
    }

    available := make([]int64, 0)
    for id := p.allocedFrom; id < p.end; id++ {
        if _, exists := p.alloced[id]; !exists {
            available = append(available, id)
        }
	}

	for i, idx := range fillIndices {
		id := available[i]
        p.alloced[id] = struct{}{}
        result[idx] = id
	}

	return result, nil
}
