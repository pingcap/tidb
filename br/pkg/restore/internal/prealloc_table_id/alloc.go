// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid

import (
	"fmt"
	"math"

	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/meta/model"
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
		end:         maxv,
		allocedFrom: math.MaxInt64,
		alloced:     make(map[int64]struct{}),
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
	return nil
}

func (p *PreallocIDs) RewriteTableInfoInplace(idMap map[int64]*int64) error {
	available := p.end - p.allocedFrom
	if available < 0 {
		return fmt.Errorf("invalid state: available IDs (%d) cannot be negative", available)
	}
	if int64(len(idMap)) > available {
		return fmt.Errorf("need alloc %d IDs but only %d available", len(idMap), available)
	}
	needRewrite := make([]*int64, 0, len(idMap))
	dups := make(map[int64]struct{})
	for upstreamID, ptr := range idMap {
		if upstreamID >= p.allocedFrom && upstreamID < p.end {
			if _, exists := p.alloced[upstreamID]; !exists {
				p.alloced[upstreamID] = struct{}{}
				*ptr = upstreamID
				continue
			}

			// will there be duplicated upstreamID?
			if _, exists := dups[upstreamID]; exists {
				return fmt.Errorf("duplicate upstream ID: %d", upstreamID)
			}
			dups[upstreamID] = struct{}{}
		}
		needRewrite = append(needRewrite, ptr)
	}

	if int64(len(needRewrite)) > (available - int64(len(p.alloced))) {
		return fmt.Errorf("need alloc %d IDs but only %d available", len(needRewrite), (available - int64(len(p.alloced))))
	}

	current := p.allocedFrom
	for _, ptr := range needRewrite {
		for ; current < p.end; current++ {
			if _, exists := p.alloced[current]; !exists {
				p.alloced[current] = struct{}{}
				*ptr = current
				current++
				break
			}
		}
	}
	return nil
}

func (p *PreallocIDs) BatchAlloc(tables []*metautil.Table) (map[string][]*model.TableInfo, error) {
	clonedInfos := make(map[string][]*model.TableInfo, len(tables))
	if len(tables) == 0 {
		return clonedInfos, nil
	}

	idMapping := make(map[int64]*int64)
	for _, t := range tables {
		infoClone := t.Info.Clone()
		originalID := infoClone.ID
		idMapping[originalID] = &infoClone.ID
		if partition := infoClone.Partition; partition != nil {
			for i := range partition.Definitions {
				def := &partition.Definitions[i]
				idMapping[def.ID] = &def.ID
			}
		}
		clonedInfos[t.DB.Name.L] = append(clonedInfos[t.DB.Name.L], infoClone)
	}

	if err := p.RewriteTableInfoInplace(idMapping); err != nil {
		return clonedInfos, err
	}
	return clonedInfos, nil
}
