// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid

import (
	"fmt"
	"math"

	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/meta"
)

type PreallocIDs struct {
	end int64

	allocedFrom int64
}

func New(tables []*metautil.Table) *PreallocIDs {
	if len(tables) == 0 {
		return &PreallocIDs{
			allocedFrom: math.MaxInt64,
		}
	}

	max := int64(0)

	for _, t := range tables {
		if t.Info.ID > max {
			max = t.Info.ID
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
func (p *PreallocIDs) Alloc(m *meta.Meta) error {
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

func (p *PreallocIDs) Prealloced(tid int64) bool {
	return p.allocedFrom <= tid && tid < p.end
}
