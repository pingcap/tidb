// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pkg/errors"
)

const (
	// InsaneTableIDThreshold is the threshold for "normal" table ID.
	// Sometimes there might be some tables with huge table ID.
	// For example, DDL metadata relative tables may have table ID up to 1 << 48.
	// When calculating the max table ID, we would ignore tables with table ID greater than this.
	// NOTE: In fact this could be just `1 << 48 - 1000` (the max available global ID),
	// however we are going to keep some gap here for some not-yet-known scenario, which means
	// at least, BR won't exhaust all global IDs.
	InsaneTableIDThreshold = math.MaxUint32
)

// Allocator is the interface needed to allocate table IDs.
type Allocator interface {
	GetGlobalID() (int64, error)
	AdvanceGlobalIDs(n int) (int64, error)
}

// PreallocIDs mantains the state of preallocated table IDs.
type PreallocIDs struct {
	start          int64
	reusableBorder int64
	end            int64
	count          int64
	next           atomic.Int64
}

// New collects the requirement of prealloc IDs and return a
// not-yet-allocated PreallocIDs.
func New(tables []*metautil.Table) *PreallocIDs {
	if len(tables) == 0 {
		return &PreallocIDs{
			start: math.MaxInt64,
		}
	}

	maxID := int64(0)
	count := int64(len(tables))

	for _, t := range tables {
		if t.Info.ID > maxID && t.Info.ID < InsaneTableIDThreshold {
			maxID = t.Info.ID
		}

		if t.Info.Partition != nil && t.Info.Partition.Definitions != nil {
			count += int64(len(t.Info.Partition.Definitions))
			for _, part := range t.Info.Partition.Definitions {
				if part.ID > maxID && part.ID < InsaneTableIDThreshold {
					maxID = part.ID
				}
			}
		}
	}
	if maxID+count+1 > InsaneTableIDThreshold {
		return nil
	}
	return &PreallocIDs{
		start:          math.MaxInt64,
		reusableBorder: maxID + 1,
		count:          count,
		next:           atomic.Int64{},
	}
}

// String implements fmt.Stringer.
func (p *PreallocIDs) String() string {
	if p.start >= p.end {
		return fmt.Sprintf("ID:empty(end=%d)", p.end)
	}
	return fmt.Sprintf("ID:[%d,%d)", p.start, p.end)
}

func (p *PreallocIDs) GetIDRange() (int64, int64) {
	return p.start, p.end
}

// preallocTableIDs peralloc the id for [start, end)
func (p *PreallocIDs) Alloc(m Allocator) error {
	if p.count == 0 {
		return nil
	}
	if p.start < p.end {
		return errors.Errorf("table ID should only be allocated once")
	}

	currentID, err := m.GetGlobalID()
	if err != nil {
		return err
	}
	p.start = currentID + 1

	if p.reusableBorder <= p.start {
		p.reusableBorder = p.start
	}
	idRange := p.reusableBorder - p.start + p.count
	if _, err := m.AdvanceGlobalIDs(int(idRange)); err != nil {
		return err
	}

	p.end = p.start + idRange
	p.next.Store(p.reusableBorder)
	return nil
}

func (p *PreallocIDs) allocID(originalID int64) (int64, error) {
	if originalID >= p.start && originalID < p.reusableBorder {
		return originalID, nil
	}

	rewriteID := p.next.Add(1) - 1
	if rewriteID >= p.end {
		return 0, errors.Errorf("no available IDs")
	}
	return rewriteID, nil
}

func (p *PreallocIDs) RewriteTableInfo(info *model.TableInfo) (*model.TableInfo, error) {
	if info == nil {
		return nil, errors.Errorf("table info is nil")
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
