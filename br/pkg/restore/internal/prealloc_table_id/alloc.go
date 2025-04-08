// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid

import (
	"fmt"
	"math"
	"sync"

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
	mu             sync.Mutex
	start          int64
	reusableBorder int64
	end            int64
	count          int64
	used           map[int64]struct{}
	next           int64
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
		used:           make(map[int64]struct{}, count),
		next:           math.MaxInt64,
	}
}

// String implements fmt.Stringer.
func (p *PreallocIDs) String() string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.start >= p.end {
		return fmt.Sprintf("ID:empty(end=%d)", p.end)
	}
	return fmt.Sprintf("ID:[%d,%d)", p.start, p.end)
}

func (p *PreallocIDs) GetIDRange() (int64, int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.start, p.end
}

// preallocTableIDs peralloc the id for [start, end)
func (p *PreallocIDs) Alloc(m Allocator) error {
	p.mu.Lock()
	defer p.mu.Unlock()

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
	if p.reusableBorder <= currentID+1 {
		p.reusableBorder = currentID + 1
	}
	idRnage := p.reusableBorder - (currentID + 1) + p.count
	if _, err := m.AdvanceGlobalIDs(int(idRnage)); err != nil {
		return err
	}

	p.start = currentID + 1
	p.end = p.start + idRnage
	p.next = p.reusableBorder
	return nil
}

func (p *PreallocIDs) allocID(originalID int64) (int64, error) {
	if int64(len(p.used)) >= p.end-p.start {
		return 0, errors.Errorf("no available IDs")
	}

	if originalID >= p.start && originalID < p.reusableBorder {
		if _, exists := p.used[originalID]; !exists {
			p.used[originalID] = struct{}{}
			return originalID, nil
		}
	}

	start := p.next
	for {
		current := p.next
		p.next = (current+1-p.reusableBorder)%(p.end-p.reusableBorder) + p.reusableBorder

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
		return nil, errors.Errorf("table info is nil")
	}
	infoCopy := info.Clone()

	p.mu.Lock()
	defer p.mu.Unlock()

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
