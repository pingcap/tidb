// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/pingcap/tidb/br/pkg/checkpoint"
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
	hash           [32]byte
	reallocRule    map[int64]int64
}

// New collects the requirement of prealloc IDs and return a
// not-yet-allocated PreallocIDs.
func New(tables []*metautil.Table) (*PreallocIDs, error) {
	if len(tables) == 0 {
		return &PreallocIDs{
			start: math.MaxInt64,
		}, nil
	}

	maxID := int64(0)
	count := int64(len(tables))
	ids := make([]int64, 0, len(tables))

	//TODO: (ris) sort all tables by ID, also in createTables
	for _, t := range tables {
		if t.Info.ID > maxID && t.Info.ID < InsaneTableIDThreshold {
			maxID = t.Info.ID
		}
		ids = append(ids, t.Info.ID)

		if t.Info.Partition != nil && t.Info.Partition.Definitions != nil {
			count += int64(len(t.Info.Partition.Definitions))
			for _, part := range t.Info.Partition.Definitions {
				if part.ID > maxID && part.ID < InsaneTableIDThreshold {
					maxID = part.ID
				}
				ids = append(ids, part.ID)
			}
		}
	}
	if maxID+count+1 > InsaneTableIDThreshold {
		return nil, errors.Errorf("table ID %d is too large", maxID)
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	reallocRule := make(map[int64]int64, len(ids))
	for idx, id := range ids {
		reallocRule[id] = int64(idx) + maxID + 1
	}

	return &PreallocIDs{
		start:          math.MaxInt64,
		reusableBorder: maxID + 1,
		count:          count,
		hash:           hashSortedIds(ids),
		reallocRule:    reallocRule,
	}, nil
}

func Reuse(lagacy *checkpoint.PreallocIDs, tables []*metautil.Table) (*PreallocIDs, error) {
	if lagacy == nil {
		return nil, errors.Errorf("no prealloc IDs to be reused")
	}

	count := int64(len(tables))
	if count != lagacy.Count {
		return nil, errors.Errorf("prealloc IDs count %d are not match with the tables count %d", lagacy.Count, count)
	}

	maxID := int64(0)
	ids := make([]int64, 0, len(tables))
	for _, t := range tables {
		if t.Info.ID > maxID && t.Info.ID < InsaneTableIDThreshold {
			maxID = t.Info.ID
		}
		ids = append(ids, t.Info.ID)

		if t.Info.Partition != nil && t.Info.Partition.Definitions != nil {
			count += int64(len(t.Info.Partition.Definitions))
			for _, part := range t.Info.Partition.Definitions {
				if part.ID > maxID && part.ID < InsaneTableIDThreshold {
					maxID = part.ID
				}
				ids = append(ids, part.ID)
			}
		}
	}

	if lagacy.ReusableBorder != maxID+1 {
		return nil, errors.Errorf("prealloc IDs reusable border %d are not match with the tables max ID %d", lagacy.ReusableBorder, maxID+1)
	}
	if maxID+count+1 > InsaneTableIDThreshold {
		return nil, errors.Errorf("table ID %d is too large", maxID)
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	hash := hashSortedIds(ids)
	if lagacy.Hash != hash {
		return nil, errors.Errorf("prealloc IDs hash %x are not match with the tables hash %x", lagacy.Hash, hash)
	}

	reallocRule := make(map[int64]int64, len(ids))
	for idx, id := range ids {
		reallocRule[id] = int64(idx) + lagacy.ReusableBorder
	}

	ret := PreallocIDs{
		start:          lagacy.Start,
		reusableBorder: lagacy.ReusableBorder,
		end:            lagacy.End,
		count:          lagacy.Count,
		hash:           lagacy.Hash,
		reallocRule:    reallocRule,
	}
	return &ret, nil
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
	return nil
}

func (p *PreallocIDs) allocID(originalID int64) (int64, error) {
	if originalID >= p.start && originalID < p.reusableBorder {
		return originalID, nil
	}

	rewriteID := p.reallocRule[originalID]
	if rewriteID == 0 {
		return 0, errors.Errorf("table ID %d is not in the range [%d, %d)", originalID, p.start, p.end)
	}
	if rewriteID >= p.end {
		return 0, errors.Errorf("table ID can't rewrite (%d -> %d), out of range [%d, %d)", originalID, rewriteID, p.start, p.end)
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

func (p *PreallocIDs) CreateCheckpoint() *checkpoint.PreallocIDs {
	if p == nil || p.start >= p.end {
		return nil
	}

	return &checkpoint.PreallocIDs{
		Start:          p.start,
		ReusableBorder: p.reusableBorder,
		End:            p.end,
		Count:          p.count,
		Hash:           p.hash,
	}
}

func hashSortedIds(ids []int64) [32]byte {
	h := sha256.New()
	buffer := make([]byte, 8)

	for _, id := range ids {
		binary.BigEndian.PutUint64(buffer, uint64(id))
		h.Write(buffer)
	}

	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}
