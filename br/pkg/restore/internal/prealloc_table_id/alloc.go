// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/meta/model"
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
	hash           [32]byte
	unallocedIDs   []int64
	allocRule      map[int64]int64
}

func NewAndPrealloc(tables []*metautil.Table, m Allocator) (*PreallocIDs, error) {
	if len(tables) == 0 {
		return &PreallocIDs{
			start: math.MaxInt64,
		}, nil
	}
	preallocIDs, err := New(tables)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create preallocIDs")
	}
	if err := preallocIDs.PreallocIDs(m); err != nil {
		return nil, errors.Wrap(err, "failed to allocate prealloc IDs")
	}
	return preallocIDs, nil
}

// collectTableIDs collects table and partition IDs from the given tables.
// Returns the maximum ID and a sorted slice of IDs.
func collectTableIDs(tables []*metautil.Table) (int64, []int64, error) {
	maxID := int64(0)
	ids := make([]int64, 0, len(tables))

	for _, t := range tables {
		if t.Info.ID > maxID && t.Info.ID < InsaneTableIDThreshold {
			maxID = t.Info.ID
		}
		ids = append(ids, t.Info.ID)

		if t.Info.Partition != nil && t.Info.Partition.Definitions != nil {
			for _, part := range t.Info.Partition.Definitions {
				if part.ID > maxID && part.ID < InsaneTableIDThreshold {
					maxID = part.ID
				}
				ids = append(ids, part.ID)
			}
		}
	}

	if maxID+int64(len(ids))+1 > InsaneTableIDThreshold {
		return 0, nil, errors.Errorf("table ID %d is too large", maxID)
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return maxID, ids, nil
}

// New collects the requirement of prealloc IDs and returns a not-yet-allocated PreallocIDs.
func New(tables []*metautil.Table) (*PreallocIDs, error) {
	if len(tables) == 0 {
		return &PreallocIDs{
			start: math.MaxInt64,
		}, nil
	}

	maxID, unallocedIDs, err := collectTableIDs(tables)
	if err != nil {
		return nil, err
	}

	return &PreallocIDs{
		start:          math.MaxInt64,
		reusableBorder: maxID + 1,
		hash:           computeSortedIDsHash(unallocedIDs),
		unallocedIDs:   unallocedIDs,
		allocRule:      make(map[int64]int64, len(unallocedIDs)),
	}, nil
}

func ReuseCheckpoint(legacy *checkpoint.PreallocIDs, tables []*metautil.Table) (*PreallocIDs, error) {
	if legacy == nil {
		return nil, errors.Errorf("no prealloc IDs to be reused")
	}

	maxID, ids, err := collectTableIDs(tables)
	if err != nil {
		return nil, err
	}

	if legacy.ReusableBorder < maxID+1 {
		return nil, errors.Annotatef(berrors.ErrInvalidRange, "prealloc IDs reusable border %d does not match with the tables max ID %d", legacy.ReusableBorder, maxID+1)
	}
	if legacy.Hash != computeSortedIDsHash(ids) {
		return nil, errors.Annotatef(berrors.ErrInvalidRange, "prealloc IDs hash mismatch")
	}

	allocRule := make(map[int64]int64, len(ids))
	rewriteCnt := int64(0)
	for _, id := range ids {
		if id < legacy.Start || id > InsaneTableIDThreshold {
			allocRule[id] = legacy.ReusableBorder + rewriteCnt
			rewriteCnt++
		} else if id < legacy.ReusableBorder {
			allocRule[id] = id
		} else {
			return nil, errors.Annotatef(berrors.ErrInvalidRange, "table ID %d is out of range [%d, %d)", id, legacy.Start, legacy.ReusableBorder)
		}
	}

	return &PreallocIDs{
		start:          legacy.Start,
		reusableBorder: legacy.ReusableBorder,
		end:            legacy.End,
		hash:           legacy.Hash,
		allocRule:      allocRule,
	}, nil
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
func (p *PreallocIDs) PreallocIDs(m Allocator) error {
	if len(p.unallocedIDs) == 0 {
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

	rewriteCnt := int64(0)
	for _, id := range p.unallocedIDs {
		if id >= p.start && id < InsaneTableIDThreshold {
			p.allocRule[id] = id
			continue
		}
		p.allocRule[id] = p.reusableBorder + rewriteCnt
		rewriteCnt++
	}
	idRange := p.reusableBorder - p.start + rewriteCnt
	if _, err := m.AdvanceGlobalIDs(int(idRange)); err != nil {
		return err
	}
	p.end = p.start + idRange
	p.unallocedIDs = nil

	return nil
}

func (p *PreallocIDs) allocID(originalID int64) (int64, error) {
	if p.unallocedIDs != nil {
		return 0, errors.Errorf("table ID %d is not allocated yet", originalID)
	}
	rewriteID := p.allocRule[originalID]
	if rewriteID < p.start || rewriteID >= p.end {
		return 0, errors.Errorf("table ID %d is not in range [%d, %d)", rewriteID, p.start, p.end)
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
		Hash:           p.hash,
	}
}

func computeSortedIDsHash(ids []int64) [32]byte {
	h := sha256.New()
	buffer := make([]byte, 8)

	for _, id := range ids {
		binary.BigEndian.PutUint64(buffer, uint64(id))
		_, err := h.Write(buffer)
		if err != nil {
			panic(errors.Wrapf(err, "failed to write table ID %d to hash", id))
		}
	}

	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}
