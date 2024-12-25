// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package prealloctableid

import (
	"cmp"
	"fmt"
	"math"
	"slices"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
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
// The table ID in [preallocedFrom, preallocedEnd) can be reused.
type PreallocIDs struct {
	// Suppose we have a table ID set IDs = {ID0, ID1, ... , IDn}.
	// And there is an index x, {IDx, IDx+1, .., IDn} canbe reused.
	// we have preallocedFrom <= IDx < IDn < preallocedEnd,
	// and rewrite IDy (y < x) to preallocedEnd + y
	preallocedEnd  int64
	preallocedFrom int64

	insaneTableIDCount int
	unreusedCount      int

	sortedPhysicalIDs []int64
}

// New collects the requirement of prealloc IDs and return a
// not-yet-allocated PreallocIDs.
func New(tables []*metautil.Table) *PreallocIDs {
	if len(tables) == 0 {
		return &PreallocIDs{
			preallocedFrom: math.MaxInt64,
		}
	}

	maxv := int64(0)
	physicalIDs := make([]int64, 0, len(tables))
	insaneTableIDCount := 0
	for _, t := range tables {
		if t.Info.ID >= insaneTableIDThreshold {
			insaneTableIDCount += 1
		} else if t.Info.ID > maxv {
			maxv = t.Info.ID
		}
		physicalIDs = append(physicalIDs, t.Info.ID)

		if t.Info.Partition != nil && t.Info.Partition.Definitions != nil {
			for _, part := range t.Info.Partition.Definitions {
				if part.ID >= insaneTableIDThreshold {
					insaneTableIDCount += 1
				} else if part.ID > maxv {
					maxv = part.ID
				}
				physicalIDs = append(physicalIDs, part.ID)
			}
		}
	}
	slices.Sort(physicalIDs)
	return &PreallocIDs{
		preallocedEnd:      maxv + 1,
		preallocedFrom:     math.MaxInt64,
		insaneTableIDCount: insaneTableIDCount,
		sortedPhysicalIDs:  physicalIDs,
	}
}

// GetPreallocedInfo gets the (preallocedFrom, preallocedEnd, unreusedCount), and it is valid only after PreallocIDs.Allc has been called
func (p *PreallocIDs) GetPreallocedInfo() (int64, int64, int) {
	return p.preallocedFrom, p.preallocedEnd, p.unreusedCount
}

// SetPreallocedInfo sets by the already prealloced info from checkpoint metadata
func (p *PreallocIDs) SetPreallocedInfo(from, end int64, unreusedCount int) {
	p.preallocedFrom = from
	p.preallocedEnd = end
	p.unreusedCount = unreusedCount
}

// String implements fmt.Stringer.
func (p *PreallocIDs) String() string {
	if p.preallocedFrom >= p.preallocedEnd {
		return fmt.Sprintf("ID:empty(end=%d), unreusedCount: %d/%d, insaneTableIDCount: %d", p.preallocedEnd, p.unreusedCount, len(p.sortedPhysicalIDs), p.insaneTableIDCount)
	}
	return fmt.Sprintf("ID:[%d,%d), unreusedCount: %d/%d, insaneTableIDCount: %d", p.preallocedFrom, p.preallocedEnd, p.unreusedCount, len(p.sortedPhysicalIDs), p.insaneTableIDCount)
}

// preallocTableIDs peralloc the id for [start, end)
func (p *PreallocIDs) Alloc(m Allocator, sortedTableIDSpans []metautil.TableIDSpan) error {
	currentId, err := m.GetGlobalID()
	if err != nil {
		return err
	}

	// increases currentId to make sure there is no SST file that
	// part of the data needs to be rewrited but the other does not.
	// For example, there is an SST file having data:
	// [ k{table_id:100}:val, .. , k{table_id:105}:val ]
	//
	// However, this time BR pre-allocates global ID from 105 to 200.
	// So the ID of thetable 100 may be allocated 201, and then we split regions like this:
	// region 1 : [k{table_id:105}, k{table_id:106}),
	// ...
	// region N-1 : [k{table_id:200}, k{table_id:201}),
	// region N : [k{table_id:201}, k{table_id:207}),
	//
	// Finally, the SST file will be restored to region 1 and region N.
	// so here adjust the currentId from 105 to 106 so that table id 105 will
	// be allocated to 206.
	// Besides, it also make sure the IDs of tables whose data is from the same SST will be
	// allocated to a consecutive set of numbers.
	startIndex, eq := slices.BinarySearchFunc(sortedTableIDSpans, currentId, func(span metautil.TableIDSpan, id int64) int {
		return cmp.Compare(span.StartTableID, id)
	})
	fromId := currentId
	if eq {
		// the table ID[currentId == span.start] can not be reused, so also do not reuse table IDs in range [span.start, span.end].
		fromId = sortedTableIDSpans[startIndex].EndTableID
	} else if startIndex != 0 && currentId <= sortedTableIDSpans[startIndex-1].EndTableID {
		// the table IDs in range [span.start, currentId] can not be reused, so also do not reuse table IDs in range [span.start, span.end].
		fromId = sortedTableIDSpans[startIndex-1].EndTableID
	}

	unreusedCount, eq := slices.BinarySearch(p.sortedPhysicalIDs, fromId)
	if eq {
		unreusedCount += 1
	}

	// if the currentId is larger than p.preallocedEnd, allocate unreusedCount IDs.
	alloced, err := m.AdvanceGlobalIDs(int(max(p.preallocedEnd-currentId, 1) + int64(unreusedCount+p.insaneTableIDCount)))
	if err != nil {
		return err
	}
	if alloced != currentId {
		log.Panic("transaction failed!", zap.Int64("get global id", currentId), zap.Int64("write global id from", alloced))
	}

	p.preallocedFrom = max(alloced, fromId) + 1
	p.unreusedCount = unreusedCount
	return nil
}

// prealloced checks whether a table ID has been successfully allocated.
func (p *PreallocIDs) prealloced(tid int64) (int64, bool) {
	if p.preallocedFrom <= tid && tid < p.preallocedEnd {
		return tid, false
	}
	if tid >= insaneTableIDThreshold {
		index, eq := slices.BinarySearch(p.sortedPhysicalIDs[len(p.sortedPhysicalIDs)-p.insaneTableIDCount:], tid)
		if !eq {
			log.Panic("try to rewrite physical ID but not alloced before")
		}
		// preallocedFrom is larger than preallocedEnd when all the physical ID is unreused
		return max(p.preallocedEnd, p.preallocedFrom) + int64(index+p.unreusedCount), true
	}
	index, eq := slices.BinarySearch(p.sortedPhysicalIDs, tid)
	if !eq {
		log.Panic("try to rewrite physical ID but not alloced before")
	}
	// preallocedFrom is larger than preallocedEnd when all the physical ID is unreused
	return max(p.preallocedEnd, p.preallocedFrom) + int64(index), true
}

// TryRewriteTableID try to rewrite unreused id to the prealloced id
func (p *PreallocIDs) TryRewriteTableID(ti *model.TableInfo) *model.TableInfo {
	if tid, needRewrite := p.prealloced(ti.ID); needRewrite {
		ti.ID = tid
	}
	if ti.Partition != nil && ti.Partition.Definitions != nil {
		defs := ti.Partition.Definitions
		for i := range len(defs) {
			if newPid, needRewrite := p.prealloced(defs[i].ID); needRewrite {
				ti.Partition.Definitions[i].ID = newPid
			}
		}
	}
	return ti
}
