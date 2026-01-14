// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memory

import (
	"fmt"
	"sync"
	"sync/atomic"
)

var resourcePoolID = int64(-1)

// DefPoolAllocAlignSize indicates the default allocation alignment size
const DefPoolAllocAlignSize int64 = 10 * 1024

// DefMaxUnusedBlocks indicates the default maximum unused blocks*alloc-align-size of the resource pool
const DefMaxUnusedBlocks int64 = 10

// ResourcePool manages a set of resource quota
type ResourcePool struct {
	actions  PoolActions                                        // actions to be taken when the pool meets certain conditions
	parentMu struct{ prevChildren, nextChildren *ResourcePool } // accessible by parent pool only
	name     string                                             // name of pool
	mu       struct {
		headChildren *ResourcePool // head of the children pools chain
		budget       Budget        // budget of the resource quota
		allocated    int64         // allocated bytes of the resource quota
		maxAllocated int64         // maximum allocated bytes
		numChildren  int           // number of children pools
		sync.Mutex
		stopped bool
	}
	uid             uint64 // unique ID of the resource pool: uid <= 0 indicates that it is a internal pool
	reserved        int64  // quota from other sources
	limit           int64  // limit of the resource quota
	allocAlignSize  int64  // each allocation size must be a multiple of it
	maxUnusedBlocks int64  // max unused-blocks*alloc-align size before shrinking budget
}

// NoteActionState wraps the arguments of a note action
type NoteActionState struct {
	Pool      *ResourcePool
	Allocated int64
}

// NoteAction represents the action to be taken when the allocated size exceeds the threshold
type NoteAction struct {
	CB        func(NoteActionState)
	Threshold int64
}

// OutOfCapacityActionArgs wraps the arguments for out of capacity action
type OutOfCapacityActionArgs struct {
	Pool    *ResourcePool
	Request int64
}

// PoolActions represents the actions to be taken when the resource pool meets certain conditions
type PoolActions struct {
	OutOfCapacityActionCB func(OutOfCapacityActionArgs) error // Called when the resource pool is out of capacity
	OutOfLimitActionCB    func(*ResourcePool) error           // Called when the resource pool is out of limit
	NoteAction            NoteAction
}

// ResourcePoolState represents the state of a resource pool
type ResourcePoolState struct {
	Name     string
	Level    int
	ID       uint64
	ParentID uint64
	Used     int64
	Reserved int64
	Budget   int64
}

// Traverse the resource pool and calls the callback function
func (p *ResourcePool) Traverse(stateCb func(ResourcePoolState) error) error {
	return p.traverse(0, stateCb)
}

func (p *ResourcePool) traverse(level int, stateCb func(ResourcePoolState) error) error {
	p.mu.Lock()
	if p.mu.stopped {
		p.mu.Unlock()
		return nil
	}
	monitorState := ResourcePoolState{
		Level:    level,
		Name:     p.name,
		ID:       p.uid,
		ParentID: p.mu.budget.pool.UID(),
		Used:     p.mu.allocated,
		Reserved: p.reserved,
		Budget:   p.mu.budget.cap,
	}
	children := make([]*ResourcePool, 0, p.mu.numChildren)
	for c := p.mu.headChildren; c != nil; c = c.parentMu.nextChildren {
		children = append(children, c)
	}
	p.mu.Unlock()

	if err := stateCb(monitorState); err != nil {
		return err
	}
	for _, c := range children {
		if err := c.traverse(level+1, stateCb); err != nil {
			return err
		}
	}
	return nil
}

// NewResourcePoolDefault creates a new resource pool
func NewResourcePoolDefault(
	name string,
	allocAlignSize int64,
) *ResourcePool {
	return NewResourcePool(
		newPoolUID(),
		name,
		0,
		allocAlignSize,
		DefMaxUnusedBlocks,
		PoolActions{},
	)
}

func newPoolUID() uint64 {
	return uint64(atomic.AddInt64(&resourcePoolID, -1))
}

// NewResourcePool creates a new resource pool
func NewResourcePool(
	uid uint64,
	name string,
	limit int64,
	allocAlignSize int64,
	maxUnusedBlocks int64,
	actions PoolActions,
) *ResourcePool {
	if allocAlignSize <= 0 {
		allocAlignSize = DefPoolAllocAlignSize
	}
	if limit <= 0 {
		limit = DefMaxLimit
	}
	m := &ResourcePool{
		name:            name,
		uid:             uid,
		limit:           limit,
		allocAlignSize:  allocAlignSize,
		actions:         actions,
		maxUnusedBlocks: maxUnusedBlocks,
	}
	return m
}

// SetAllocAlignSize sets the allocation alignment size and returns the original value of allocAlignSize
func (p *ResourcePool) SetAllocAlignSize(size int64) (ori int64) {
	p.mu.Lock()
	ori = p.allocAlignSize
	p.allocAlignSize = size
	p.mu.Unlock()
	return
}

// NewResourcePoolInheritWithLimit creates a new resource pool inheriting from the parent pool
func (p *ResourcePool) NewResourcePoolInheritWithLimit(
	name string, limit int64,
) *ResourcePool {
	return NewResourcePool(
		newPoolUID(),
		name,
		limit,
		p.allocAlignSize,
		p.maxUnusedBlocks,
		p.actions,
	)
}

// StartNoReserved creates a new resource pool with no reserved quota
func (p *ResourcePool) StartNoReserved(pool *ResourcePool) {
	p.Start(pool, 0)
}

// UID returns the unique ID of the resource pool
func (p *ResourcePool) UID() uint64 {
	if p == nil {
		return 0
	}
	return p.uid
}

// Start starts the resource pool with a parent pool and reserved quota
func (p *ResourcePool) Start(parentPool *ResourcePool, reserved int64) {
	if p.mu.allocated != 0 {
		panic(fmt.Errorf("%s: started with %d bytes left over", p.name, p.mu.allocated))
	}
	if p.mu.budget.pool != nil {
		panic(fmt.Errorf("%s: already started with pool %s", p.name, p.mu.budget.pool.name))
	}
	p.mu.allocated = 0
	p.mu.maxAllocated = 0
	p.mu.budget = parentPool.CreateBudget()
	p.mu.stopped = false
	p.reserved = reserved

	if parentPool != nil {
		parentPool.mu.Lock()
		if s := parentPool.mu.headChildren; s != nil {
			s.parentMu.prevChildren = p
			p.parentMu.nextChildren = s
		}
		parentPool.mu.headChildren = p
		parentPool.mu.numChildren++
		parentPool.mu.Unlock()
	}
}

// Name returns the name of the resource pool
func (p *ResourcePool) Name() string {
	return p.name
}

// Limit returns the limit of the resource pool
func (p *ResourcePool) Limit() int64 {
	return p.limit
}

// IsStopped checks if the resource pool is stopped
func (p *ResourcePool) IsStopped() (res bool) {
	p.mu.Lock()
	res = p.mu.stopped
	p.mu.Unlock()
	return
}

// Stop stops the resource pool and releases the budget & returns the quota released
func (p *ResourcePool) Stop() (released int64) {
	p.mu.Lock()

	p.mu.stopped = true

	if p.mu.allocated != 0 {
		p.doRelease(p.mu.allocated)
	}

	released = p.mu.budget.cap
	p.releaseBudget()

	if parent := p.mu.budget.pool; parent != nil {
		func() {
			parent.mu.Lock()
			defer parent.mu.Unlock()
			prev, next := p.parentMu.prevChildren, p.parentMu.nextChildren
			if parent.mu.headChildren == p {
				parent.mu.headChildren = next
			}
			if prev != nil {
				prev.parentMu.nextChildren = next
			}
			if next != nil {
				next.parentMu.prevChildren = prev
			}
			parent.mu.numChildren--
		}()
	}

	p.mu.budget.pool = nil

	p.mu.Unlock()

	return released
}

// MaxAllocated returns the maximum allocated bytes
func (p *ResourcePool) MaxAllocated() (res int64) {
	p.mu.Lock()
	res = p.mu.maxAllocated
	p.mu.Unlock()
	return
}

// Allocated returns the allocated bytes
func (p *ResourcePool) Allocated() (res int64) {
	p.mu.Lock()
	res = p.allocated()
	p.mu.Unlock()
	return
}

// Budget represents the budget of a resource pool
type Budget struct {
	pool             *ResourcePool // source pool
	cap              int64         // capacity of the budget
	used             int64         // used bytes
	explicitReserved int64         // explicit reserved size which can not be shrunk
}

// Used returns the used bytes of the budget
func (b *Budget) Used() int64 {
	if b == nil {
		return 0
	}
	return b.used
}

// Pool returns the resource pool of the budget
func (b *Budget) Pool() *ResourcePool {
	if b == nil {
		return nil
	}
	return b.pool
}

// Capacity returns the capacity of the budget
func (b *Budget) Capacity() int64 {
	if b == nil {
		return 0
	}
	return b.cap
}

func (b *Budget) available() int64 {
	return b.cap - b.used
}

// CreateBudget creates a new budget from the resource pool
func (p *ResourcePool) CreateBudget() Budget {
	return Budget{pool: p}
}

// Reserve reserves the budget through the allocate aligned given size; update the explicit reserved size;
func (b *Budget) Reserve(request int64) error {
	if b == nil {
		return nil
	}
	minExtra := b.pool.roundSize(request)
	if err := b.pool.allocate(minExtra); err != nil {
		return err
	}
	b.cap += minExtra
	b.explicitReserved += request
	return nil
}

// Empty releases the used budget
func (b *Budget) Empty() {
	if b == nil {
		return
	}
	b.used = 0
	if release := b.available() - b.pool.allocAlignSize; release > 0 {
		b.pool.release(release)
		b.cap -= release
	}
}

// Clear releases the budget and resets
func (b *Budget) Clear() {
	if b == nil {
		return
	}
	release := b.cap
	b.used = 0
	b.cap = 0

	if b.pool == nil {
		return
	}
	if release > 0 {
		b.pool.release(release)
	}
}

func (b *Budget) resize(oldSz, newSz int64) error {
	if b == nil {
		return nil
	}
	delta := newSz - oldSz
	switch {
	case delta > 0:
		return b.Grow(delta)
	case delta < 0:
		b.Shrink(-delta)
	}
	return nil
}

// ResizeTo resizes the budget to the new size
func (b *Budget) ResizeTo(newSz int64) error {
	if b == nil {
		return nil
	}
	if newSz == b.used {
		return nil
	}
	return b.resize(b.used, newSz)
}

// Grow the budget by the given size
func (b *Budget) Grow(request int64) error {
	if b == nil {
		return nil
	}
	if extra := request - b.available(); extra > 0 {
		minExtra := b.pool.roundSize(extra)
		if err := b.pool.allocate(minExtra); err != nil {
			return err
		}
		b.cap += minExtra
	}
	b.used += request
	return nil
}

// Shrink the budget and reduce the given size
func (b *Budget) Shrink(delta int64) {
	if b == nil || delta == 0 {
		return
	}
	if b.used < delta {
		delta = b.used
	}
	b.used -= delta

	if b.pool == nil {
		return
	}

	if release := b.available() - b.pool.allocAlignSize; release > 0 && (b.explicitReserved == 0 || b.used+b.pool.allocAlignSize > b.explicitReserved) {
		b.pool.release(release)
		b.cap -= release
	}
}

func (p *ResourcePool) doAlloc(request int64) error {
	if p.mu.allocated > p.limit-request {
		if p.actions.OutOfLimitActionCB == nil {
			return newBudgetExceededError("out of limit", p, request, p.mu.allocated, p.limit)
		}
		if err := p.actions.OutOfLimitActionCB(p); err != nil {
			return err
		}
	}

	// Check whether we need to request an increase of our budget.
	if delta := request + p.mu.allocated - p.mu.budget.used - p.reserved; delta > 0 {
		if err := p.increaseBudget(delta); err != nil {
			return err
		}
	}
	p.mu.allocated += request
	if p.mu.maxAllocated < p.mu.allocated {
		p.mu.maxAllocated = p.mu.allocated
	}

	return nil
}

// ExplicitReserve reserves the budget explicitly
func (p *ResourcePool) ExplicitReserve(request int64) (err error) {
	p.mu.Lock()
	err = p.mu.budget.Reserve(request)
	p.mu.Unlock()
	return
}

func (p *ResourcePool) allocate(request int64) error {
	{
		p.mu.Lock()

		if err := p.doAlloc(request); err != nil {
			p.mu.Unlock()
			return err
		}

		p.mu.Unlock()
	}

	if p.actions.NoteAction.CB != nil {
		if allocated := p.allocated(); allocated > p.actions.NoteAction.Threshold {
			p.actions.NoteAction.CB(NoteActionState{
				Pool:      p,
				Allocated: allocated,
			})
		}
	}

	return nil
}

func (p *ResourcePool) allocated() int64 {
	return p.mu.allocated
}

func (p *ResourcePool) capacity() int64 {
	return p.mu.budget.cap
}

// ApproxCap returns the approximate capacity of the resource pool
func (p *ResourcePool) ApproxCap() int64 {
	return p.capacity()
}

// Capacity returns the capacity of the resource pool
func (p *ResourcePool) Capacity() (res int64) {
	p.mu.Lock()
	res = p.capacity()
	p.mu.Unlock()
	return
}

// SetLimit sets the limit of the resource pool
func (p *ResourcePool) SetLimit(newLimit int64) {
	p.mu.Lock()
	p.limit = newLimit
	p.mu.Unlock()
}

func (p *ResourcePool) release(sz int64) {
	p.mu.Lock()
	p.doRelease(sz)
	p.mu.Unlock()
}

func (p *ResourcePool) doRelease(sz int64) {
	if p.mu.allocated < sz {
		sz = p.mu.allocated
	}
	p.mu.allocated -= sz

	p.doAdjustBudget()
}

// SetOutOfCapacityAction sets the out of capacity action
// It is called when the resource pool is out of capacity
func (p *ResourcePool) SetOutOfCapacityAction(f func(OutOfCapacityActionArgs) error) {
	p.mu.Lock()
	p.doSetOutOfCapacityAction(f)
	p.mu.Unlock()
}

func (p *ResourcePool) doSetOutOfCapacityAction(f func(OutOfCapacityActionArgs) error) {
	p.actions.OutOfCapacityActionCB = f
}

// SetOutOfLimitAction sets the out of limit action
// It is called when the resource pool is out of limit
func (p *ResourcePool) SetOutOfLimitAction(f func(*ResourcePool) error) {
	p.mu.Lock()
	p.actions.OutOfLimitActionCB = f
	p.mu.Unlock()
}

func (p *ResourcePool) increaseBudget(request int64) error {
	if p.mu.budget.pool == nil { // Root Pool
		need := request - p.mu.budget.available()
		if need <= 0 {
			p.mu.budget.used += request
			return nil
		}

		if p.actions.OutOfCapacityActionCB != nil {
			if err := p.actions.OutOfCapacityActionCB(OutOfCapacityActionArgs{
				Pool:    p,
				Request: need,
			}); err != nil {
				return err
			}

			p.mu.budget.used += request
			return nil
		}
		return newBudgetExceededError("out of quota",
			p,
			request,
			p.mu.budget.used,
			p.mu.budget.cap,
		)
	}

	return p.mu.budget.Grow(request)
}

func (p *ResourcePool) roundSize(sz int64) int64 {
	alignSize := p.allocAlignSize
	if alignSize <= 1 {
		return sz
	}
	return (sz + alignSize - 1) / alignSize * alignSize
}

func (p *ResourcePool) releaseBudget() {
	p.mu.budget.Clear()
}

// AdjustBudget adjusts the budget of the resource pool
func (p *ResourcePool) AdjustBudget() {
	p.mu.Lock()
	p.doAdjustBudget()
	p.mu.Unlock()
}

func (p *ResourcePool) doAdjustBudget() {
	needed := p.mu.allocated - p.reserved
	if needed <= 0 {
		needed = 0
	} else {
		needed = p.roundSize(needed)
	}
	if p.allocAlignSize*p.maxUnusedBlocks <= p.mu.budget.used-needed {
		delta := p.mu.budget.used - needed
		p.mu.budget.Shrink(delta)
	}
}

func (p *ResourcePool) forceAddCap(c int64) {
	if c == 0 {
		return
	}
	p.mu.budget.cap += c
}

func newBudgetExceededError(
	reason string,
	root *ResourcePool,
	requestedBytes int64, allocatedBytes int64, limitBytes int64,
) error {
	return fmt.Errorf(
		"resource pool `%s` meets `%s`: requested(%d) + allocated(%d) > limit(%d)",
		root.name,
		reason,
		requestedBytes,
		allocatedBytes,
		limitBytes,
	)
}
