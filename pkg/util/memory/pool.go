package memory

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

var ResourcePoolID = uint64(0)

type ResourcePool struct {
	mu struct {
		sync.Mutex

		allocated int64

		maxAllocated int64

		budget Budget

		headChildren *ResourcePool

		numChildren int

		stopped bool
	}

	parentMu struct {
		prevChildren, nextChildren *ResourcePool
	}

	name string

	uid uint64

	reserved int64 // not only allocate quota from parent

	limit int64

	allocAlignSize int64

	cleanAllOnReleaseBytes bool

	actions PoolActions
}

// type ReclaimerStatus int

// const (
// 	ReclaimerUnavailable ReclaimerStatus = iota
// 	ReclaimerRunning
// 	ReclaimerReady
// )

/*
ResourceArbitrator:

	if Status() is Ready:
		if AsyncReclaim(_request_bytes, __cb):
			if status is Running:
				return false
			set status to Running
			async run in other coroutine
			set status to Stopped
			call __cb(TRUE if success else FALSE)
			set status to Ready if available
	else if Status() is Running:
		wait util status is NOT Running
*/
// type Reclaimer interface {
// 	// Status return Ready, Running, Unavailable
// 	Status() ReclaimerStatus
// 	// AsyncReclaim return result(BOOL), mayReclaimBytes(INT64).
// 	// 	 `result` is true only if status is NOT Running.
// 	//   `mayReclaimBytes`(GT than 0) is the approximate bytes to be recliamed.
// 	// 		resource-manager will use min(`mayReclaimBytes`, resource-pool.budget) for evaluation
// 	AsyncReclaim(int64, func(bool)) (bool, int64)
// }

type NoteActionState struct {
	pool      *ResourcePool
	allocated int64
}

type NoteAction struct {
	usedBytes int64
	action    func(NoteActionState)
}

type OutOfCapacityActionArgs struct {
	pool    *ResourcePool
	request int64
}

type PoolActions struct {
	noteAction          NoteAction
	outOfCapacityAction func(OutOfCapacityActionArgs) error
	outOfLimitAction    func(*ResourcePool) error
}

// ResourcePoolState
type ResourcePoolState struct {
	Level    int
	Name     string
	ID       uint64
	ParentID uint64
	Used     int64
	Reserved int64
	Budget   int64
}

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
		ParentID: p.mu.budget.pool.Uid(),
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

var maxAllocatedButUnusedBlocks = 10

var DefaultPoolAllocationSize int64 = 10 * 1024

func NewResourcePool(
	name string,
	allocAlignSize int64,
) *ResourcePool {
	return NewResourcePoolWithLimit(
		name, math.MaxInt64, allocAlignSize, PoolActions{})
}

func NewResourcePoolWithLimit(
	name string,
	limit int64,
	allocAlignSize int64,
	actions PoolActions,
) *ResourcePool {
	if allocAlignSize <= 0 {
		allocAlignSize = DefaultPoolAllocationSize
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	m := &ResourcePool{
		name:           name,
		uid:            atomic.AddUint64(&ResourcePoolID, 1),
		limit:          limit,
		allocAlignSize: allocAlignSize,
		actions:        actions,
	}
	return m
}

func (p *ResourcePool) SetAllocAlignSize(size int64) (ori int64) {
	p.mu.Lock()
	ori = p.allocAlignSize
	p.allocAlignSize = size
	p.mu.Unlock()
	return
}

func (p *ResourcePool) NewResourcePoolInheritWithLimit(
	name string, limit int64,
) *ResourcePool {
	return NewResourcePoolWithLimit(
		name,
		limit,
		p.allocAlignSize,
		p.actions,
	)
}

func (p *ResourcePool) StartNoReserved(pool *ResourcePool) {
	p.Start(pool, 0)
}

func (p *ResourcePool) Uid() uint64 {
	if p == nil {
		return 0
	}
	return p.uid
}

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

func (p *ResourcePool) Name() string {
	return p.name
}

func (p *ResourcePool) Limit() int64 {
	return p.limit
}

func (p *ResourcePool) IsStopped() (res bool) {
	p.mu.Lock()
	res = p.mu.stopped
	p.mu.Unlock()
	return
}

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

func (p *ResourcePool) MaxAllocated() (res int64) {
	p.mu.Lock()
	res = p.mu.maxAllocated
	p.mu.Unlock()
	return
}

func (p *ResourcePool) AllocatedBytes() (res int64) {
	p.mu.Lock()
	res = p.mu.allocated
	p.mu.Unlock()
	return
}

func (p *ResourcePool) ApproxAllocated() int64 {
	return p.allocated()
}

type Budget struct {
	cap  int64
	used int64
	pool *ResourcePool

	explicitReserved int64
}

func (b *Budget) Used() int64 {
	if b == nil {
		return 0
	}
	return b.used
}

func (b *Budget) Pool() *ResourcePool {
	if b == nil {
		return nil
	}
	return b.pool
}

func (b *Budget) Capacity() int64 {
	if b == nil {
		return 0
	}
	return b.cap
}

func (b *Budget) available() int64 {
	return b.cap - b.used
}

func (b *Budget) release() (reclaimed int64) {
	reclaimed = b.available()
	b.cap = b.used
	return
}

func (p *ResourcePool) CreateBudget() Budget {
	return Budget{pool: p}
}

func (p *ResourcePool) TransferBudget(
	origAccount *Budget,
) (newAccount Budget, err error) {
	b := p.CreateBudget()
	if err = b.Grow(origAccount.used); err != nil {
		return newAccount, err
	}
	origAccount.Clear()
	return b, nil
}

func (b *Budget) Reserve(x int64) error {
	if b == nil {
		return nil
	}
	minExtra := b.pool.roundSize(x)
	if err := b.pool.allocate(minExtra); err != nil {
		return err
	}
	b.cap += minExtra
	b.explicitReserved += x
	return nil
}

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

func (b *Budget) Resize(oldSz, newSz int64) error {
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

func (b *Budget) ResizeTo(newSz int64) error {
	if b == nil {
		return nil
	}
	if newSz == b.used {
		return nil
	}
	return b.Resize(b.used, newSz)
}

func (b *Budget) Grow(x int64) error {
	if b == nil {
		return nil
	}
	if extra := x - b.available(); extra > 0 {
		minExtra := b.pool.roundSize(extra)
		if err := b.pool.allocate(minExtra); err != nil {
			return err
		}
		b.cap += minExtra
	}
	b.used += x
	return nil
}

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
		if p.actions.outOfLimitAction != nil {
			if err := p.actions.outOfLimitAction(p); err != nil {
				return err
			}
		} else {
			return newBudgetExceededError("out of limit", p, request, p.mu.allocated, p.limit)
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

func (p *ResourcePool) ExplicitReserveBytes(x int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mu.budget.Reserve(x)
}

func (p *ResourcePool) allocate(x int64) error {
	{
		p.mu.Lock()

		if err := p.doAlloc(x); err != nil {
			p.mu.Unlock()
			return err
		}

		p.mu.Unlock()
	}

	if p.actions.noteAction.action != nil {
		if approxUsed := p.allocated(); approxUsed > p.actions.noteAction.usedBytes {
			p.actions.noteAction.action(NoteActionState{
				pool:      p,
				allocated: approxUsed,
			})
		}
	}

	return nil
}

func (p *ResourcePool) allocated() int64 {
	return atomic.LoadInt64(&p.mu.allocated)
}

func (p *ResourcePool) cap() int64 {
	return p.mu.budget.cap
}

func (p *ResourcePool) ApproxBudgetAvailable() int64 {
	return p.mu.budget.available()
}

func (p *ResourcePool) ApproxCap() int64 {
	return p.cap()
}

func (p *ResourcePool) Capacity() (res int64) {
	p.mu.Lock()
	res = p.cap()
	p.mu.Unlock()
	return
}

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

	p.doAdjustBudget(p.cleanAllOnReleaseBytes)
}

func (p *ResourcePool) SetOutOfCapacityAction(f func(OutOfCapacityActionArgs) error) {
	p.mu.Lock()
	p.actions.outOfCapacityAction = f
	p.mu.Unlock()
}

func (p *ResourcePool) SetOutOfLimitAction(f func(*ResourcePool) error) {
	p.mu.Lock()
	p.actions.outOfLimitAction = f
	p.mu.Unlock()
}

func (p *ResourcePool) increaseBudget(request int64) error {
	if p.mu.budget.pool == nil { // Root Pool
		need := request - p.mu.budget.available()
		if need <= 0 {
			p.mu.budget.used += request
			return nil
		}

		if p.actions.outOfCapacityAction != nil {
			if err := p.actions.outOfCapacityAction(OutOfCapacityActionArgs{
				pool:    p,
				request: need,
			}); err != nil {
				return err
			}

			p.mu.budget.used += request
			return nil
		} else {
			return newBudgetExceededError("out of quota",
				p,
				request,
				p.mu.budget.used,
				p.mu.budget.cap,
			)
		}
	}

	return p.mu.budget.Grow(request)
}

func (p *ResourcePool) roundSize(sz int64) int64 {
	return roundSize(sz, p.allocAlignSize)
}

func (p *ResourcePool) releaseBudget() {
	p.mu.budget.Clear()
}

func (p *ResourcePool) SetCleanAllOnReleaseBytes() {
	p.cleanAllOnReleaseBytes = true
}

func (p *ResourcePool) AdjustBudget(cleanAllOnReleaseBytes bool) {
	p.mu.Lock()
	p.doAdjustBudget(cleanAllOnReleaseBytes)
	p.mu.Unlock()
}

func (p *ResourcePool) doAdjustBudget(cleanAllOnReleaseBytes bool) {
	var need int64
	if !cleanAllOnReleaseBytes {
		need = p.allocAlignSize * int64(maxAllocatedButUnusedBlocks)
	}

	neededBytes := p.mu.allocated - p.reserved
	if neededBytes <= 0 {
		neededBytes = 0
	} else {
		neededBytes = p.roundSize(neededBytes)
	}
	if neededBytes <= p.mu.budget.used-need {
		delta := p.mu.budget.used - neededBytes
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
