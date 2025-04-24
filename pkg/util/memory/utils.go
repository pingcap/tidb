package memory

import (
	"container/list"
	"math/bits"
	"sync/atomic"
	"time"
)

type WrapList[V any] struct {
	base list.List
	end  WrapListElement
	num  int64
}

type WrapListElement struct {
	base *list.Element
}

func (w *WrapListElement) valid() bool {
	return w.base != nil
}

func (w *WrapListElement) reset() {
	w.base = nil
}

func (l *WrapList[V]) init() {
	l.base.Init()
	l.base.PushBack(nil)
	l.end = WrapListElement{l.base.Back()}
}

func (l *WrapList[V]) moveToFront(e WrapListElement) {
	l.base.MoveToFront(e.base)
}

func (l *WrapList[V]) remove(e WrapListElement) {
	e.base.Value = nil
	l.base.MoveToBack(e.base)
	l.num--
}

func (l *WrapList[V]) front() (res V) {
	if l.num == 0 {
		return
	}
	return l.base.Front().Value.(V)
}

func (l *WrapList[V]) popFront() (res V) {
	if l.num == 0 {
		return
	}
	e := l.base.Front()
	res = e.Value.(V)
	l.remove(WrapListElement{e})
	return
}

func (l *WrapList[V]) size() int64 {
	return l.num
}

func (l *WrapList[V]) empty() bool {
	return l.num == 0
}

func (l *WrapList[V]) pushBack(v V) WrapListElement {
	var x *list.Element
	if l.size()+1 == int64(l.base.Len()) {
		x = l.base.InsertBefore(v, l.end.base)
	} else {
		x = l.base.Back()
		l.base.MoveBefore(x, l.end.base)
		x.Value = v
	}
	l.num++
	return WrapListElement{x}
}

// Notifer is used for multiple producer & single consumer
type Notifer struct {
	C     chan struct{}
	awake int32
}

func NewNotifer() Notifer {
	return Notifer{
		C: make(chan struct{}, 1),
	}
}

// return previous awake status
func (n *Notifer) setNotAwake() bool {
	return atomic.SwapInt32(&n.awake, 0) != 0
}

// Wait only can be invoked by single consumer
func (n *Notifer) Wait() {
	<-n.C
	n.setNotAwake()
}

// return previous awake status
func (n *Notifer) setAwake() bool {
	return atomic.SwapInt32(&n.awake, 1) != 0
}

// Wake notify producer if necessary
func (n *Notifer) Wake() {
	n.wake()
}

// Wake notify producer if necessary
func (n *Notifer) wake() {
	// 1 -> 1: do nothing
	// 0 -> 1: send signal
	if !n.setAwake() {
		n.C <- struct{}{}
	}
}

func (n *Notifer) isAwake() bool {
	return atomic.LoadInt32(&n.awake) != 0
}

func (n *Notifer) WeakWake() {
	if n.isAwake() {
		return
	}
	n.wake()
}

func HashStr(key string) uint64 {
	hashKey := initHashKey
	for _, c := range key {
		hashKey *= prime64
		hashKey ^= uint64(c)
	}
	return hashKey
}

func shardIndexByUID(key uint64, shardsMask uint64) uint64 {
	const step = 8
	const stepMask uint64 = uint64(1)<<step - 1

	hashKey := initHashKey
	{
		// handle significant last 8 bits
		hashKey ^= (key & stepMask)
		hashKey *= prime64
		key >>= step
	}
	{
		hashKey ^= key
		hashKey *= prime64
	}
	return hashKey & shardsMask
}

func getQuotaShard(quota int64, maxQuotaShard int) int {
	p := uint64(quota) / uint64(BaseQuotaUnit)
	pos := bits.Len64(p)
	pos = min(pos, maxQuotaShard-1)
	return pos
}

func nowUnixMilli() int64 {
	return time.Now().UnixMilli()
}

func now() time.Time {
	return time.Now()
}

func nextPow2(n uint64) uint64 {
	if n == 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}

func roundSize(sz int64, poolAllocationSize int64) int64 {
	chunks := (sz + poolAllocationSize - 1) / poolAllocationSize
	return chunks * poolAllocationSize
}

func calcRatio(x, y int64) (zMilli int64) {
	zMilli = x * Kilo / y
	return
}

func multiRatio(x, yMilli int64) int64 {
	return x * yMilli / Kilo
}

func intoRatio(x float64) (zMilli int64) {
	zMilli = int64(x * Kilo)
	return
}
