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
	"container/list"
	"math/bits"
	"sync/atomic"
	"time"

	"golang.org/x/sys/cpu"
)

const (
	byteSize   = 1
	byteSizeKB = 1 << 10
	byteSizeMB = 1 << 20
	byteSizeGB = 1 << 30
	kilo       = 1000
)

// list with cache to avoid the cost of allocating and deallocating list elements.
type wrapList[V any] struct {
	end  wrapListElement
	base list.List
	num  int64
}

type wrapListElement struct {
	base *list.Element
}

func (w *wrapListElement) valid() bool {
	return w.base != nil
}

func (w *wrapListElement) reset() {
	w.base = nil
}

func (l *wrapList[V]) init() {
	l.base.Init()
	l.base.PushBack(nil)
	l.end = wrapListElement{l.base.Back()}
}

func (l *wrapList[V]) moveToFront(e wrapListElement) {
	l.base.MoveToFront(e.base)
}

func (l *wrapList[V]) doAddNum(x int64) {
	l.num += x
}

func (l *wrapList[V]) remove(e wrapListElement) {
	e.base.Value = nil
	l.base.MoveToBack(e.base)
	l.doAddNum(-1)
}

func (l *wrapList[V]) front() (res V) {
	if l.empty() {
		return
	}
	return l.base.Front().Value.(V)
}

func (l *wrapList[V]) popFront() (res V) {
	if l.empty() {
		return
	}
	e := l.base.Front()
	res = e.Value.(V)
	l.remove(wrapListElement{e})
	return
}

func (l *wrapList[V]) size() int64 {
	return l.num
}

func (l *wrapList[V]) empty() bool {
	return l.size() == 0
}

//go:norace
func (l *wrapList[V]) approxSize() int64 {
	return l.size()
}

//go:norace
func (l *wrapList[V]) approxEmpty() bool {
	return l.empty()
}

func (l *wrapList[V]) pushBack(v V) wrapListElement {
	var x *list.Element
	if l.size()+1 == int64(l.base.Len()) {
		x = l.base.InsertBefore(v, l.end.base)
	} else {
		x = l.base.Back()
		l.base.MoveBefore(x, l.end.base)
		x.Value = v
	}
	l.doAddNum(1)
	return wrapListElement{x}
}

// Notifer works as the multiple producer & single consumer mode.
type Notifer struct {
	C     chan struct{}
	awake int32
}

// NewNotifer creates a new Notifer instance.
func NewNotifer() Notifer {
	return Notifer{
		C: make(chan struct{}, 1),
	}
}

// return previous awake status
func (n *Notifer) clear() bool {
	return atomic.SwapInt32(&n.awake, 0) != 0
}

// Wait for signal synchronously (consumer)
func (n *Notifer) Wait() {
	<-n.C
	n.clear()
}

// Wake the consumer
func (n *Notifer) Wake() {
	n.wake()
}

func (n *Notifer) wake() {
	// 1 -> 1: do nothing
	// 0 -> 1: send signal
	if atomic.SwapInt32(&n.awake, 1) == 0 {
		n.C <- struct{}{}
	}
}

func (n *Notifer) isAwake() bool {
	return atomic.LoadInt32(&n.awake) != 0
}

// WeakWake wakes the consumer if it is not awake (may loose signal under concurrent scenarios).
func (n *Notifer) WeakWake() {
	if n.isAwake() {
		return
	}
	n.wake()
}

// HashStr hashes a string to a uint64 value
func HashStr(key string) uint64 {
	hashKey := initHashKey
	for _, c := range key {
		hashKey *= prime64
		hashKey ^= uint64(c)
	}
	return hashKey
}

// HashEvenNum hashes a uint64 even number to a uint64 value
func HashEvenNum(key uint64) uint64 {
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

	return hashKey
}

func shardIndexByUID(key uint64, shardsMask uint64) uint64 {
	return HashEvenNum(key) & shardsMask
}

func getQuotaShard(quota int64, maxQuotaShard int) int {
	p := uint64(quota) / uint64(baseQuotaUnit)
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

func calcRatio(x, y int64) (zMilli int64) {
	zMilli = x * kilo / y
	return
}

func multiRatio(x, yMilli int64) int64 {
	return x * yMilli / kilo
}

func intoRatio(x float64) (zMilli int64) {
	zMilli = int64(x * kilo)
	return
}

type cpuCacheLinePad cpu.CacheLinePad
