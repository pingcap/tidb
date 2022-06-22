// Copyright 2020 PingCAP, Inc.
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

//go:build !race

package localpool

// Get gets an object from the pool.
func (p *LocalPool) Get() interface{} {
	pid := procPin()
	slot := p.slots[pid]
	objLen := len(slot.objs)
	var result interface{}
	if objLen > 0 {
		lastIdx := objLen - 1
		result = slot.objs[lastIdx]
		slot.objs[lastIdx] = nil
		slot.objs = slot.objs[:lastIdx]
		slot.getHit++
	} else {
		slot.getMiss++
	}
	procUnpin()
	if result == nil {
		result = p.newFn()
	}
	return result
}

// Put puts an object back to the pool.
// It returns true if the pool is not full and the obj is successfully put into the pool.
func (p *LocalPool) Put(obj interface{}) bool {
	if p.resetFn != nil {
		p.resetFn(obj)
	}
	var ok bool
	pid := procPin()
	slot := p.slots[pid]
	if len(slot.objs) < p.sizePerProc {
		slot.objs = append(slot.objs, obj)
		slot.putHit++
		ok = true
	} else {
		slot.putMiss++
	}
	procUnpin()
	return ok
}
