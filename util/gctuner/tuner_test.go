// Copyright 2022 PingCAP, Inc.
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

package gctuner

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testHeap []byte

func TestTuner(t *testing.T) {
	is := assert.New(t)
	memLimit := uint64(100 * 1024 * 1024) //100 MB
	threshold := memLimit / 2
	tn := newTuner(threshold)
	is.Equal(tn.threshold, threshold)
	is.Equal(defaultGCPercent, tn.getGCPercent())

	// no heap
	for i := 0; i < 100; i++ {
		runtime.GC()
		is.Equal(MaxGCPercent, tn.getGCPercent())
	}

	// 1/4 threshold
	testHeap = make([]byte, threshold/4)
	for i := 0; i < 100; i++ {
		runtime.GC()
		// ~= 300
		is.GreaterOrEqual(tn.getGCPercent(), uint32(250))
		is.LessOrEqual(tn.getGCPercent(), uint32(300))
	}

	// 1/2 threshold
	testHeap = make([]byte, threshold/2)
	runtime.GC()
	for i := 0; i < 100; i++ {
		runtime.GC()
		// ~= 100
		is.GreaterOrEqual(tn.getGCPercent(), uint32(50))
		is.LessOrEqual(tn.getGCPercent(), uint32(100))
	}

	// 3/4 threshold
	testHeap = make([]byte, threshold/4*3)
	runtime.GC()
	for i := 0; i < 100; i++ {
		runtime.GC()
		is.Equal(MinGCPercent, tn.getGCPercent())
	}

	// out of threshold
	testHeap = make([]byte, threshold+1024)
	runtime.GC()
	for i := 0; i < 100; i++ {
		runtime.GC()
		is.Equal(MinGCPercent, tn.getGCPercent())
	}
}

func TestCalcGCPercent(t *testing.T) {
	is := assert.New(t)
	const gb = 1024 * 1024 * 1024
	// use default value when invalid params
	is.Equal(defaultGCPercent, calcGCPercent(0, 0))
	is.Equal(defaultGCPercent, calcGCPercent(0, 1))
	is.Equal(defaultGCPercent, calcGCPercent(1, 0))

	is.Equal(MaxGCPercent, calcGCPercent(1, 3*gb))
	is.Equal(MaxGCPercent, calcGCPercent(gb/10, 4*gb))
	is.Equal(MaxGCPercent, calcGCPercent(gb/2, 4*gb))
	is.Equal(uint32(300), calcGCPercent(1*gb, 4*gb))
	is.Equal(uint32(166), calcGCPercent(1.5*gb, 4*gb))
	is.Equal(uint32(100), calcGCPercent(2*gb, 4*gb))
	is.Equal(MinGCPercent, calcGCPercent(3*gb, 4*gb))
	is.Equal(MinGCPercent, calcGCPercent(4*gb, 4*gb))
	is.Equal(MinGCPercent, calcGCPercent(5*gb, 4*gb))
}
