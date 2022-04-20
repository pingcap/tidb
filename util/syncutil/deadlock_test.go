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

package syncutil

import (
	"fmt"
	"sync"
	"testing"
)

func TestDeadLock(t *testing.T) {
	var a Mutex
	var b Mutex
	var i int

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		a.Lock()
		b.Lock()
		for j := 0; j < 1000; j++ {
			i += j
		}
		b.Unlock()
		a.Unlock()
		wg.Done()
	}()
	go func() {
		b.Lock()
		a.Lock()
		for j := 0; j < 1000; j++ {
			i += j
		}
		a.Unlock()
		b.Unlock()
		wg.Done()
	}()
	wg.Wait()
	fmt.Println(i)
}
