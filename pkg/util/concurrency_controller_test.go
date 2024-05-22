// Copyright 2024 PingCAP, Inc.
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

package util

import (
	"testing"
	"time"
)

func TestBlockConcurrencyController(t *testing.T) {
	cc := NewConcurrencyController(10)
	exit := make(chan struct{})
	for i := 0; i < 10; i++ {
		cc.Run(func() {
			<-exit
		})
	}
	select {
	case <-time.After(10 * time.Millisecond):
		close(exit)
	case <-cc.ch:
		t.Fatal("TestBlockConcurrencyController failed")
	}
}

func TestPanicConcurrencyController(t *testing.T) {
	cc := NewConcurrencyController(1)
	cc.Run(func() {
		panic("test")
	})
	select {
	case <-time.After(10 * time.Millisecond):
		t.Fatal("TestBlockConcurrencyController failed")
	case <-cc.ch:
		return
	}
}
