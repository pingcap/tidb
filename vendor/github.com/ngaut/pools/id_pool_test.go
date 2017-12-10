// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pools

import (
	"reflect"
	"strings"
	"testing"
)

func (pool *IDPool) want(want *IDPool, t *testing.T) {
	if pool.maxUsed != want.maxUsed {
		t.Errorf("pool.maxUsed = %#v, want %#v", pool.maxUsed, want.maxUsed)
	}

	if !reflect.DeepEqual(pool.used, want.used) {
		t.Errorf("pool.used = %#v, want %#v", pool.used, want.used)
	}
}

func TestIDPoolFirstGet(t *testing.T) {
	pool := NewIDPool()

	if got := pool.Get(); got != 1 {
		t.Errorf("pool.Get() = %v, want 1", got)
	}

	pool.want(&IDPool{used: map[uint32]bool{}, maxUsed: 1}, t)
}

func TestIDPoolSecondGet(t *testing.T) {
	pool := NewIDPool()
	pool.Get()

	if got := pool.Get(); got != 2 {
		t.Errorf("pool.Get() = %v, want 2", got)
	}

	pool.want(&IDPool{used: map[uint32]bool{}, maxUsed: 2}, t)
}

func TestIDPoolPutToUsedSet(t *testing.T) {
	pool := NewIDPool()
	id1 := pool.Get()
	pool.Get()
	pool.Put(id1)

	pool.want(&IDPool{used: map[uint32]bool{1: true}, maxUsed: 2}, t)
}

func TestIDPoolPutMaxUsed1(t *testing.T) {
	pool := NewIDPool()
	id1 := pool.Get()
	pool.Put(id1)

	pool.want(&IDPool{used: map[uint32]bool{}, maxUsed: 0}, t)
}

func TestIDPoolPutMaxUsed2(t *testing.T) {
	pool := NewIDPool()
	pool.Get()
	id2 := pool.Get()
	pool.Put(id2)

	pool.want(&IDPool{used: map[uint32]bool{}, maxUsed: 1}, t)
}

func TestIDPoolGetFromUsedSet(t *testing.T) {
	pool := NewIDPool()
	id1 := pool.Get()
	pool.Get()
	pool.Put(id1)

	if got := pool.Get(); got != 1 {
		t.Errorf("pool.Get() = %v, want 1", got)
	}

	pool.want(&IDPool{used: map[uint32]bool{}, maxUsed: 2}, t)
}

func wantError(want string, t *testing.T) {
	rec := recover()
	if rec == nil {
		t.Errorf("expected panic, but there wasn't one")
	}
	err, ok := rec.(error)
	if !ok || !strings.Contains(err.Error(), want) {
		t.Errorf("wrong error, got '%v', want '%v'", err, want)
	}
}

func TestIDPoolPut0(t *testing.T) {
	pool := NewIDPool()
	pool.Get()

	defer wantError("invalid value", t)
	pool.Put(0)
}

func TestIDPoolPutInvalid(t *testing.T) {
	pool := NewIDPool()
	pool.Get()

	defer wantError("invalid value", t)
	pool.Put(5)
}

func TestIDPoolPutDuplicate(t *testing.T) {
	pool := NewIDPool()
	pool.Get()
	pool.Get()
	pool.Put(1)

	defer wantError("already recycled", t)
	pool.Put(1)
}
