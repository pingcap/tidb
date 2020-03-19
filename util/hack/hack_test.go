// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package hack

import (
	"bytes"
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

func TestString(t *testing.T) {
	b := []byte("hello world")
	a := String(b)

	if a != "hello world" {
		t.Fatal(a)
	}

	b[0] = 'a'

	if a != "aello world" {
		t.Fatal(a)
	}

	b = append(b, "abc"...)
	if a != "aello world" {
		t.Fatalf("a:%v, b:%v", a, b)
	}
}

func TestByte(t *testing.T) {
	a := "hello world"

	b := Slice(a)

	if !bytes.Equal(b, []byte("hello world")) {
		t.Fatal(string(b))
	}
}

func TestMutable(t *testing.T) {
	a := []byte{'a', 'b', 'c'}
	b := String(a) // b is a mutable string.
	c := string(b) // Warn, c is a mutable string
	if c != "abc" {
		t.Fatalf("assert fail")
	}

	// c changed after a is modified
	a[0] = 's'
	if c != "sbc" {
		t.Fatal("test mutable string fail")
	}
}
