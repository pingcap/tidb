// Copyright 2016 Google Inc.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package uuid

import (
	"encoding/json"
	"reflect"
	"testing"
)

var testUUID = Must(Parse("f47ac10b-58cc-0372-8567-0e02b2c3d479"))

func TestJSON(t *testing.T) {
	type S struct {
		ID1 UUID
		ID2 UUID
	}
	s1 := S{ID1: testUUID}
	data, err := json.Marshal(&s1)
	if err != nil {
		t.Fatal(err)
	}
	var s2 S
	if err := json.Unmarshal(data, &s2); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&s1, &s2) {
		t.Errorf("got %#v, want %#v", s2, s1)
	}
}

func BenchmarkUUID_MarshalJSON(b *testing.B) {
	x := &struct {
		UUID UUID `json:"uuid"`
	}{}
	var err error
	x.UUID, err = Parse("f47ac10b-58cc-0372-8567-0e02b2c3d479")
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		js, err := json.Marshal(x)
		if err != nil {
			b.Fatalf("marshal json: %#v (%v)", js, err)
		}
	}
}

func BenchmarkUUID_UnmarshalJSON(b *testing.B) {
	js := []byte(`{"uuid":"f47ac10b-58cc-0372-8567-0e02b2c3d479"}`)
	var x *struct {
		UUID UUID `json:"uuid"`
	}
	for i := 0; i < b.N; i++ {
		err := json.Unmarshal(js, &x)
		if err != nil {
			b.Fatalf("marshal json: %#v (%v)", js, err)
		}
	}
}
