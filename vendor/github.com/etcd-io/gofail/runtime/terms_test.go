// Copyright 2016 CoreOS, Inc.
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

package runtime

import (
	"reflect"
	"testing"
)

func TestTermsString(t *testing.T) {
	tests := []struct {
		desc  string
		weval []string
	}{
		{`off`, []string{""}},
		{`2*return("abc")`, []string{"abc", "abc", ""}},
		{`2*return("abc")->1*return("def")`, []string{"abc", "abc", "def", ""}},
		{`1*return("abc")->return("def")`, []string{"abc", "def", "def"}},
	}
	for _, tt := range tests {
		ter, err := newTerms("test", tt.desc)
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range tt.weval {
			v, eerr := ter.eval()
			if v == nil && w == "" {
				continue
			}
			if eerr != nil {
				t.Fatal(err)
			}
			if v.(string) != w {
				t.Fatalf("got %q, expected %q", v, w)
			}
		}
	}
}

func TestTermsTypes(t *testing.T) {
	tests := []struct {
		desc  string
		weval interface{}
	}{
		{`off`, nil},
		{`return("abc")`, "abc"},
		{`return(true)`, true},
		{`return(1)`, 1},
		{`return()`, struct{}{}},
	}
	for _, tt := range tests {
		ter, err := newTerms("test", tt.desc)
		if err != nil {
			t.Fatal(err)
		}
		v, _ := ter.eval()
		if v == nil && tt.weval == nil {
			continue
		}
		if !reflect.DeepEqual(v, tt.weval) {
			t.Fatalf("got %v, expected %v", v, tt.weval)
		}
	}
}
