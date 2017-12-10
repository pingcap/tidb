// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package collate_test

import (
	"fmt"
	"testing"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

func ExampleCollator_Strings() {
	c := collate.New(language.Und)
	strings := []string{
		"ad",
		"ab",
		"äb",
		"ac",
	}
	c.SortStrings(strings)
	fmt.Println(strings)
	// Output: [ab äb ac ad]
}

type sorter []string

func (s sorter) Len() int {
	return len(s)
}

func (s sorter) Swap(i, j int) {
	s[j], s[i] = s[i], s[j]
}

func (s sorter) Bytes(i int) []byte {
	return []byte(s[i])
}

func TestSort(t *testing.T) {
	c := collate.New(language.English)
	strings := []string{
		"bcd",
		"abc",
		"ddd",
	}
	c.Sort(sorter(strings))
	res := fmt.Sprint(strings)
	want := "[abc bcd ddd]"
	if res != want {
		t.Errorf("found %s; want %s", res, want)
	}
}
