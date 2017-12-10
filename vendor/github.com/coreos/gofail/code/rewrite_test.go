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

package code

import (
	"bytes"
	"strings"
	"testing"
)

var examples = []struct {
	code string
	wfps int
}{
	{"func f() {\n\t// gofail: var Test int\n\t// fmt.Println(Test)\n}", 1},
	{"func f() {\n\t\t// gofail: var Test int\n\t\t// \tfmt.Println(Test)\n}", 1},
	{"func f() {\n// gofail: var Test int\n// \tfmt.Println(Test)\n}", 1},
	{"func f() {\n\t// gofail: var Test int\n\t// fmt.Println(Test)\n}\n", 1},
	{"func f() {\n\t// gofail: var Test int\n\t// fmt.Println(Test)// return\n}\n", 1},
	{"func f() {\n\t// gofail: var OneLineTest int\n}\n", 1},
	{"func f() {\n\t// gofail: var Test int\n\t// fmt.Println(Test)\n\n\t// gofail: var Test2 int\n\t// fmt.Println(Test2)\n}\n", 2},
	{"func f() {\n\t// gofail: var NoTypeTest struct{}\n\t// fmt.Println(`hi`)\n}\n", 1},
	{"func f() {\n\t// gofail: var NoTypeTest struct{}\n}\n", 1},
	{"func f() {\n\t// gofail: var NoTypeTest struct{}\n\t// fmt.Println(`hi`)\n\t// fmt.Println(`bye`)\n}\n", 1},
}

func TestToFailpoint(t *testing.T) {
	for i, ex := range examples {
		dst := bytes.NewBuffer(make([]byte, 0, 1024))
		src := strings.NewReader(ex.code)
		fps, err := ToFailpoints(dst, src)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		if len(fps) != ex.wfps {
			t.Fatalf("%d: got %d failpoints but expected %d", i, len(fps), ex.wfps)
		}
		dstOut := dst.String()
		if len(strings.Split(dstOut, "\n")) != len(strings.Split(ex.code, "\n")) {
			t.Fatalf("%d: bad line count %q", i, dstOut)
		}
	}
}

func TestToComment(t *testing.T) {
	for i, ex := range examples {
		dst := bytes.NewBuffer(make([]byte, 0, 1024))
		src := strings.NewReader(ex.code)
		fps, err := ToFailpoints(dst, src)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}

		src = strings.NewReader(dst.String())
		dst.Reset()
		fps, err = ToComments(dst, src)
		plainCode := dst.String()

		if plainCode != ex.code {
			t.Fatalf("%d: non-preserving ToComments(); got %q, want %q", i, plainCode, ex.code)
		}
		if len(fps) != ex.wfps {
			t.Fatalf("%d: got %d failpoints but expected %d", i, len(fps), ex.wfps)
		}
	}

}
