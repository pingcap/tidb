// Copyright 2026 PingCAP, Inc.
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

package main

import (
	"bytes"
	"encoding/csv"
	"io"
	"strconv"
	"testing"
)

func TestCSV(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"-rows", "8", "-bytes", "128"}, &out); err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(&out).ReadAll()
	if err != nil || len(rows) != 8 {
		t.Fatalf("invalid CSV: rows=%d err=%v", len(rows), err)
	}
	for i, row := range rows {
		if len(row) != 2 || row[0] != strconv.Itoa(i+1) || len(row[1]) != 128 || row[1] != rows[i%4][1] {
			t.Fatalf("invalid row %d", i)
		}
	}
	for _, args := range [][]string{{"-rows", "0"}, {"-bytes", "1"}, {"unexpected"}} {
		if run(args, io.Discard) == nil {
			t.Fatalf("accepted invalid args %v", args)
		}
	}
}
