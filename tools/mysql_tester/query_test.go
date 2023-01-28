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

package main

import (
	"fmt"
	"testing"
)

func assertEqual(t *testing.T, a interface{}, b interface{}, message string) {
	if a == b {
		return
	}
	if len(message) == 0 {
		message = fmt.Sprintf("%v != %v", a, b)
	}
	t.Fatal(message)
}

func TestParseQueryies(t *testing.T) {
	q := query{Query: "select * from t;"}
	if qe, err := ParseQueries(q); err == nil {
		assertEqual(t, qe[0].tp, Q_QUERY, fmt.Sprintf("Expected: %d, got: %d", Q_QUERY, qe[0].tp))
		assertEqual(t, qe[0].Query, q, fmt.Sprintf("Expected: %s, got: %s", q.Query, qe[0].Query))
	} else {
		t.Fatalf("error is not nil. %v", err)
	}

	q = query{Query: "--sorted_result select * from t;"}
	if q, err := ParseQueries(q); err == nil {
		assertEqual(t, q[0].tp, Q_SORTED_RESULT, "sorted_result")
		assertEqual(t, q[0].Query, "select * from t;", fmt.Sprintf("Expected: '%s', got '%s'", "select * from t;", q[0].Query))
	} else {
		t.Fatalf("error is not nil. %s", err)
	}

	// invalid comment command style
	q = query{Query: "--abc select * from t;"}
	_, err := ParseQueries(q)
	assertEqual(t, err, ErrInvalidCommand, fmt.Sprintf("Expected: %v, got %v", ErrInvalidCommand, err))
}
