// Copyright 2019 PingCAP, Inc.
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

package sessionctx

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

func TestBasicCtxTypeToString(t *testing.T) {
	tests := []struct {
		key fmt.Stringer
		v   string
	}{
		{QueryString, "query_string"},
		{Initing, "initing"},
		{LastExecuteDDL, "last_execute_ddl"},
		{basicCtxType(9), "unknown"},
	}
	for _, tt := range tests {
		if tt.key.String() != tt.v {
			t.Fatalf("want %s but got %s", tt.v, tt.key.String())
		}
	}
}
