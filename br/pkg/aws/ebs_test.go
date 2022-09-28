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
// See the License for the specific language governing permissions and
// limitations under the License.

package aws

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEC2SessionExtractSnapProgress(t *testing.T) {
	strPtr := func(s string) *string {
		return &s
	}
	tests := []struct {
		str  *string
		want int64
	}{
		{nil, 0},
		{strPtr("12.12%"), 12},
		{strPtr("44.99%"), 44},
		{strPtr("  89.89%  "), 89},
		{strPtr("100%"), 100},
		{strPtr("111111%"), 100},
	}
	e := &EC2Session{}
	for _, tt := range tests {
		require.Equal(t, tt.want, e.extractSnapProgress(tt.str))
	}
}
