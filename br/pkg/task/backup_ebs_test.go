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

package task

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
)

func TestIsRegionsHasHole(t *testing.T) {
	type args struct {
		allRegions []*metapb.Region
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "one region",
			args: args{
				allRegions: []*metapb.Region{
					{},
				},
			},
			want: false,
		},
		{
			name: "2 region",
			args: args{
				allRegions: []*metapb.Region{
					{EndKey: []byte("a")},
					{StartKey: []byte("a")},
				},
			},
			want: false,
		},
		{
			name: "many regions",
			args: args{
				allRegions: []*metapb.Region{
					{EndKey: []byte("a")},
					{StartKey: []byte("a"), EndKey: []byte("c")},
					{StartKey: []byte("c"), EndKey: []byte("f")},
					{StartKey: []byte("f"), EndKey: []byte("g")},
					{StartKey: []byte("g")},
				},
			},
			want: false,
		},
		{
			name: "region hole 1",
			args: args{
				allRegions: []*metapb.Region{
					{EndKey: []byte("a")},
					{StartKey: []byte("a"), EndKey: []byte("c")},
					{StartKey: []byte("c"), EndKey: []byte("f")},
					{StartKey: []byte("e"), EndKey: []byte("g")},
					{StartKey: []byte("g")},
				},
			},
			want: true,
		},
		{
			name: "multiple end keys(should not happen normally)",
			args: args{
				allRegions: []*metapb.Region{
					{EndKey: []byte("a")},
					{StartKey: []byte("a"), EndKey: []byte("c")},
					{StartKey: []byte("c")},
					{StartKey: []byte("e"), EndKey: []byte("g")},
					{StartKey: []byte("g")},
				},
			},
			want: true,
		},
		{
			name: "region hole",
			args: args{
				allRegions: []*metapb.Region{
					{EndKey: []byte("a")},
					{StartKey: []byte("b")},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRegionsHasHole(tt.args.allRegions); got != tt.want {
				t.Errorf("isRegionsHasHole() = %v, want %v", got, tt.want)
			}
		})
	}
}
