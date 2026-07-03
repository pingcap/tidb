// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

func TestBuildContinuousKeyRangesByRegionStartKeys(t *testing.T) {
	tableStart := kv.Key("a")
	tableEnd := kv.Key("z")
	regionStartKeys := []kv.Key{
		kv.Key("n"),
		kv.Key("0"),
		kv.Key("c"),
		kv.Key("h"),
		kv.Key("h"),
		kv.Key("u"),
		kv.Key("x"),
	}

	ranges := buildContinuousKeyRangesByRegionStartKeys(tableStart, tableEnd, regionStartKeys, 2)

	require.Equal(t, []kv.KeyRange{
		{StartKey: kv.Key("a"), EndKey: kv.Key("n")},
		{StartKey: kv.Key("n"), EndKey: kv.Key("x")},
		{StartKey: kv.Key("x"), EndKey: kv.Key("z")},
	}, ranges)
}
