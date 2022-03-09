// Copyright 2018 PingCAP, Inc.
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

package core

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestCacheKey(t *testing.T) {
	ctx := MockContext()
	ctx.GetSessionVars().SnapshotTS = 0
	ctx.GetSessionVars().SQLMode = mysql.ModeNone
	ctx.GetSessionVars().TimeZone = time.UTC
	ctx.GetSessionVars().ConnectionID = 0
	key, err := NewPlanCacheKey(ctx.GetSessionVars(), "", "test", 1)
	if err.Error() != "no statement text" {
		t.Fail() // no statement text
	}
	key, err = NewPlanCacheKey(ctx.GetSessionVars(), "select 1", "", 1)
	if err != nil {
		t.Fail() // schema can be nil
	}
	key, err = NewPlanCacheKey(ctx.GetSessionVars(), "select 1", "test", 1)
	if err != nil {
		t.Fail()
	}
	require.Equal(t, []byte{0x74, 0x65, 0x73, 0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x31, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x74, 0x69, 0x64, 0x62, 0x74, 0x69, 0x6b, 0x76, 0x74, 0x69, 0x66, 0x6c, 0x61, 0x73, 0x68, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, key.Hash())
}
