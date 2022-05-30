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

package util_test

import (
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/assert"
)

func TestGlobalConnID(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.EnableGlobalKill = true
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()
	connID := util.GlobalConnID{
		Is64bits:    true,
		ServerID:    1001,
		LocalConnID: 123,
	}
	assert.Equal(t, (uint64(1001)<<41)|(uint64(123)<<1)|1, connID.ID())

	next := connID.NextID()
	assert.Equal(t, (uint64(1001)<<41)|(uint64(124)<<1)|1, next)

	connID1, isTruncated, err := util.ParseGlobalConnID(next)
	assert.Nil(t, err)
	assert.False(t, isTruncated)
	assert.Equal(t, uint64(1001), connID1.ServerID)
	assert.Equal(t, uint64(124), connID1.LocalConnID)
	assert.True(t, connID1.Is64bits)

	_, isTruncated, err = util.ParseGlobalConnID(101)
	assert.Nil(t, err)
	assert.True(t, isTruncated)

	_, _, err = util.ParseGlobalConnID(0x80000000_00000321)
	assert.NotNil(t, err)

	connID2 := util.GlobalConnID{
		Is64bits:       true,
		ServerIDGetter: func() uint64 { return 2002 },
		LocalConnID:    123,
	}
	assert.Equal(t, (uint64(2002)<<41)|(uint64(123)<<1)|1, connID2.ID())
}
