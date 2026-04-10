// Copyright 2020 PingCAP, Inc.
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

package physicalop_test

import (
	"testing"

	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
)

// Type aliases for exported types
type PhysicalExchangeReceiver = physicalop.ExportPhysicalExchangeReceiver
type PhysicalExchangeSender = physicalop.ExportPhysicalExchangeSender
type PhysicalHashJoin = physicalop.ExportPhysicalHashJoin
type Fragment = physicalop.ExportFragment

func RunFragmentInitSingleton(t *testing.T) {
	r1, r2 := &PhysicalExchangeReceiver{}, &PhysicalExchangeReceiver{}
	r1.SetChildren(&PhysicalExchangeSender{ExchangeType: tipb.ExchangeType_PassThrough})
	r2.SetChildren(&PhysicalExchangeSender{ExchangeType: tipb.ExchangeType_Broadcast})
	p := &PhysicalHashJoin{}

	f := &Fragment{}
	p.SetChildren(r1, r1)
	err := physicalop.ExportFragmentInit(f, p)
	require.NoError(t, err)
	require.Equal(t, physicalop.ExportFragmentGetSingleton(f), true)

	f = &Fragment{}
	p.SetChildren(r1, r2)
	err = physicalop.ExportFragmentInit(f, p)
	require.NoError(t, err)
	require.Equal(t, physicalop.ExportFragmentGetSingleton(f), true)

	f = &Fragment{}
	p.SetChildren(r2, r1)
	err = physicalop.ExportFragmentInit(f, p)
	require.NoError(t, err)
	require.Equal(t, physicalop.ExportFragmentGetSingleton(f), true)

	f = &Fragment{}
	p.SetChildren(r2, r2)
	err = physicalop.ExportFragmentInit(f, p)
	require.NoError(t, err)
	require.Equal(t, physicalop.ExportFragmentGetSingleton(f), false)
}
