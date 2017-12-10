// Copyright 2017 PingCAP, Inc.
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

package schedulers

import (
	"github.com/pingcap/check"
	"github.com/pingcap/pd/server/schedule"
)

// CheckAddPeer check add peer
func CheckAddPeer(c *check.C, op *schedule.Operator, storeID uint64) {
	c.Assert(op, check.NotNil)
	c.Assert(op.Len(), check.Equals, 1)
	c.Assert(op.Step(0).(schedule.AddPeer).ToStore, check.Equals, storeID)
}

// CheckTransferLeader check whether leader is transfered
func CheckTransferLeader(c *check.C, op *schedule.Operator, sourceID, targetID uint64) {
	c.Assert(op, check.NotNil)
	c.Assert(op.Len(), check.Equals, 1)
	c.Assert(op.Step(0), check.Equals, schedule.TransferLeader{FromStore: sourceID, ToStore: targetID})
}

// CheckTransferPeer checks peer transfer
func CheckTransferPeer(c *check.C, op *schedule.Operator, sourceID, targetID uint64) {
	c.Assert(op, check.NotNil)
	if op.Len() == 2 {
		c.Assert(op.Step(0).(schedule.AddPeer).ToStore, check.Equals, targetID)
		c.Assert(op.Step(1).(schedule.RemovePeer).FromStore, check.Equals, sourceID)
	} else {
		c.Assert(op.Len(), check.Equals, 3)
		c.Assert(op.Step(0).(schedule.AddPeer).ToStore, check.Equals, targetID)
		c.Assert(op.Step(1).(schedule.TransferLeader).FromStore, check.Equals, sourceID)
		c.Assert(op.Step(2).(schedule.RemovePeer).FromStore, check.Equals, sourceID)
	}
}
