// Copyright 2023 PingCAP, Inc.
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

package globalconn

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/ngaut/sync2"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// GCID is the Global Connection ID, providing UNIQUE connection IDs across the whole TiDB cluster.
// Used when GlobalKill feature is enable.
// See https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md
// 32 bits version:
//
//	 31    21 20               1    0
//	+--------+------------------+------+
//	|serverID|   local connID   |markup|
//	| (11b)  |       (20b)      |  =0  |
//	+--------+------------------+------+
//
// 64 bits version:
//
//	 63 62                 41 40                                   1   0
//	+--+---------------------+--------------------------------------+------+
//	|  |      serverID       |             local connID             |markup|
//	|=0|       (22b)         |                 (40b)                |  =1  |
//	+--+---------------------+--------------------------------------+------+
//
// NOTE:
// 1. `serverId“ in 64 bits version can be less than 2^11. This will happen when the 32 bits local connID has been used up, while `serverID` stay unchanged.
// 2. The local connID of a 32 bits GCID can be the same with another 64 bits GCID. This will not violate the uniqueness of GCID.
type GCID struct {
	ServerID    uint64
	LocalConnID uint64
	Is64bits    bool
}

var (
	// ServerIDBits32 is the number of bits of serverID for 32bits global connection ID.
	ServerIDBits32 uint = 11
	// MaxServerID32 is maximum serverID for 32bits global connection ID.
	MaxServerID32 uint64 = 1<<ServerIDBits32 - 1
	// LocalConnIDBits32 is the number of bits of localConnID for 32bits global connection ID.
	LocalConnIDBits32 uint = 20
	// MaxLocalConnID32 is maximum localConnID for 32bits global connection ID.
	MaxLocalConnID32 uint64 = 1<<LocalConnIDBits32 - 1
)

const (
	// MaxServerID64 is maximum serverID for 64bits global connection ID.
	MaxServerID64 = 1<<22 - 1
	// LocalConnIDBits64 is the number of bits of localConnID for 64bits global connection ID.
	LocalConnIDBits64 = 40
	// MaxLocalConnID64 is maximum localConnID for 64bits global connection ID.
	MaxLocalConnID64 = 1<<LocalConnIDBits64 - 1

	// ReservedCount is the count of reserved connection IDs for internal processes.
	ReservedCount = 200
)

// ToConnID returns the 64bits connection ID
func (g *GCID) ToConnID() uint64 {
	var id uint64
	if g.Is64bits {
		if g.LocalConnID > MaxLocalConnID64 {
			panic(fmt.Sprintf("unexpected localConnID %d exceeds %d", g.LocalConnID, MaxLocalConnID64))
		}
		if g.ServerID > MaxServerID64 {
			panic(fmt.Sprintf("unexpected serverID %d exceeds %d", g.ServerID, MaxServerID64))
		}

		id |= 0x1
		id |= g.LocalConnID << 1 // 40 bits local connID.
		id |= g.ServerID << 41   // 22 bits serverID.
	} else {
		if g.LocalConnID > MaxLocalConnID32 {
			panic(fmt.Sprintf("unexpected localConnID %d exceeds %d", g.LocalConnID, MaxLocalConnID32))
		}
		if g.ServerID > MaxServerID32 {
			panic(fmt.Sprintf("unexpected serverID %d exceeds %d", g.ServerID, MaxServerID32))
		}

		id |= g.LocalConnID << 1 // 20 bits local connID.
		id |= g.ServerID << 21   // 11 bits serverID.
	}
	return id
}

// ParseConnID parses an uint64 connection ID to GlobalConnID.
//
//	`isTruncated` indicates that older versions of the client truncated the 64-bit GlobalConnID to 32-bit.
func ParseConnID(id uint64) (g GCID, isTruncated bool, err error) {
	if id&0x80000000_00000000 > 0 {
		return GCID{}, false, errors.New("unexpected connectionID exceeds int64")
	}
	if id&0x1 > 0 { // 64bits
		if id&0xffffffff_00000000 == 0 {
			return GCID{}, true, nil
		}
		return GCID{
			Is64bits:    true,
			LocalConnID: (id >> 1) & MaxLocalConnID64,
			ServerID:    (id >> 41) & MaxServerID64,
		}, false, nil
	}

	// 32bits
	if id&0xffffffff_00000000 > 0 {
		return GCID{}, false, errors.New("unexpected connectionID exceeds uint32")
	}
	return GCID{
		Is64bits:    false,
		LocalConnID: (id >> 1) & MaxLocalConnID32,
		ServerID:    (id >> 21) & MaxServerID32,
	}, false, nil
}

///////////////////////////////// Class Diagram ///////////////////////////////////
//                                                                               //
//  +----------+      +-----------------+         +-----------------------+      //
//  |  Server  | ---> | ConnIDAllocator | <<--+-- | GlobalConnIDAllocator | --+  //
//  +----------+      +-----------------+     |   +-----------------------+   |  //
//                                            +-- | SimpleConnIDAllocator |   |  //
//                                                +----------+------------+   |  //
//                                                           |                |  //
//                                                           V                |  //
//                            +--------+          +----------------------+    |  //
//                            | IDPool | <<--+--  |     AutoIncPool      | <--+  //
//                            +--------+     |    +----------------------+    |  //
//                                           +--  | LockFreeCircularPool | <--+  //
//                                                +----------------------+       //
//                                                                               //
///////////////////////////////////////////////////////////////////////////////////

type serverIDGetterFn func() uint64

// Allocator allocates global connection IDs.
type Allocator interface {
	// NextID returns next connection ID.
	NextID() uint64
	// Release releases connection ID to allocator.
	Release(connectionID uint64)
	// GetReservedConnID returns reserved connection ID.
	GetReservedConnID(reservedNo uint64) uint64
}

var (
	_ Allocator = (*SimpleAllocator)(nil)
	_ Allocator = (*GlobalAllocator)(nil)
)

// SimpleAllocator is a simple connection id allocator used when GlobalKill feature is disable.
type SimpleAllocator struct {
	pool AutoIncPool
}

// NewSimpleAllocator creates a new SimpleAllocator.
func NewSimpleAllocator() *SimpleAllocator {
	a := &SimpleAllocator{}
	a.pool.Init(math.MaxUint64 - ReservedCount)
	return a
}

// NextID implements ConnIDAllocator interface.
func (a *SimpleAllocator) NextID() uint64 {
	id, _ := a.pool.Get()
	return id
}

// Release implements ConnIDAllocator interface.
func (a *SimpleAllocator) Release(id uint64) {
	a.pool.Put(id)
}

// GetReservedConnID implements ConnIDAllocator interface.
func (*SimpleAllocator) GetReservedConnID(reservedNo uint64) uint64 {
	if reservedNo >= ReservedCount {
		panic("invalid reservedNo exceed ReservedCount")
	}
	return math.MaxUint64 - reservedNo
}

// GlobalAllocator is global connection ID allocator.
type GlobalAllocator struct {
	is64bits       sync2.AtomicInt32 // !0: true, 0: false
	serverIDGetter func() uint64

	local32 LockFreeCircularPool
	local64 AutoIncPool
}

// is64 indicates allocate 64bits global connection ID or not.
func (g *GlobalAllocator) is64() bool {
	return g.is64bits.Get() != 0
}

// upgradeTo64 upgrade allocator to 64bits.
func (g *GlobalAllocator) upgradeTo64() {
	g.is64bits.Set(1)
	logutil.BgLogger().Info("GlobalAllocator upgrade to 64 bits")
}

func (g *GlobalAllocator) downgradeTo32() {
	g.is64bits.Set(0)
	logutil.BgLogger().Info("GlobalAllocator downgrade to 32 bits")
}

// LocalConnIDAllocator64TryCount is the try count of 64bits local connID allocation.
const LocalConnIDAllocator64TryCount = 10

// NewGlobalAllocator creates a GlobalAllocator.
func NewGlobalAllocator(serverIDGetter serverIDGetterFn, enable32Bits bool) *GlobalAllocator {
	g := &GlobalAllocator{
		serverIDGetter: serverIDGetter,
	}
	g.local32.InitExt(1<<LocalConnIDBits32, math.MaxUint32)
	g.local64.InitExt((1<<LocalConnIDBits64)-ReservedCount, true, LocalConnIDAllocator64TryCount)

	var is64 int32
	if enable32Bits {
		is64 = 0
	} else {
		is64 = 1
	}
	g.is64bits.Set(is64)
	return g
}

// NextID returns next connection ID.
func (g *GlobalAllocator) NextID() uint64 {
	globalConnID := g.Allocate()
	return globalConnID.ToConnID()
}

// GetReservedConnID implements ConnIDAllocator interface.
func (g *GlobalAllocator) GetReservedConnID(reservedNo uint64) uint64 {
	if reservedNo >= ReservedCount {
		panic("invalid reservedNo exceed ReservedCount")
	}

	serverID := g.serverIDGetter()
	globalConnID := GCID{
		ServerID:    serverID,
		LocalConnID: (1 << LocalConnIDBits64) - 1 - reservedNo,
		Is64bits:    true,
	}
	return globalConnID.ToConnID()
}

// Allocate allocates a new global connection ID.
func (g *GlobalAllocator) Allocate() GCID {
	serverID := g.serverIDGetter()

	// 32bits.
	if !g.is64() && serverID <= MaxServerID32 {
		localConnID, ok := g.local32.Get()
		if ok {
			return GCID{
				ServerID:    serverID,
				LocalConnID: localConnID,
				Is64bits:    false,
			}
		}
		g.upgradeTo64() // go on to 64bits.
	}

	// 64bits.
	localConnID, ok := g.local64.Get()
	if !ok {
		// local connID with 40bits pool size is big enough and should not be exhausted, as `MaxServerConnections` is no more than math.MaxUint32.
		panic(fmt.Sprintf("Failed to allocate 64bits local connID after try %v times. Should never happen", LocalConnIDAllocator64TryCount))
	}
	return GCID{
		ServerID:    serverID,
		LocalConnID: localConnID,
		Is64bits:    true,
	}
}

// Release releases connectionID to pool.
func (g *GlobalAllocator) Release(connectionID uint64) {
	globalConnID, isTruncated, err := ParseConnID(connectionID)
	if err != nil || isTruncated {
		logutil.BgLogger().Error("failed to ParseGlobalConnID", zap.Error(err), zap.Uint64("connectionID", connectionID), zap.Bool("isTruncated", isTruncated))
		return
	}

	if globalConnID.Is64bits {
		g.local64.Put(globalConnID.LocalConnID)
	} else {
		if ok := g.local32.Put(globalConnID.LocalConnID); ok {
			if g.local32.Len() < g.local32.Cap()/2 {
				g.downgradeTo32()
			}
		} else {
			logutil.BgLogger().Error("failed to release 32bits connection ID", zap.Uint64("connectionID", connectionID), zap.Uint64("localConnID", globalConnID.LocalConnID))
		}
	}
}

var (
	ldflagIsGlobalKillTest  = "0"  // 1:Yes, otherwise:No.
	ldflagServerIDBits32    = "11" // Bits of ServerID32.
	ldflagLocalConnIDBits32 = "20" // Bits of LocalConnID32.
)

func initByLDFlagsForGlobalKill() {
	if ldflagIsGlobalKillTest == "1" {
		var (
			i   int
			err error
		)

		if i, err = strconv.Atoi(ldflagServerIDBits32); err != nil {
			panic("invalid ldflagServerIDBits32")
		}
		ServerIDBits32 = uint(i)
		MaxServerID32 = 1<<ServerIDBits32 - 1

		if i, err = strconv.Atoi(ldflagLocalConnIDBits32); err != nil {
			panic("invalid ldflagLocalConnIDBits32")
		}
		LocalConnIDBits32 = uint(i)
		MaxLocalConnID32 = 1<<LocalConnIDBits32 - 1

		logutil.BgLogger().Info("global_kill_test is enabled",
			zap.Uint("ServerIDBits32", ServerIDBits32),
			zap.Uint64("MaxServerID32", MaxServerID32),
			zap.Uint("LocalConnIDBits32", LocalConnIDBits32),
			zap.Uint64("MaxLocalConnID32", MaxLocalConnID32),
		)
	}
}

func init() {
	initByLDFlagsForGlobalKill()
}
