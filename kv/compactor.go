// Copyright 2015 PingCAP, Inc.
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

package kv

import "time"

// CompactPolicy defines gc policy of MVCC storage.
type CompactPolicy struct {
	// SafePoint specifies
	SafePoint int
	// TriggerInterval specifies how often should the compactor
	// scans outdated data.
	TriggerInterval time.Duration
	// BatchDeleteCnt specifies the batch size for
	// deleting outdated data transaction.
	BatchDeleteCnt int
}

// Compactor compacts MVCC storage.
type Compactor interface {
	// OnGet is the hook point on Txn.Get.
	OnGet(k Key)
	// OnSet is the hook point on Txn.Set.
	OnSet(k Key)
	// OnDelete is the hook point on Txn.Delete.
	OnDelete(k Key)
	// Compact is the function removes the given key.
	Compact(k Key) error
}
