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

package local

import "github.com/pingcap/tidb/br/pkg/lightning/verification"

// RemoteChecksum represents a checksum result got from tidb.
type RemoteChecksum struct {
	Schema     string
	Table      string
	Checksum   uint64
	TotalKVs   uint64
	TotalBytes uint64
}

// IsEqual checks whether the checksum is equal to the other.
func (rc *RemoteChecksum) IsEqual(other *verification.KVChecksum) bool {
	return rc.Checksum == other.Sum() &&
		rc.TotalKVs == other.SumKVS() &&
		rc.TotalBytes == other.SumSize()
}
