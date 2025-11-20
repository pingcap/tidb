// Copyright 2025 PingCAP, Inc.
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

package errdef

import (
	"fmt"

	"github.com/pingcap/errors"
)

// errors of ingest API
var (
	ErrNoLeader              = errors.Normalize("region has no leader, region '%d'", errors.RFCCodeText("KV:ErrNoLeader"))
	ErrKVEpochNotMatch       = errors.Normalize("epoch not match", errors.RFCCodeText("Ingest:EpochNotMatch"))
	ErrKVNotLeader           = errors.Normalize("not leader", errors.RFCCodeText("Ingest:NotLeader"))
	ErrKVServerIsBusy        = errors.Normalize("server is busy", errors.RFCCodeText("Ingest:ServerIsBusy"))
	ErrKVRegionNotFound      = errors.Normalize("region not found", errors.RFCCodeText("Ingest:RegionNotFound"))
	ErrKVReadIndexNotReady   = errors.Normalize("read index not ready", errors.RFCCodeText("Ingest:ReadIndexNotReady"))
	ErrKVDiskFull            = errors.Normalize("store disk full", errors.RFCCodeText("Ingest:StoreDiskFull"))
	ErrKVIngestFailed        = errors.Normalize("ingest tikv failed", errors.RFCCodeText("Ingest:ErrKVIngestFailed"))
	ErrKVRaftProposalDropped = errors.Normalize("raft proposal dropped", errors.RFCCodeText("Ingest:ErrKVRaftProposalDropped"))
)

// HTTPStatusError is used in nextgen write and ingest API to indicate that the
// request failed with a non 200 status code, and there is no response body to
// indicate what the error detail is, we use this to help determine whether it
// can be retried or not.
type HTTPStatusError struct {
	StatusCode int
	Message    string
}

var _ error = (*HTTPStatusError)(nil)

// Error implements the error interface.
func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("request failed with status code %d: %s", e.StatusCode, e.Message)
}
