// Copyright 2021 PingCAP, Inc.
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

package reporter

import (
	"time"
)

// DataSink sends data to the target server.
type DataSink interface {
	Send(data reportData, deadline time.Time)

	// IsPaused indicates that the DataSink is not expecting to receive records for now
	// and may resume in the future.
	IsPaused() bool

	// IsDown indicates that the DataSink has been down and can be cleared.
	// Note that: once a DataSink is down, it cannot go back to be up.
	IsDown() bool

	Close()
}
