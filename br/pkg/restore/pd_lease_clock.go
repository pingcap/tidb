// Copyright 2026 PingCAP, Inc.
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

package restore

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

// PDLeaseClock gets lease time from PD TSO physical time.
type PDLeaseClock struct {
	pdClient pd.Client
}

// NewPDLeaseClock returns a lease clock backed by PD TSO.
func NewPDLeaseClock(pdClient pd.Client) objstore.LeaseClock {
	return PDLeaseClock{pdClient: pdClient}
}

// Now returns the physical time component of a retried PD TSO request.
func (c PDLeaseClock) Now(ctx context.Context) (time.Time, error) {
	ts, err := GetTSWithRetry(ctx, c.pdClient)
	if err != nil {
		return time.Time{}, errors.Trace(err)
	}
	return oracle.GetTimeFromTS(ts), nil
}
