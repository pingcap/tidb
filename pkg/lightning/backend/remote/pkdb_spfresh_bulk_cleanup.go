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

package remote

import (
	"context"
	"net/http"

	"github.com/pingcap/errors"
)

// RetireSPFreshBulkBuild cancels the fenced run and checks that all its work has drained.
// Busy and transport failures leave the durable cleanup task pending for retry.
func RetireSPFreshBulkBuild(ctx context.Context, client *http.Client, endpoint, runID string) error {
	request := struct {
		BuildID string `json:"build_id"`
	}{BuildID: runID}
	var response struct {
		Retired bool `json:"retired"`
	}
	if err := requestSPFreshDistributedBuild(ctx, client, endpoint, "/spfresh/bulk-retire", "retire SPFresh bulk build", request, &response); err != nil {
		return err
	}
	if !response.Retired {
		return errors.New("SPFresh bulk retire did not acknowledge the writer barrier")
	}
	return nil
}
