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

package ddl

import (
	stdErrors "errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/backend/remote"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestSPFreshSealTerminalErrorRollsBackWithoutCommit(t *testing.T) {
	for _, body := range []string{"build task or durable proof unavailable", "task failed: context deadline exceeded", "task failed: requested lease not found"} {
		for _, status := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusConflict} {
			t.Run(http.StatusText(status)+"/"+body, func(t *testing.T) {
				seals, commits := 0, 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/spfresh/build/commit" {
						commits++
					} else {
						seals++
					}
					http.Error(w, body, status)
				}))
				t.Cleanup(server.Close)
				err := runSPFreshCoordinatorTestSubtask(t, server, true)
				require.ErrorContains(t, err, fmt.Sprintf("SPFresh build seal failed (HTTP %d)", status))
				var controlErr *remote.SPFreshControlRequestError
				require.True(t, stdErrors.As(err, &controlErr))
				require.Equal(t, body, controlErr.Body)
				require.Equal(t, status, controlErr.StatusCode)
				cause, normalized := errors.Cause(err).(*terror.Error)
				require.True(t, normalized)
				// Use the DXF storage error encoding contract: flatten the
				// message while preserving RFC and MySQL codes, then decode.
				persisted := errors.Normalize(errors.GetErrStackMsg(err),
					errors.RFCCodeText(string(cause.RFCCode())),
					errors.MySQLErrorCode(int(cause.Code())))
				encoded, marshalErr := persisted.MarshalJSON()
				require.NoError(t, marshalErr)
				decoded := errors.Normalize("")
				require.NoError(t, decoded.UnmarshalJSON(encoded))
				for _, candidate := range []error{err, errors.Trace(err), decoded} {
					require.True(t, dbterror.ErrIngestFailed.Equal(candidate))
					require.False(t, common.IsRetryableError(candidate))
					require.False(t, (&backfillDistExecutor{}).IsRetryableError(candidate))
					require.False(t, isRetryableError(candidate))
					require.False(t, isRetryableJobError(candidate, 0))
				}
				require.Equal(t, 1, seals)
				require.Zero(t, commits)
			})
		}
	}
}
