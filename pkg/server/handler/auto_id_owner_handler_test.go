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

package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeAutoIDOwnerChecker struct {
	healthy bool
	owner   bool
}

func (c *fakeAutoIDOwnerChecker) Health() bool {
	return c.healthy
}

func (c *fakeAutoIDOwnerChecker) IsAutoIDOwner() bool {
	return c.owner
}

func TestAutoIDOwnerHandler(t *testing.T) {
	checker := &fakeAutoIDOwnerChecker{healthy: true}
	h := NewAutoIDOwnerHandler(checker)

	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/owner_manager/auto_id_service", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"is_owner": false}`, recorder.Body.String())

	checker.owner = true
	recorder = httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/owner_manager/auto_id_service", nil))
	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"is_owner": true}`, recorder.Body.String())

	checker.healthy = false
	recorder = httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/owner_manager/auto_id_service", nil))
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
}
