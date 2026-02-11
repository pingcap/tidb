// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikvhandler

import (
	"net/http"
	"testing"

	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/stretchr/testify/require"
)

func TestParseAdminCheckIndexLimit(t *testing.T) {
	limit, err := parseAdminCheckIndexLimit(&http.Request{})
	require.NoError(t, err)
	require.Equal(t, adminCheckIndexDefaultLimit, limit)

	limit, err = parseAdminCheckIndexLimit(&http.Request{Form: map[string][]string{handler.Limit: {"200"}}})
	require.NoError(t, err)
	require.Equal(t, 200, limit)

	_, err = parseAdminCheckIndexLimit(&http.Request{Form: map[string][]string{handler.Limit: {"abc"}}})
	require.ErrorContains(t, err, "invalid limit")

	_, err = parseAdminCheckIndexLimit(&http.Request{Form: map[string][]string{handler.Limit: {"0"}}})
	require.ErrorContains(t, err, "greater than 0")

	_, err = parseAdminCheckIndexLimit(&http.Request{Form: map[string][]string{handler.Limit: {"-1"}}})
	require.ErrorContains(t, err, "greater than 0")
}
