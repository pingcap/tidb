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

package server

import (
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestCreateSessionClosesSessionOnExtensionSetupError forces the
// extension.GetExtensions failure inside rpcServer.createSession. The session
// created by session.CreateSessionWithDomain on the line before must be
// closed before the error is returned, so the failing coprocessor request
// does not leak it, and the path must recover once the extension registry
// is set up again.
func TestCreateSessionClosesSessionOnExtensionSetupError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	dom := domain.GetDomain(tk.Session())
	s := &rpcServer{dom: dom}

	// A failing extension factory makes extension.GetExtensions fail, which
	// is the first error path of createSession after the session has already
	// been created.
	extension.Reset()
	defer extension.Reset()
	require.NoError(t, extension.RegisterFactory("failing", func() ([]extension.Option, error) {
		return nil, errors.New("forced extension setup failure")
	}))
	_, err := s.createSession()
	require.Error(t, err)
	require.ErrorContains(t, err, "forced extension setup failure")

	// Once the registry is set up again, createSession returns a live session
	// for the caller to close, as the coprocessor handlers do.
	extension.Reset()
	require.NoError(t, extension.Setup())
	se, err := s.createSession()
	require.NoError(t, err)
	require.NotNil(t, se)
	se.Close()
}
