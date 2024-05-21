// Copyright 2024 PingCAP, Inc.
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

package brietest

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

func TestCreateClient(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/pdutil/DNSError", "119*return"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/pdutil/FastRetry", "return(true)"))
	ctl, err := pdutil.NewPdController(context.Background(), "127.0.0.1:2379", nil, pd.SecurityOption{})
	require.NoError(t, err)
	ctl.Close()
}
