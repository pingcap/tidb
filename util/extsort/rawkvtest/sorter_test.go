// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rawkvtest

import (
	"context"
	"flag"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
)

var (
	// PdAddr is used by rawkv client.
	PdAddr = flag.String("pd-addr", "127.0.0.1:2379", "PD addr")
)

func TestPlayground(t *testing.T) {
	ctx := context.Background()
	cli, err := rawkv.NewClient(ctx, strings.Split(*PdAddr, ","), config.Security{})
	require.NoError(t, err)

	err = cli.Put(ctx, []byte("key"), []byte("value"))
	require.NoError(t, err)
	err = cli.Delete(ctx, []byte("key"))
	require.NoError(t, err)
}
