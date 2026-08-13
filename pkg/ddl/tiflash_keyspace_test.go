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
	"context"
	stdErrors "errors"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
)

type stubPDClient struct {
	pd.Client
	meta *keyspacepb.KeyspaceMeta
	err  error
}

func (c *stubPDClient) LoadKeyspace(context.Context, string) (*keyspacepb.KeyspaceMeta, error) {
	return c.meta, c.err
}

type stubStore struct {
	kv.Storage
	keyspaceName string
	meta         *keyspacepb.KeyspaceMeta
	loadErr      error
}

func (s *stubStore) GetKeyspace() string {
	return s.keyspaceName
}

func (s *stubStore) GetPDClient() pd.Client {
	return &stubPDClient{meta: s.meta, err: s.loadErr}
}

func (s *stubStore) GetPDHTTPClient() pdhttp.Client {
	return nil
}

func TestCheckColumnarStorageEnabled(t *testing.T) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.CSE.ColumnarStoreType = "columnar"
	})
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.CSE.ColumnarStoreType = "tiflash"
	})

	mkStore := func(name string, meta *keyspacepb.KeyspaceMeta, loadErr error) kv.Storage {
		return &stubStore{keyspaceName: name, meta: meta, loadErr: loadErr}
	}
	flagMeta := func(v string) *keyspacepb.KeyspaceMeta {
		return &keyspacepb.KeyspaceMeta{
			Name:   "testks",
			Config: map[string]string{keyspace.KeyspaceConfigColumnarStorageEnabled: v},
		}
	}

	tests := []struct {
		name   string
		store  kv.Storage
		expect *dbterror.Error
	}{
		{
			name:   "flag false",
			store:  mkStore("testks", flagMeta("false"), nil),
			expect: dbterror.ErrTiFlashColumnarStorageNotEnabled,
		},
		{
			name:   "flag true",
			store:  mkStore("testks", flagMeta("true"), nil),
			expect: nil,
		},
		{
			name:   "flag missing",
			store:  mkStore("testks", &keyspacepb.KeyspaceMeta{Name: "testks"}, nil),
			expect: nil,
		},
		{
			name:   "load keyspace meta error",
			store:  mkStore("testks", nil, stdErrors.New("pd unavailable")),
			expect: dbterror.ErrTiFlashColumnarStorageCheckFailed,
		},
		{
			name:   "null keyspace",
			store:  mkStore("", nil, nil),
			expect: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := checkColumnarStorageEnabled(tc.store)
			if tc.expect == nil {
				require.NoError(t, err)
			} else {
				require.True(t, tc.expect.Equal(err), "expected %v, got %v", tc.expect, err)
			}
		})
	}

	// Non-columnar architecture skips the gate even if the flag is false.
	config.UpdateGlobal(func(conf *config.Config) {
		conf.CSE.ColumnarStoreType = "tiflash"
	})
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.CSE.ColumnarStoreType = "columnar"
	})
	require.NoError(t, checkColumnarStorageEnabled(mkStore("testks", flagMeta("false"), nil)))
}
