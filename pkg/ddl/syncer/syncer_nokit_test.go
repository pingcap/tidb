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

package syncer

import (
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestDecodeJobVersionEvent(t *testing.T) {
	prefix := util.DDLAllSchemaVersionsByJob + "/"
	_, _, _, valid := decodeJobVersionEvent(&mvccpb.KeyValue{Key: []byte(prefix + "1")}, mvccpb.PUT, prefix)
	require.False(t, valid)
	_, _, _, valid = decodeJobVersionEvent(&mvccpb.KeyValue{Key: []byte(prefix + "a/aa")}, mvccpb.PUT, prefix)
	require.False(t, valid)
	_, _, _, valid = decodeJobVersionEvent(&mvccpb.KeyValue{
		Key: []byte(prefix + "1/aa"), Value: []byte("aa")}, mvccpb.PUT, prefix)
	require.False(t, valid)
	jobID, tidbID, schemaVer, valid := decodeJobVersionEvent(&mvccpb.KeyValue{
		Key: []byte(prefix + "1/aa"), Value: []byte("123")}, mvccpb.PUT, prefix)
	require.True(t, valid)
	require.EqualValues(t, 1, jobID)
	require.EqualValues(t, "aa", tidbID)
	require.EqualValues(t, 123, schemaVer)
	// value is not used on delete
	jobID, tidbID, schemaVer, valid = decodeJobVersionEvent(&mvccpb.KeyValue{
		Key: []byte(prefix + "1/aa"), Value: []byte("aaaa")}, mvccpb.DELETE, prefix)
	require.True(t, valid)
	require.EqualValues(t, 1, jobID)
	require.EqualValues(t, "aa", tidbID)
	require.EqualValues(t, 0, schemaVer)
}
