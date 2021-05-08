// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mocktikv

import "github.com/pingcap/kvproto/pkg/kvrpcpb"

// PutMutations is exported for testing.
var PutMutations = putMutations

func putMutations(kvpairs ...string) []*kvrpcpb.Mutation {
	var mutations []*kvrpcpb.Mutation
	for i := 0; i < len(kvpairs); i += 2 {
		mutations = append(mutations, &kvrpcpb.Mutation{
			Op:    kvrpcpb.Op_Put,
			Key:   []byte(kvpairs[i]),
			Value: []byte(kvpairs[i+1]),
		})
	}
	return mutations
}

// MustPrewrite write mutations to mvcc store.
func MustPrewrite(store MVCCStore, mutations []*kvrpcpb.Mutation, primary string, startTS uint64, ttl uint64) bool {
	return mustPrewriteWithTTL(store, mutations, primary, startTS, ttl)
}

func mustPrewriteWithTTL(store MVCCStore, mutations []*kvrpcpb.Mutation, primary string, startTS uint64, ttl uint64) bool {
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    mutations,
		PrimaryLock:  []byte(primary),
		StartVersion: startTS,
		LockTtl:      ttl,
		MinCommitTs:  startTS + 1,
	}
	errs := store.Prewrite(req)
	for _, err := range errs {
		if err != nil {
			return false
		}
	}
	return true
}
