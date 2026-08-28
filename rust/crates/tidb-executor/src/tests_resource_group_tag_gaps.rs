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

//! Gap test for Go `pkg/executor/resource_tag_test.go:37::TestResourceGroupTag`.

/// Go `pkg/executor/resource_tag_test.go:37::TestResourceGroupTag`: with Top
/// SQL enabled and the unistore `unistoreRPCClientSendHook` failpoint armed,
/// every RPC the mock store issues (Get/BatchGet/Prewrite/Commit/Cop/
/// PessimisticLock) carries a `kvrpcpb.Context` whose `resource_group_tag`
/// decodes to a `tipb.ResourceGroupTag` whose SQL digest equals the executed
/// statement's digest, and whose labels name the start-key's table
/// (`tablecodec.DecodeTableID`). Needs the unistore mock store, the Top SQL
/// state machine (`pkg/util/topsql/state`), and the digest propagation
/// chain.
#[test]
#[ignore = "go-parity-gap: no unistore RPC hook / Top SQL pipeline — resource_group_tag construction per tikvrpc.Request (pkg/store/driver, pkg/util/topsql) is unported"]
fn resource_group_tag_carries_the_sql_digest_and_table_label() {}
