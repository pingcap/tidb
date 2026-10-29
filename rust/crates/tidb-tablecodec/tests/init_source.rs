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

//! Go `tablecodec`'s `init`: the table-ID decoder it installs as
//! `kv.DecodeTableIDFunc`, read by the resource-group tagger.

use prost::Message;
use tidb_proto::ResourceGroupTag;
use tidb_tablecodec::table_key::{encode_row_key, gen_table_record_prefix};
use tidb_txnkv::ResourceGroupTagBuilder;

fn tagged_table_id(key: &[u8]) -> Option<i64> {
    let tag = ResourceGroupTagBuilder::new(None).encode_tag_with_key(key);
    ResourceGroupTag::decode(tag.as_slice())
        .expect("valid tag")
        .table_id
}

#[test]
fn init_installs_the_table_id_decoder_with_gos_pre_check() {
    tidb_tablecodec::init();
    assert_eq!(tagged_table_id(&encode_row_key(15, &[0x80; 8])), Some(15));
    assert_eq!(tagged_table_id(&gen_table_record_prefix(42)), Some(42));
    // Go's pre-check: a key without the table prefix -- an API V2 key, whose
    // `x` + keyspace prefix `DecodeTableID` itself would strip -- or one
    // shorter than `TableSplitKeyLen` tags table 0.
    let mut keyspace_key = vec![b'x', 0, 0, 1];
    keyspace_key.extend(encode_row_key(15, &[0x80; 8]));
    assert_eq!(tagged_table_id(&keyspace_key), Some(0));
    assert_eq!(tagged_table_id(&gen_table_record_prefix(42)[..8]), Some(0));
    // A second call keeps the one decoder.
    tidb_tablecodec::init();
    assert_eq!(tagged_table_id(&encode_row_key(16, &[0x80; 8])), Some(16));
}
