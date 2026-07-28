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

//! Byte vectors captured from Go, pinning this crate to `pkg/meta`'s real
//! output rather than to a re-reading of its source.
//!
//! Generator (throwaway, deleted after capture): `pkg/meta/zz_dump_metavec_test.go`,
//! `package meta`, run as
//! `go test -tags=intest -run TestZZDumpMetaVectors ./pkg/meta/ -v`.
//! It called `structure.NewStructure(nil, nil, mMetaPrefix)` and the exported
//! `EncodeStringDataKey` / `EncodeHashDataKey` / `EncodeHashMetaKey` directly,
//! and `json.Marshal` on the real `model.DBInfo` / `model.TableInfo`.

use tidb_meta::key;
use tidb_meta::structure::{
    decode_hash_data_key, decode_string_data_key, encode_hash_data_key,
    encode_hash_data_key_prefix, encode_hash_meta_key, encode_string_data_key,
};
use tidb_meta::value;

/// Decodes one captured hex vector.
fn hex(input: &str) -> Vec<u8> {
    assert!(input.len().is_multiple_of(2), "odd-length hex vector");
    (0..input.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&input[at..at + 2], 16).expect("hex digit"))
        .collect()
}

// --- String-data keys: m + EncodeBytes(key) + EncodeUint('s') ---------------

const STRING_KEY_VECTORS: &[(&[u8], &str)] = &[
    (
        key::NEXT_GLOBAL_ID,
        "6d4e657874476c6f62ff616c494400000000fb0000000000000073",
    ),
    (
        key::SCHEMA_VERSION,
        "6d536368656d615665ff7273696f6e4b6579ff0000000000000000f70000000000000073",
    ),
    (
        key::BOOTSTRAP,
        "6d426f6f7473747261ff704b657900000000fb0000000000000073",
    ),
    (
        key::DDL_TABLE_VERSION,
        "6d44444c5461626c65ff56657273696f6e00fe0000000000000073",
    ),
    (
        key::BOOT_TABLE_VERSION,
        "6d426f6f745461626cff6556657273696f6eff0000000000000000f70000000000000073",
    ),
    (key::BDR_ROLE, "6d424452526f6c6500fe0000000000000073"),
    (
        key::METADATA_LOCK,
        "6d6d65746164617461ff4c6f636b00000000fb0000000000000073",
    ),
    (
        key::SCHEMA_CACHE_SIZE,
        "6d536368656d614361ff63686553697a6500fe0000000000000073",
    ),
    (
        key::POLICY_GLOBAL_ID,
        "6d506f6c696379476cff6f62616c49440000fd0000000000000073",
    ),
    (
        key::MASKING_POLICY_GLOBAL_ID,
        "6d4d61736b696e6750ff6f6c696379476c6fff62616c4944000000fc0000000000000073",
    ),
    (b"", "6d0000000000000000f70000000000000073"),
];

#[test]
fn string_data_keys_match_go() {
    for (name, expected) in STRING_KEY_VECTORS {
        let encoded = encode_string_data_key(name);
        assert_eq!(
            encoded,
            hex(expected),
            "key {:?}",
            String::from_utf8_lossy(name)
        );
        assert_eq!(decode_string_data_key(&encoded).unwrap(), *name);
    }
}

#[test]
fn schema_diff_key_matches_go() {
    // Go: EncodeStringDataKey([]byte("Diff:7")).
    assert_eq!(
        key::schema_diff_kv_key(7),
        hex("6d446966663a370000fd0000000000000073")
    );
}

#[test]
fn named_string_key_helpers_agree_with_the_generic_encoder() {
    assert_eq!(
        key::next_global_id_kv_key(),
        encode_string_data_key(key::NEXT_GLOBAL_ID)
    );
    assert_eq!(
        key::schema_version_kv_key(),
        encode_string_data_key(key::SCHEMA_VERSION)
    );
    assert_eq!(
        key::bootstrap_kv_key(),
        encode_string_data_key(key::BOOTSTRAP)
    );
}

// --- Hash-data keys: m + EncodeBytes(key) + 'h' + EncodeBytes(field) --------

#[test]
fn hash_data_keys_match_go() {
    let cases: &[(Vec<u8>, Vec<u8>, &str)] = &[
        (
            key::DBS.to_vec(),
            key::db_key(1),
            "6d4442730000000000fa000000000000006844423a3100000000fb",
        ),
        (
            key::DBS.to_vec(),
            key::db_key(12_345_678_901),
            "6d4442730000000000fa000000000000006844423a3132333435ff3637383930310000fd",
        ),
        (
            key::db_key(1),
            key::table_key(2),
            "6d44423a3100000000fb00000000000000685461626c653a3200fe",
        ),
        (
            key::db_key(1),
            key::auto_table_id_key(2),
            "6d44423a3100000000fb00000000000000685449443a32000000fc",
        ),
        (
            key::db_key(1),
            key::auto_increment_id_key(2),
            "6d44423a3100000000fb00000000000000684949443a32000000fc",
        ),
        (
            key::db_key(1),
            key::auto_random_table_id_key(2),
            "6d44423a3100000000fb000000000000006854415249443a3200fe",
        ),
        (
            key::db_key(1),
            key::sequence_key(2),
            "6d44423a3100000000fb00000000000000685349443a32000000fc",
        ),
        (
            key::POLICIES.to_vec(),
            key::policy_key(3),
            "6d506f6c6963696573ff0000000000000000f70000000000000068506f6c6963793a33ff0000000000000000f7",
        ),
        (
            key::MASKING_POLICIES.to_vec(),
            key::masking_policy_key(3),
            "6d4d61736b696e6750ff6f6c696369657300fe00000000000000684d61736b696e6750ff6f6c6963793a3300fe",
        ),
        (
            key::RESOURCE_GROUPS.to_vec(),
            key::resource_group_key(1),
            "6d5265736f75726365ff47726f7570730000fd000000000000006852473a3100000000fb",
        ),
    ];

    for (hash_key, field, expected) in cases {
        let encoded = encode_hash_data_key(hash_key, field);
        assert_eq!(
            encoded,
            hex(expected),
            "hash {:?} field {:?}",
            String::from_utf8_lossy(hash_key),
            String::from_utf8_lossy(field)
        );
        let (decoded_key, decoded_field) = decode_hash_data_key(&encoded).unwrap();
        assert_eq!(
            (decoded_key, decoded_field),
            (hash_key.clone(), field.clone())
        );
    }
}

#[test]
fn named_hash_key_helpers_agree_with_the_generic_encoder() {
    assert_eq!(
        key::database_kv_key(1),
        encode_hash_data_key(key::DBS, &key::db_key(1))
    );
    assert_eq!(
        key::table_kv_key(1, 2),
        encode_hash_data_key(&key::db_key(1), &key::table_key(2))
    );
    assert_eq!(
        key::auto_table_id_kv_key(1, 2),
        encode_hash_data_key(&key::db_key(1), &key::auto_table_id_key(2))
    );
    assert_eq!(
        key::auto_increment_id_kv_key(1, 2),
        encode_hash_data_key(&key::db_key(1), &key::auto_increment_id_key(2))
    );
    assert_eq!(
        key::auto_random_table_id_kv_key(1, 2),
        encode_hash_data_key(&key::db_key(1), &key::auto_random_table_id_key(2))
    );
    assert_eq!(
        key::policy_kv_key(3),
        encode_hash_data_key(key::POLICIES, &key::policy_key(3))
    );
    assert_eq!(
        key::resource_group_kv_key(1),
        encode_hash_data_key(key::RESOURCE_GROUPS, &key::resource_group_key(1))
    );
}

#[test]
fn hash_scan_prefixes_match_go() {
    // Go hashDataKeyPrefix("DBs") and EncodeHashMetaKey("DBs").
    assert_eq!(
        encode_hash_data_key_prefix(key::DBS),
        hex("6d4442730000000000fa0000000000000068")
    );
    assert_eq!(
        encode_hash_meta_key(key::DBS),
        hex("6d4442730000000000fa0000000000000048")
    );
    assert_eq!(
        key::databases_kv_prefix(),
        encode_hash_data_key_prefix(key::DBS)
    );
    assert_eq!(
        key::database_metas_kv_prefix(7),
        encode_hash_data_key_prefix(&key::db_key(7))
    );

    // Every field of a hash sorts under that hash's prefix.
    let prefix = key::databases_kv_prefix();
    assert!(key::database_kv_key(1).starts_with(&prefix));
    assert!(key::database_kv_key(i64::MAX).starts_with(&prefix));
}

// --- Field names -----------------------------------------------------------

#[test]
fn field_names_match_go() {
    assert_eq!(key::db_key(1), b"DB:1");
    assert_eq!(key::table_key(2), b"Table:2");
    assert_eq!(key::auto_table_id_key(2), b"TID:2");
    assert_eq!(key::auto_increment_id_key(2), b"IID:2");
    assert_eq!(key::auto_random_table_id_key(2), b"TARID:2");
    assert_eq!(key::sequence_key(2), b"SID:2");
    assert_eq!(key::sequence_cycle_key(2), b"SequenceCycle:2");
    assert_eq!(key::schema_diff_key(7), b"Diff:7");
    assert_eq!(key::policy_key(3), b"Policy:3");
    assert_eq!(key::masking_policy_key(3), b"MaskingPolicy:3");
    assert_eq!(key::resource_group_key(1), b"RG:1");

    assert_eq!(key::parse_db_key(b"DB:1").unwrap(), 1);
    assert_eq!(key::parse_table_key(b"Table:-5").unwrap(), -5);
    assert!(key::parse_db_key(b"Table:1").is_err());
    assert!(key::parse_table_key(b"Table:").is_err());

    // Go's Is*Key family tests the "<prefix>:" prefix, so `TID:1` is not a
    // `Table:` key even though "T" is shared.
    assert!(key::has_prefix(key::TABLE_PREFIX, b"Table:1"));
    assert!(!key::has_prefix(key::TABLE_PREFIX, b"TID:1"));
    assert!(!key::has_prefix(key::TABLE_PREFIX, b"Table"));
}

// --- Scalar values ---------------------------------------------------------

#[test]
fn int_values_match_go() {
    for (value, expected) in [
        (0_i64, "30"),
        (42, "3432"),
        (-1, "2d31"),
        (i64::MAX, "39323233333732303336383534373735383037"),
    ] {
        let encoded = value::encode_int_value(value);
        assert_eq!(encoded, hex(expected), "value {value}");
        assert_eq!(value::parse_int_value(&encoded).unwrap(), value);
    }
    assert!(value::parse_int_value(b"").is_err());
    assert!(value::parse_int_value(b"1.5").is_err());
}

#[test]
fn magic_byte_matches_go() {
    assert_eq!(value::attach_magic_byte(b"{}"), b"\x00{}");
    assert_eq!(value::detach_magic_byte(b"\x00{}").unwrap(), b"{}");
    // 0x40 and above select a handler module that does not exist.
    assert!(value::detach_magic_byte(b"\x40{}").is_err());
    assert!(value::detach_magic_byte(b"").is_err());
}

// --- Catalog JSON ----------------------------------------------------------

/// `json.Marshal(&model.DBInfo{ID: 3, Name: "Test", Charset: "utf8mb4",
/// Collate: "utf8mb4_bin", State: StatePublic})`.
const GO_DBINFO: &str = r#"{"id":3,"db_name":{"O":"Test","L":"test"},"charset":"utf8mb4","collate":"utf8mb4_bin","Deprecated":{},"state":5,"policy_ref_info":null}"#;

/// `json.Marshal(&model.DBInfo{})`.
const GO_DBINFO_ZERO: &str = r#"{"id":0,"db_name":{"O":"","L":""},"charset":"","collate":"","Deprecated":{},"state":0,"policy_ref_info":null}"#;

/// `json.Marshal` of a two-column, one-index `model.TableInfo`.
const GO_TABLEINFO: &str = r#"{"id":77,"name":{"O":"T","L":"t"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"Id","L":"id"},"offset":0,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":8,"Flag":3,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":2,"name":{"O":"Name","L":"name"},"offset":1,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":15,"Flag":0,"Flen":64,"Decimal":-1,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2}],"index_info":[{"id":1,"idx_name":{"O":"idx_name","L":"idx_name"},"tbl_name":{"O":"t","L":"t"},"idx_cols":[{"name":{"O":"Name","L":"name"},"offset":1,"length":-1}],"state":5,"backfill_state":0,"comment":"","index_type":0,"is_unique":true,"is_primary":false,"is_invisible":false,"is_global":false,"mv_index":false,"vector_index":null,"inverted_index":null,"full_text_index":null,"condition_expr_string":""}],"constraint_info":null,"fk_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"common_handle_version":0,"comment":"hi","auto_inc_id":0,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":2,"max_idx_id":1,"max_fk_id":0,"max_cst_id":0,"update_timestamp":445566778899,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"auto_random_range_bits":0,"pre_split_regions":0,"partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":5,"tiflash_replica":null,"is_columnar":false,"temp_table_type":0,"cache_table_status":0,"policy_ref_info":null,"stats_options":null,"exchange_partition_info":null,"ttl_info":null,"revision":0}"#;

/// `json.Marshal(&model.TableInfo{})`.
const GO_TABLEINFO_ZERO: &str = r#"{"id":0,"name":{"O":"","L":""},"charset":"","collate":"","cols":null,"index_info":null,"constraint_info":null,"fk_info":null,"state":0,"pk_is_handle":false,"is_common_handle":false,"common_handle_version":0,"comment":"","auto_inc_id":0,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":0,"max_idx_id":0,"max_fk_id":0,"max_cst_id":0,"update_timestamp":0,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"auto_random_range_bits":0,"pre_split_regions":0,"partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":0,"tiflash_replica":null,"is_columnar":false,"temp_table_type":0,"cache_table_status":0,"policy_ref_info":null,"stats_options":null,"exchange_partition_info":null,"ttl_info":null,"revision":0}"#;

#[test]
fn dbinfo_round_trips_byte_identically_with_go() {
    for stored in [GO_DBINFO, GO_DBINFO_ZERO] {
        let db = value::parse_db_info(stored.as_bytes()).unwrap();
        let reserialized = value::serialize_db_info(&db).unwrap();
        assert_eq!(String::from_utf8(reserialized).unwrap(), stored);
    }

    let db = value::parse_db_info(GO_DBINFO.as_bytes()).unwrap();
    assert_eq!(db.id, 3);
    assert_eq!(db.name.original(), "Test");
    assert_eq!(db.name.lowercase(), "test");
    assert_eq!(db.charset, "utf8mb4");
    assert_eq!(db.collate, "utf8mb4_bin");
}

#[test]
fn tableinfo_round_trips_byte_identically_with_go() {
    for stored in [GO_TABLEINFO, GO_TABLEINFO_ZERO] {
        let table = value::parse_table_info(stored.as_bytes(), 0).unwrap();
        let reserialized = value::serialize_table_info(&table).unwrap();
        assert_eq!(String::from_utf8(reserialized).unwrap(), stored);
    }

    let table = value::parse_table_info(GO_TABLEINFO.as_bytes(), 3).unwrap();
    assert_eq!(table.id, 77);
    assert_eq!(table.name.original(), "T");
    assert_eq!(table.columns.len(), 2);
    assert_eq!(table.columns[1].name.original(), "Name");
    assert_eq!(table.indices.len(), 1);
    assert_eq!(table.indices[0].name.original(), "idx_name");
    assert!(table.pk_is_handle);
    assert_eq!(table.update_ts, 445_566_778_899);
    assert_eq!(table.version, 5);
    // DBID is json:"-"; Go's GetTable/ListTables set it after decoding.
    assert_eq!(table.db_id, 3);
}

/// `json.Marshal` of a `model.TableInfo` with every nested struct populated:
/// partition, view, sequence, lock, TiFlash replica, constraints, foreign keys,
/// TTL, placement, exchange-partition, softdelete, affinity, split policy,
/// storage class and table mode.
const GO_TABLEINFO_FULL: &str = r#"{"id":77,"name":{"O":"T","L":"t"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[{"id":1,"name":{"O":"Id","L":"id"},"offset":0,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":8,"Flag":3,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2},{"id":2,"name":{"O":"Name","L":"name"},"offset":1,"origin_default":null,"origin_default_bit":null,"default":null,"default_bit":null,"default_is_expr":false,"generated_expr_string":"","generated_stored":false,"dependences":null,"type":{"Tp":15,"Flag":0,"Flen":64,"Decimal":-1,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"ElemsIsBinaryLit":null,"Array":false},"state":5,"comment":"","hidden":false,"change_state_info":null,"version":2}],"index_info":[{"id":1,"idx_name":{"O":"idx_name","L":"idx_name"},"tbl_name":{"O":"t","L":"t"},"idx_cols":[{"name":{"O":"Name","L":"name"},"offset":1,"length":-1}],"state":5,"backfill_state":0,"comment":"","index_type":0,"is_unique":true,"is_primary":false,"is_invisible":false,"is_global":false,"mv_index":false,"vector_index":null,"inverted_index":null,"full_text_index":null,"condition_expr_string":""}],"constraint_info":[{"id":1,"constraint_name":{"O":"c1","L":"c1"},"tbl_name":{"O":"t","L":"t"},"constraint_cols":[{"O":"Id","L":"id"}],"enforced":true,"in_column":false,"expr_string":"`id` \u003e 0","state":5}],"fk_info":[{"id":1,"fk_name":{"O":"fk1","L":"fk1"},"ref_schema":{"O":"test","L":"test"},"ref_table":{"O":"o","L":"o"},"ref_cols":[{"O":"Id","L":"id"}],"cols":[{"O":"Id","L":"id"}],"on_delete":1,"on_update":2,"state":5,"version":1}],"state":5,"pk_is_handle":true,"is_common_handle":false,"common_handle_version":0,"comment":"hi","auto_inc_id":0,"auto_inc_id_extra":7,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":2,"max_idx_id":1,"max_fk_id":0,"max_cst_id":0,"update_timestamp":445566778899,"old_schema_id":8,"ShardRowIDBits":4,"max_shard_row_id_bits":4,"auto_random_bits":5,"auto_random_range_bits":64,"pre_split_regions":3,"partition":{"type":1,"expr":"`id`","columns":[{"O":"Id","L":"id"}],"enable":true,"is_empty_columns":false,"definitions":[{"id":78,"name":{"O":"p0","L":"p0"},"less_than":["10"],"in_values":null,"policy_ref_info":null,"comment":"c"},{"id":79,"name":{"O":"p1","L":"p1"},"less_than":null,"in_values":[["1","2"]],"policy_ref_info":null}],"adding_definitions":null,"dropping_definitions":null,"states":[{"id":78,"state":5}],"num":2,"ddl_state":0},"compression":"lz4","view":{"view_algorithm":1,"view_definer":null,"view_security":1,"view_select":"select 1","view_checkoption":1,"view_cols":[{"O":"a","L":"a"}]},"sequence":{"sequence_start":1,"sequence_cache":true,"sequence_cycle":false,"sequence_min_value":1,"sequence_max_value":100,"sequence_increment":1,"sequence_cache_value":1000,"sequence_comment":"s"},"Lock":{"Tp":1,"Sessions":[{"ServerID":"srv","SessionID":9}],"State":2,"TS":1234},"version":5,"tiflash_replica":{"Count":2,"LocationLabels":["zone"],"Available":true,"AvailablePartitionIDs":[78]},"is_columnar":true,"temp_table_type":1,"cache_table_status":1,"policy_ref_info":{"id":5,"name":{"O":"p","L":"p"}},"stats_options":null,"exchange_partition_info":{"exchange_partition_id":90,"exchange_partition_def_id":91,"exchange_partition_flag":false},"ttl_info":{"column":{"O":"Id","L":"id"},"interval_expr":"1","interval_time_unit":3,"enable":true,"job_interval":"1h"},"is_active_active":true,"softdelete_info":{"retention":"1d","job_enable":true,"job_interval":"2h"},"affinity":{"level":"table"},"table_split_policy":{"lower":["1"],"upper":["9"],"regions":4},"revision":11,"engine_attribute":"{\"a\":1}","storage_class_tier":"standard","storage_class_transitions":[{"tier":"cold","after_days":30,"after_seconds":5}],"mode":1}"#;

#[test]
fn fully_populated_tableinfo_round_trips_byte_identically_with_go() {
    let table = value::parse_table_info(GO_TABLEINFO_FULL.as_bytes(), 3).unwrap();
    let reserialized = value::serialize_table_info(&table).unwrap();
    assert_eq!(String::from_utf8(reserialized).unwrap(), GO_TABLEINFO_FULL);

    let partition = table.partition.as_ref().unwrap();
    assert_eq!(partition.definitions.len(), 2);
    assert_eq!(partition.definitions[0].name.original(), "p0");
    assert_eq!(table.view.as_ref().unwrap().select_stmt, "select 1");
    assert_eq!(table.sequence.as_ref().unwrap().cache_value, 1000);
    assert_eq!(table.lock.as_ref().unwrap().ts, 1234);
    assert_eq!(table.tiflash_replica.as_ref().unwrap().count, 2);
    assert_eq!(table.foreign_keys.len(), 1);
    assert_eq!(table.ttl_info.as_ref().unwrap().job_interval, "1h");
    assert_eq!(table.affinity.as_ref().unwrap().level, "table");

    // Go HTML-escapes `>` inside a string; the constraint expression proves the
    // encoder reproduces that rather than emitting the raw byte.
    assert_eq!(table.constraints.len(), 1);
    assert!(GO_TABLEINFO_FULL.contains(r"\u003e"));
}
