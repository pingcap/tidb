// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/ingest/util.go`.
use crate::Error;
/// Go `TryConvertToKeyExistsErr` and `ddlutil.GenKeyExistsErr`.
pub fn try_convert_to_key_exists_err(
    error: Error,
    index: &tidb_model::IndexInfo,
    table: &tidb_model::TableInfo,
) -> Error {
    match error {
        Error::Backend(tidb_ingestor_ingestctrl::Error::Duplicate { key, value }) => {
            gen_key_exists_err(key, value, index, table)
        }
        error => match tidb_ingestor_simplesst::duplicate_key_and_value(&error) {
            Some((key, value)) => gen_key_exists_err(key.to_vec(), value.to_vec(), index, table),
            None => error,
        },
    }
}

/// Go `TryConvertToKeyExistsErr` over any error, as the global-sort steps
/// meet it: an `ErrFoundDuplicateKeys` in `error`'s chain (with its key and
/// value arguments) becomes the index's `ErrKeyExists`; `None` otherwise.
#[must_use]
pub fn key_exists_err_of(
    error: &(dyn std::error::Error + 'static),
    index: &tidb_model::IndexInfo,
    table: &tidb_model::TableInfo,
) -> Option<Error> {
    let (key, value) = tidb_ingestor_simplesst::duplicate_key_and_value(error)?;
    Some(gen_key_exists_err(key.to_vec(), value.to_vec(), index, table))
}

/// Go `ddlutil.GenKeyExistsErr`.
fn gen_key_exists_err(
    mut key: Vec<u8>,
    value: Vec<u8>,
    index: &tidb_model::IndexInfo,
    table: &tidb_model::TableInfo,
) -> Error {
    if key.len() > 4
        && key[0] == b'x'
        && !tidb_config::config_tree::config::get_global_keyspace_name().is_empty()
    {
        key.drain(..4);
    }
    let codec_index = |index: &tidb_model::IndexInfo| tidb_tablecodec::IndexInfo {
        id: index.id,
        columns: index
            .columns
            .iter_deref()
            .map(|c| {
                let c = c.read();
                tidb_tablecodec::IndexColumn {
                    offset: c.offset as usize,
                    length: c.length as i64,
                    use_changing_type: c.use_changing_type,
                }
            })
            .collect(),
        unique: index.unique,
        global: index.global,
        global_index_version: index.global_index_version,
        primary: index.primary,
    };
    let codec_table = tidb_tablecodec::TableInfo {
        columns: table
            .columns
            .iter_deref()
            .map(|c| {
                let c = c.read();
                tidb_tablecodec::TableColumn {
                    id: c.id,
                    offset: c.offset as usize,
                    field_type: c.field_type.clone(),
                    primary_key: c
                        .field_type
                        .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY),
                    changing_field_type: c.changing_field_type.as_ref().map(|f| f.read().clone()),
                }
            })
            .collect(),
        indices: table
            .indices
            .iter_deref()
            .map(|i| codec_index(&i.read()))
            .collect(),
        pk_is_handle: table.pk_is_handle,
        is_common_handle: table.is_common_handle,
        common_handle_version: table.common_handle_version as u8,
    };
    let text = codec_table
        .generate_index_values_from_index(
            tidb_datatype::new_collation_enabled(),
            Some(&tidb_datatype::SessionTimeZone::utc()),
            &codec_index(index),
            &key,
            &value,
        )
        .map(|values| values.join(&b'-'))
        .unwrap_or(key);
    let index_name = format!("{}.{}", table.name.original(), index.name.original());
    Error::Database(tidb_kv::kv_errors::ERR_KEY_EXISTS.fast_gen_by_args(&[
        String::from_utf8_lossy(&text).as_ref().into(),
        index_name.as_str().into(),
    ]))
}
