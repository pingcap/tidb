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
    let columns: Option<Vec<_>> = index
        .columns
        .iter_deref()
        .map(|index_column| {
            let index_column = index_column.read();
            let offset = index_column.offset as usize;
            if offset >= table.columns.len() {
                return None;
            }
            let column = table.columns.get(offset).expect("nil pointer in Go slice");
            let column = column.read();
            let field_type = tidb_model::get_idx_changing_field_type(&index_column, &column);
            Some(tidb_rowcodec::ColumnInfo {
                id: column.id,
                is_pk_handle: table.pk_is_handle
                    && column.field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY),
                virtual_generated: false,
                field_type: field_type.clone(),
            })
        })
        .collect();
    let text = columns
        .and_then(|columns| {
            tidb_tablecodec::render_index_values_from_columns(
                tidb_datatype::new_collation_enabled(),
                Some(&tidb_datatype::SessionTimeZone::utc()),
                &columns,
                &key,
                &value,
            )
            .ok()
        })
        .map(|mut values| {
            if values.len() == 1 {
                values.pop().expect("one index value")
            } else {
                values.join(&b'-')
            }
        })
        .unwrap_or(key);
    let index_name = format!("{}.{}", table.name.original(), index.name.original());
    Error::Database(tidb_kv::kv_errors::ERR_KEY_EXISTS.fast_gen_by_args(&[
        String::from_utf8_lossy(&text).into_owned().into(),
        index_name.into(),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_error_falls_back_to_key_when_index_offset_is_invalid() {
        let table = tidb_model::TableInfo::default();
        let index = tidb_model::IndexInfo {
            columns: vec![tidb_model::IndexColumn {
                offset: 1,
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        };
        let error = gen_key_exists_err(b"raw-key".to_vec(), Vec::new(), &index, &table);
        assert_eq!(
            error.to_string(),
            "[kv:1062]Duplicate entry 'raw-key' for key '.'",
        );
    }
}
