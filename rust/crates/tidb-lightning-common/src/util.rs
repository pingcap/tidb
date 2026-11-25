//! The part of Go `pkg/lightning/common/util.go` the encoder reads.

use tidb_model::{ColumnInfo, GoShared, TableInfo};

/// Go `EncodeIntRowID`: `codec.EncodeComparableVarint(nil, rowID)`.
#[must_use]
pub fn encode_int_row_id(row_id: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    tidb_codec::encode_comparable_varint(&mut out, row_id);
    out
}

/// Go `TableHasAutoRowID`: the table's rows live under `_tidb_rowid`.
#[must_use]
pub fn table_has_auto_row_id(info: &TableInfo) -> bool {
    !info.pk_is_handle && !info.is_common_handle
}

/// Go `TableHasAutoID`.
#[must_use]
pub fn table_has_auto_id(info: &TableInfo) -> bool {
    table_has_auto_row_id(info)
        || info.get_auto_increment_col_info().is_some()
        || info.contains_auto_random_bits()
}

/// Go `GetAutoRandomColumn`: the integer primary key, or the first column
/// of a clustered primary key, of a table with AUTO_RANDOM bits.
#[must_use]
pub fn get_auto_random_column(info: &TableInfo) -> Option<GoShared<ColumnInfo>> {
    if !info.contains_auto_random_bits() {
        return None;
    }
    if info.pk_is_handle {
        return info.get_pk_col_info();
    }
    if info.is_common_handle {
        let pk = info.indices.iter_deref().find(|index| index.read().primary)?;
        let offset = pk.read().columns.get(0)?.read().offset;
        return usize::try_from(offset).ok().and_then(|offset| info.columns.get(offset));
    }
    None
}

/// Go `DefaultImportantVariables`: the session variables an import keeps,
/// with their values when the session has none.
pub const DEFAULT_IMPORTANT_VARIABLES: &[(&str, &str)] = &[
    ("max_allowed_packet", "67108864"),
    ("div_precision_increment", "4"),
    ("time_zone", "SYSTEM"),
    ("lc_time_names", "en_US"),
    ("default_week_format", "0"),
    ("block_encryption_mode", "aes-128-ecb"),
    ("group_concat_max_len", "1024"),
    ("tidb_backoff_weight", "6"),
];

/// Go `DefaultImportVariablesTiDB`.
pub const DEFAULT_IMPORT_VARIABLES_TIDB: &[(&str, &str)] = &[("tidb_row_format_version", "1")];

/// Go `WriteMySQLIdentifier`: always backquoted, a backquote doubled.
#[inline]
pub fn write_mysql_identifier(out: &mut String, mut identifier: &str) {
    let mut tick = identifier.find('`');
    if identifier.len() > 32 && tick.is_some() {
        write_long_escaped_identifier(out, identifier);
        return;
    }
    // One small reservation covers even consecutive backticks in short names.
    let extra = tick.map_or(0, |index| identifier.len() - index);
    out.reserve(identifier.len() + extra + 2);
    out.push('`');
    while let Some(index) = tick {
        let (head, tail) = identifier.split_at(index + 1);
        out.push_str(head);
        out.push('`');
        identifier = tail;
        tick = identifier.find('`');
    }
    out.push_str(identifier);
    out.push('`');
}

#[inline(never)]
fn write_long_escaped_identifier(out: &mut String, identifier: &str) {
    out.reserve(identifier.len() + 2);
    out.push('`');
    out.push_str(&identifier.replace('`', "``"));
    out.push('`');
}

/// Go `EscapeIdentifier`.
#[must_use]
pub fn escape_identifier(identifier: &str) -> String {
    if identifier.is_empty() {
        return "``".to_owned();
    }
    if identifier.len() == 1 {
        if identifier == "`" {
            return "````".to_owned();
        }
        let mut out = String::with_capacity(3);
        out.push('`');
        out.push_str(identifier);
        out.push('`');
        return out;
    }
    let mut out = String::new();
    write_mysql_identifier(&mut out, identifier);
    out
}

/// Go `UniqueTable`: `` `schema`.`table` ``.
#[must_use]
pub fn unique_table(schema: &str, table: &str) -> String {
    let mut out = String::with_capacity(schema.len() + table.len() + 5);
    write_mysql_identifier(&mut out, schema);
    out.push('.');
    write_mysql_identifier(&mut out, table);
    out
}

/// Go `SkipReadRowCount`: a table whose row IDs come from neither the
/// file's row count nor an allocator need not count Parquet rows.
#[must_use]
pub fn skip_read_row_count(info: &TableInfo) -> bool {
    use tidb_datatype::FieldTypeFlags;
    if table_has_auto_row_id(info) || info.contains_auto_random_bits() {
        return false;
    }
    for idx in info.indices.iter_deref() {
        let idx = idx.read();
        if !idx.unique || !idx.primary {
            continue;
        }
        for col in idx.columns.iter_deref() {
            let offset = usize::try_from(col.read().offset).unwrap_or(0);
            if let Some(info) = info.columns.get(offset) {
                if info.read().field_type.flags() & FieldTypeFlags::AUTO_INCREMENT != 0 {
                    return false;
                }
            }
        }
    }
    for col in info.columns.iter_deref() {
        let flags = col.read().field_type.flags();
        if flags & FieldTypeFlags::PRI_KEY != 0 && flags & FieldTypeFlags::AUTO_INCREMENT != 0 {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::{escape_identifier, unique_table, write_mysql_identifier};

    #[test]
    fn quotes_identifiers_with_multibyte_text_and_consecutive_backticks() {
        assert_eq!(escape_identifier(""), "``");
        assert_eq!(escape_identifier("`"), "````");
        assert_eq!(unique_table("库`", "𠮷``表"), "`库```.`𠮷````表`");

        let mut output = String::from("CREATE TABLE ");
        let name = "é`".repeat(17);
        write_mysql_identifier(&mut output, &name);
        assert_eq!(output, format!("CREATE TABLE `{}`", "é``".repeat(17)));
    }
}
