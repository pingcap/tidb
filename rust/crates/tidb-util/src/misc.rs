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

//! Go `pkg/util/misc.go`: the statement-level helpers of the root `util`
//! package.

use tidb_datatype::DateModes;

/// Go `GetTypeFlagsForInsert` / `ResetUpdateStmtCtx`, in the two bits that
/// decide whether a zero-in or invalid date PARSES at all.
///
/// The `IgnoreZeroInDate` expression is Go's verbatim, and its shape is the
/// point: the zero-in-date is let through unless `NO_ZERO_IN_DATE` **and**
/// `NO_ZERO_DATE` **and** strict mode are all present and
/// `ALLOW_INVALID_DATES` is absent. Any one of those missing and
/// `'2024-00-01'` reaches Go `handleZeroDatetime` as a real value instead of
/// a conversion failure -- which is what makes `NO_ZERO_IN_DATE` alone a
/// warning rather than an error.
#[must_use]
pub fn get_type_flags_for_insert(
    base: tidb_datatype::ConversionFlags,
    modes: DateModes,
    strict: bool,
) -> tidb_datatype::ConversionFlags {
    base.with_ignore_invalid_date_err(modes.allow_invalid_dates)
        .with_ignore_zero_in_date_err(
            !modes.no_zero_in_date || !modes.no_zero_date || !strict || modes.allow_invalid_dates,
        )
}

/// Go `SyntaxErrorPrefix`: the common prefix for SQL syntax errors in TiDB.
pub const SYNTAX_ERROR_PREFIX: &str = "You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use";

/// Go `SyntaxError` for a parser failure that is not already a
/// `*terror.Error`: `parser.ErrParse.GenWithStackByArgs(SyntaxErrorPrefix,
/// err.Error())`.
pub fn syntax_error(message: impl Into<String>) -> tidb_terror::TerrorError {
    tidb_parser::parser_errors::ERR_PARSE
        .gen_with_stack_by_args(&[SYNTAX_ERROR_PREFIX.into(), message.into().into()])
}

/// Go `util.SequenceTable`: the sequence operations a table object of a
/// `CREATE SEQUENCE` provides, so expression evaluation can drive one without
/// depending on the table layer. `ctx` is Go's session argument; the
/// database and sequence names are what its errors name.
pub trait SequenceTable: std::fmt::Debug + Send + Sync {
    /// Go `GetSequenceID`.
    fn get_sequence_id(&self) -> i64;

    /// Go `GetSequenceNextVal`.
    fn get_sequence_next_val(
        &self,
        ctx: &dyn std::any::Any,
        db_name: &str,
        seq_name: &str,
    ) -> Result<i64, tidb_terror::TerrorError>;

    /// Go `SetSequenceVal`: the value set, and whether the counter was
    /// already at or past `new_val` (Go's `alreadySatisfied`, nothing set).
    fn set_sequence_val(
        &self,
        ctx: &dyn std::any::Any,
        new_val: i64,
        db_name: &str,
        seq_name: &str,
    ) -> Result<(i64, bool), tidb_terror::TerrorError>;
}

/// Go `util.GetSequenceByName` over a statement's infoschema: the
/// sequences its `NEXTVAL` / `LASTVAL` / `SETVAL` resolve by name.
pub trait SequenceSchema: std::fmt::Debug + Send + Sync {
    /// The session's current database, lowercase: the schema of a bare name.
    fn current_db(&self) -> &str;

    /// Go `infoschema.GetSequenceByName`: the sequence `schema.sequence`
    /// names, or the infoschema's error for a missing object or an object
    /// that is not a sequence.
    fn get_sequence_by_name(
        &self,
        schema: &str,
        sequence: &str,
    ) -> Result<&dyn SequenceTable, tidb_terror::TerrorError>;

    /// The `(database, name)` a one- or two-part sequence path names.
    fn schema_and_name<'a>(&'a self, path: &'a [String]) -> Option<(&'a str, &'a str)> {
        match path {
            [name] => Some((self.current_db(), name.as_str())),
            [database, name] => Some((database.as_str(), name.as_str())),
            _ => None,
        }
    }

    /// The lowercase `(database, name)` the lookup and its errors name for
    /// `path`.
    fn key(&self, path: &[String]) -> (String, String) {
        use tidb_hack::GoToLower;
        match path {
            [name] => (self.current_db().to_string(), name.go_to_lower()),
            [database, name] => (database.go_to_lower(), name.go_to_lower()),
            [] => (String::new(), String::new()),
            [leading @ .., name] => (
                leading
                    .iter()
                    .map(|part| part.go_to_lower())
                    .collect::<Vec<_>>()
                    .join("."),
                name.go_to_lower(),
            ),
        }
    }
}

/// The sequences of a context with no infoschema: none, so every name is
/// Go's `infoschema.ErrTableNotExists` (`ClassSchema.NewStd(ErrNoSuchTable)`).
#[derive(Debug)]
pub struct NoSequences;

impl SequenceSchema for NoSequences {
    fn current_db(&self) -> &str {
        ""
    }

    fn get_sequence_by_name(
        &self,
        schema: &str,
        sequence: &str,
    ) -> Result<&dyn SequenceTable, tidb_terror::TerrorError> {
        Err(tidb_dbterror::catalog_support::std_err(
            tidb_dbterror::catalog_support::Class::Schema,
            tidb_errno::errcode::ErrNoSuchTable,
        )
        .gen_with_stack_by_args(&[schema.into(), sequence.into()]))
    }
}

/// Go `GetLocalIP`: the first global-unicast address among the host's
/// interface addresses (`net.InterfaceAddrs`), or `""` when there is none.
#[must_use]
pub fn get_local_ip() -> String {
    let Ok(addresses) = nix::ifaddrs::getifaddrs() else {
        return String::new();
    };
    for interface in addresses {
        let Some(address) = interface.address.as_ref() else {
            continue;
        };
        let ip = if let Some(v4) = address.as_sockaddr_in() {
            std::net::IpAddr::V4(v4.ip())
        } else if let Some(v6) = address.as_sockaddr_in6() {
            std::net::IpAddr::V6(v6.ip())
        } else {
            continue;
        };
        if is_global_unicast(ip) {
            return ip.to_string();
        }
    }
    String::new()
}

/// Go `net.IP.IsGlobalUnicast`: neither the IPv4 limited broadcast address,
/// unspecified, loopback, multicast nor link-local unicast. Private ranges
/// are global unicast.
#[must_use]
pub fn is_global_unicast(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            v4 != std::net::Ipv4Addr::BROADCAST
                && !v4.is_unspecified()
                && !v4.is_loopback()
                && !v4.is_multicast()
                && !v4.is_link_local()
        }
        std::net::IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => is_global_unicast(std::net::IpAddr::V4(v4)),
            None => {
                !v6.is_unspecified()
                    && !v6.is_loopback()
                    && !v6.is_multicast()
                    && (v6.segments()[0] & 0xffc0) != 0xfe80
            }
        },
    }
}

#[cfg(test)]
mod local_ip_tests {
    use super::is_global_unicast;

    /// Go `net`'s `TestIPAttrs` rows for `IsGlobalUnicast`.
    #[test]
    fn global_unicast_follows_go_net() {
        for (ip, global) in [
            ("0.0.0.0", false),
            ("255.255.255.255", false),
            ("127.0.0.1", false),
            ("10.0.0.1", true),
            ("192.168.1.219", true),
            ("169.254.0.1", false),
            ("224.0.0.1", false),
            ("8.8.8.8", true),
            ("::", false),
            ("::1", false),
            ("fe80::1", false),
            ("ff02::1", false),
            ("2001:db8::1", true),
            ("::ffff:192.168.1.1", true),
            ("::ffff:127.0.0.1", false),
        ] {
            assert_eq!(is_global_unicast(ip.parse().unwrap()), global, "{ip}");
        }
    }
}

/// Go `ColumnsToProto`: the coprocessor descriptors of `columns`, a column
/// marked `PkHandle` when it is the int handle (`pk_is_handle` and a
/// primary-key column) or `_tidb_rowid`.
#[must_use]
pub fn columns_to_proto(
    columns: &[tidb_model::GoShared<tidb_model::ColumnInfo>],
    pk_is_handle: bool,
    for_index: bool,
    is_tiflash_store: bool,
) -> Vec<tidb_proto::tipb::ColumnInfo> {
    columns
        .iter()
        .map(|c| {
            let c = c.read();
            let mut col = column_to_proto(&c, for_index, is_tiflash_store);
            // TODO: Here `PkHandle`'s meaning is changed, we will change it to `IsHandle` when tikv's old select logic
            // is abandoned.
            col.pk_handle = Some(
                (pk_is_handle && tidb_mysql::types::has_pri_key_flag(c.field_type.flags() as usize))
                    || c.id == tidb_model::column::EXTRA_HANDLE_ID,
            );
            col
        })
        .collect()
}

/// Go `ColumnToProto`: converts `model.ColumnInfo` to `tipb.ColumnInfo`.
/// Go reads the collation id from `mysql.CollationNames`; a column's
/// collation is always one TiDB supports, which the collation registry
/// answers alike.
#[must_use]
pub fn column_to_proto(c: &tidb_model::ColumnInfo, for_index: bool, is_tiflash_store: bool) -> tidb_proto::tipb::ColumnInfo {
    let mut pc = tidb_proto::tipb::ColumnInfo {
        column_id: Some(c.id),
        collation: Some(tidb_datatype::collation_to_proto(c.field_type.collation_name())),
        column_len: Some(c.field_type.flen() as i32),
        decimal: Some(c.field_type.decimal() as i32),
        flag: Some(c.field_type.flags() as i32),
        elems: c.field_type.elems_snapshot().into_iter().map(|elem| elem.to_string()).collect(),
        ..tidb_proto::tipb::ColumnInfo::default()
    };
    if is_tiflash_store && c.is_virtual_generated() {
        pc.flag = Some(pc.flag.unwrap_or_default() | tidb_mysql::types::GeneratedColumnFlag as i32);
    }
    if for_index {
        // Use array type for read the multi-valued index.
        pc.tp = Some(i32::from(c.field_type.array_type().code().mysql_type()));
        if c.field_type.is_array() {
            // Use "binary" collation for read the multi-valued index. Most of the time, the `Collation` of this hidden
            // column should already been set to "binary". However, in old versions, the collation is set to the default
            // value. See https://github.com/pingcap/tidb/issues/46717
            pc.collation = Some(tidb_datatype::collation_name_to_id("binary"));
        }
    } else {
        pc.tp = Some(i32::from(c.field_type.code().mysql_type()));
    }
    pc
}

#[cfg(test)]
mod column_to_proto_tests {
    use tidb_datatype::{set_new_collation_enabled_for_test, FieldType, FieldTypeBuilder, FieldTypeCode};
    use tidb_model::{ColumnInfo, GoShared};
    use tidb_mysql::types::GeneratedColumnFlag;
    use tidb_proto::tipb;

    use super::{column_to_proto, columns_to_proto};

    fn field_type(tp: u8) -> FieldType {
        FieldType::new(FieldTypeCode::from_mysql_type(tp))
    }

    fn column(field_type: FieldType) -> ColumnInfo {
        ColumnInfo { field_type, ..ColumnInfo::default() }
    }

    fn pb(tp: i32, collation: i32, column_len: i32, flag: i32) -> tipb::ColumnInfo {
        tipb::ColumnInfo {
            column_id: Some(0),
            tp: Some(tp),
            collation: Some(collation),
            column_len: Some(column_len),
            decimal: Some(0),
            flag: Some(flag),
            ..tipb::ColumnInfo::default()
        }
    }

    /// Go `TestColumnToProto` (`pkg/planner/core/plan_to_pb_test.go`).
    #[test]
    fn test_column_to_proto() {
        // Make sure the flag is set in tipb.ColumnInfo
        set_new_collation_enabled_for_test(false);
        let mut tp = field_type(3);
        tp.set_flag(10);
        tp.set_collation_name("utf8_bin");
        let col = column(tp);
        assert_eq!(column_to_proto(&col, false, false), pb(3, 83, 11, 10));

        let cols = vec![GoShared::new(col.clone()), GoShared::new(col.clone())];
        for v in columns_to_proto(&cols, false, false, false) {
            assert_eq!(v.flag, Some(10));
        }
        for v in columns_to_proto(&cols, true, false, false) {
            assert_eq!(v.flag, Some(10));
        }

        // Make sure the collation ID is successfully set.
        let mut tp = field_type(15);
        tp.set_flag(10);
        tp.set_collation_name("latin1_swedish_ci");
        let col1 = column(tp);
        assert_eq!(column_to_proto(&col1, false, false).collation, Some(8));

        set_new_collation_enabled_for_test(true);

        assert_eq!(column_to_proto(&col, false, false), pb(3, -83, 11, 10));
        for v in columns_to_proto(&cols, true, false, false) {
            assert_eq!(v.collation, Some(-83));
        }
        assert_eq!(column_to_proto(&col1, false, false).collation, Some(-8));

        let mut tp = field_type(0xf7);
        tp.set_flag(10);
        tp.set_elems(vec!["a".into(), "b".into()]);
        let col2 = column(tp);
        assert_eq!(column_to_proto(&col2, false, false).elems.len(), 2);

        let tp = FieldTypeBuilder::new()
            .with_code(FieldTypeCode::from_mysql_type(0xfe))
            .charset_set("utf8mb4")
            .collation_set("utf8mb4_bin")
            .flen_set(100)
            .flags_set(10)
            .array(true)
            .build();
        let col3 = column(tp);
        assert_eq!(column_to_proto(&col3, true, false), pb(0xfe, 63, 100, 10));
    }

    /// Go `TestGeneratedColumnFlagForTiFlash`'s `ColumnToProto` /
    /// `ColumnsToProto` half (issue 59831).
    #[test]
    fn test_generated_column_flag_for_tiflash() {
        set_new_collation_enabled_for_test(false);
        let mut tp = field_type(15);
        tp.set_collation_name("utf8_bin");

        let gen_col = ColumnInfo {
            id: 1,
            generated_expr_string: "lower(c1)".to_owned(),
            generated_stored: false,
            ..column(tp.clone())
        };
        assert!(gen_col.is_virtual_generated());
        let normal_col = ColumnInfo { id: 2, ..column(tp) };

        let gen_flag = GeneratedColumnFlag as i32;

        // When isTiFlashStore=true, GeneratedColumnFlag should be set on virtual generated columns.
        assert_ne!(column_to_proto(&gen_col, false, true).flag.unwrap() & gen_flag, 0);
        assert_eq!(column_to_proto(&gen_col, false, false).flag.unwrap() & gen_flag, 0);
        assert_eq!(column_to_proto(&normal_col, false, true).flag.unwrap() & gen_flag, 0);

        // Test via ColumnsToProto with isTiFlashStore=true.
        let pcs = columns_to_proto(&[GoShared::new(gen_col), GoShared::new(normal_col)], false, false, true);
        assert_ne!(pcs[0].flag.unwrap() & gen_flag, 0);
        assert_eq!(pcs[1].flag.unwrap() & gen_flag, 0);
    }

    /// Go `TestToPB` (`pkg/util/misc_test.go`): `column_id:1 collation:-45
    /// columnLen:-1 decimal:-1`.
    #[test]
    fn test_to_pb() {
        set_new_collation_enabled_for_test(true);
        let mut column = ColumnInfo { id: 1, hidden: true, ..column(field_type(0)) };
        column.field_type.set_collation_name("utf8mb4_general_ci");
        let mut column2 = column.clone();
        column2.field_type.set_collation_name("utf8mb4_bin");

        let expect = tipb::ColumnInfo {
            column_id: Some(1),
            tp: Some(0),
            collation: Some(-45),
            column_len: Some(-1),
            decimal: Some(-1),
            flag: Some(0),
            ..tipb::ColumnInfo::default()
        };
        assert_eq!(column_to_proto(&column, false, false), expect);
        let expect = tipb::ColumnInfo { pk_handle: Some(false), ..expect };
        assert_eq!(columns_to_proto(&[GoShared::new(column), GoShared::new(column2)], false, false, false)[0], expect);
    }
}
