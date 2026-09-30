//! The `time_zone` a real-TiKV session reads and writes in.
//!
//! One value serves both directions of the round trip: its name and current
//! offset go into the DAG request, and the same DST-aware zone converts write
//! literals. Parsing is delegated to `tidb_util::timeutil::parse_time_zone`,
//! the package owner of Go `timeutil.ParseTimeZone`'s accepted domain.

/// A real-TiKV session's resolved `time_zone`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RealTiKvSessionTimeZone {
    zone: tidb_datatype::SessionTimeZone,
}

impl Default for RealTiKvSessionTimeZone {
    fn default() -> Self {
        Self::from_timeutil(tidb_util::timeutil::system_location())
    }
}

impl RealTiKvSessionTimeZone {
    pub(crate) fn from_zone(zone: tidb_datatype::SessionTimeZone) -> Self {
        Self { zone }
    }
    fn from_timeutil(zone: tidb_util::timeutil::TimeZone) -> Self {
        let zone = match zone {
            tidb_util::timeutil::TimeZone::Local => tidb_datatype::SessionTimeZone::Local,
            tidb_util::timeutil::TimeZone::Named(zone) => {
                tidb_datatype::SessionTimeZone::Named(zone)
            }
            tidb_util::timeutil::TimeZone::Fixed { name, offset_secs } => {
                tidb_datatype::SessionTimeZone::Fixed { name, offset_secs }
            }
        };
        Self { zone }
    }

    /// The shared zone used by reads and writes.
    pub(crate) fn zone(&self) -> tidb_datatype::SessionTimeZone {
        self.zone.clone()
    }

    /// Parses the complete Go `timeutil.ParseTimeZone` domain.
    pub(crate) fn parse(value: &str) -> Result<Self, tidb_error::terror::TerrorError> {
        Ok(Self::from_timeutil(tidb_util::timeutil::parse_time_zone(
            value,
        )?))
    }
}

#[cfg(test)]
pub(crate) fn time_zone_sql_error(
    error: tidb_error::terror::TerrorError,
) -> crate::sql_node::SqlQueryError {
    let error = error.to_sql_error();
    crate::sql_node::SqlQueryError::new(
        error.code,
        error
            .state
            .as_bytes()
            .try_into()
            .expect("five-byte SQLSTATE"),
        error.message,
    )
}

/// Routes supported variable assignments through the normal SQL parser and SET owner.
/// The lightweight table reader has no lifecycle for other session commands.
pub(crate) fn execute_storage_session_set(
    session: &mut tidb_session::Session,
    sql: &str,
) -> Result<bool, crate::sql_node::SqlQueryError> {
    let Ok(tidb_ast::Stmt::Session(statement)) = tidb_parser::parse(sql) else {
        return Ok(false);
    };
    let tidb_ast::SessionStmt::Set(set) = statement.as_ref() else {
        return Ok(false);
    };
    // These variables have storage consumers in the lightweight session.
    // Transaction-control SETs still need its separate lifecycle adapter.
    use tidb_vardef::tidb_vars::{
        TIDB_ENABLE1_PC, TIDB_ENABLE_ASYNC_COMMIT, TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING,
        TIDB_TXN_ASSERTION_LEVEL,
    };
    if !set.assignments.iter().all(|assignment| {
        [
            "time_zone",
            "innodb_lock_wait_timeout",
            TIDB_ENABLE1_PC,
            TIDB_ENABLE_ASYNC_COMMIT,
            TIDB_TXN_ASSERTION_LEVEL,
            TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING,
        ]
        .iter()
        .any(|name| assignment.name.eq_ignore_ascii_case(name))
    }) {
        return Ok(false);
    }
    session.execute_statement(sql).map_err(|error| {
        let error = error.to_mysql_error();
        crate::sql_node::SqlQueryError::new(error.code, error.state, error.message)
    })?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_set_prefix_is_not_a_statement() {
        let mut session = tidb_session::Session::new();
        assert!(!execute_storage_session_set(&mut session, "SETtime_zone='UTC'").unwrap());
        assert!(!execute_storage_session_set(&mut session, "SET SESSIONtime_zone='UTC'").unwrap());
    }

    #[test]
    fn storage_settings_use_normal_set_validation_and_expressions() {
        let mut session = tidb_session::Session::new();
        assert!(execute_storage_session_set(&mut session,
            "SET /* comment */ LOCAL time_zone = CONCAT('+', '05:00'), innodb_lock_wait_timeout = 7"
        ).unwrap());
        assert_eq!(
            session.vars().session_time_zone().dag_zone(),
            (String::new(), 18_000)
        );
        assert_eq!(
            session
                .vars()
                .get_system("innodb_lock_wait_timeout")
                .unwrap(),
            "7"
        );
        let error =
            execute_storage_session_set(&mut session, "SET time_zone='not-a-zone'").unwrap_err();
        assert_eq!(error.code, 1298);
        assert_eq!(session.vars().session_time_zone().dag_zone().1, 18_000);
    }

    /// Go `ConstructDAGReq` stamps `timeutil.Zone(SessionVars.Location())`,
    /// and `Zone` returns `loc.String()` as the NAME. An offset zone is
    /// `time.FixedZone("", ofst)` (`timeutil.ParseTimeZone`'s `+HH:MM`
    /// branch), whose `String()` is EMPTY -- TiKV then falls back to the
    /// offset, which is the only half it can use. Measured against Go on this
    /// branch (`timeutil.Zone(timeutil.ParseTimeZone(s))`, system TZ
    /// `Asia/Shanghai`):
    ///
    /// ```text
    /// 'Asia/Shanghai' -> name "Asia/Shanghai" offset 28800
    /// '+05:00'        -> name ""              offset 18000
    /// '-08:00'        -> name ""              offset -28800
    /// 'UTC'           -> name "UTC"           offset 0
    /// '+00:00'        -> name ""              offset 0
    /// 'SYSTEM'        -> the resolved `timeutil.SystemLocation()` pair
    /// ```
    ///
    /// Note the last two rows: `'+00:00'` is NOT `"UTC"` -- it is an offset
    /// zone that happens to be zero -- and `SYSTEM` sends the RESOLVED system
    /// zone's name rather than the spelling `SYSTEM`.
    #[test]
    fn an_offset_zone_stamps_an_empty_dag_name_and_a_named_one_stamps_its_name() {
        let dag = |value: &str| {
            RealTiKvSessionTimeZone::parse(value)
                .expect("the probe value parses")
                .zone()
                .dag_zone()
        };
        assert_eq!(dag("+05:00"), (String::new(), 18_000));
        assert_eq!(dag("-08:00"), (String::new(), -28_800));
        assert_eq!(dag("+00:00"), (String::new(), 0));
        assert_eq!(dag("UTC"), ("UTC".to_owned(), 0));
        let system = tidb_util::timeutil::system_location();
        assert_eq!(dag("SYSTEM"), tidb_util::timeutil::zone(&system));
    }

    #[test]
    fn real_tikv_session_time_zone_uses_the_source_parser() {
        assert_eq!(
            RealTiKvSessionTimeZone::parse("+05:00")
                .unwrap()
                .zone()
                .dag_zone(),
            (String::new(), 18_000)
        );
        assert_eq!(
            RealTiKvSessionTimeZone::parse("-12:00")
                .unwrap()
                .zone()
                .dag_zone(),
            (String::new(), -43_200)
        );
        assert_eq!(
            RealTiKvSessionTimeZone::parse("UTC")
                .unwrap()
                .zone()
                .dag_zone(),
            ("UTC".to_owned(), 0)
        );
        assert_eq!(
            RealTiKvSessionTimeZone::parse("Asia/Shanghai")
                .unwrap()
                .zone()
                .dag_zone(),
            ("Asia/Shanghai".to_owned(), 28_800)
        );
        assert_eq!(
            RealTiKvSessionTimeZone::parse("not-a-zone")
                .expect_err("an invalid name must be rejected")
                .to_sql_error()
                .code,
            1298
        );
    }

    #[test]
    fn real_tikv_session_time_zone_accepts_the_go_parse_time_zone_domain() {
        let shanghai = RealTiKvSessionTimeZone::parse("Asia/Shanghai")
            .expect("Go timeutil.ParseTimeZone accepts named IANA zones");
        assert_eq!(shanghai.zone().dag_zone().0, "Asia/Shanghai");

        let compact = RealTiKvSessionTimeZone::parse("+2")
            .expect("Go parses offsets with MySQL duration grammar");
        assert_eq!(compact.zone().dag_zone(), (String::new(), 2));

        let error = time_zone_sql_error(
            RealTiKvSessionTimeZone::parse("not-a-zone").expect_err("invalid zone"),
        );
        assert_eq!(error.code, 1298);
        assert_eq!(error.state, *b"HY000");
        assert_eq!(
            error.message,
            "Unknown or incorrect time zone: 'not-a-zone'"
        );
    }

    #[test]
    fn fresh_session_uses_the_resolved_system_location() {
        let default = RealTiKvSessionTimeZone::default().zone().dag_zone();
        let system = tidb_util::timeutil::system_location();
        assert_eq!(default, tidb_util::timeutil::zone(&system));
    }
}
