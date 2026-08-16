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

//! Go `pkg/expression/exprstatic/evalctx.go`: the static evaluation context.
//!
//! See the module header of [`super`] for the package's boundaries and the
//! two structural adaptations (the warning bridge, and `CurrentTime`
//! returning an instant).

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use tidb_datatype::{
    str_to_float, ConversionContext, ConversionFlags, ConversionLocation,
    ConversionWarningAppender, Datum, STRICT_FLAGS,
};
use tidb_error::errctx::{
    new_context_with_levels, Context as ErrCtxContext, LevelMap, SharedError,
    WarnAppender as ErrCtxWarnAppender,
};
use tidb_error::terror::TerrorError;
use tidb_mysql::consts::{format_sql_mode_str, get_sql_mode, DefaultSQLMode, SqlMode};
use tidb_util::context::WarnHandler;
use tidb_util::context::{gen_context_id, SqlWarn, StaticWarnHandler, WarnAppender, WarnErr};
use tidb_util::timeutil::{parse_time_zone, zone_name, TimeZone};
use tidb_vardef::defaults::{
    DEF_DEFAULT_WEEK_FORMAT, DEF_DIV_PRECISION_INCREMENT, DEF_GROUP_CONCAT_MAX_LEN,
    DEF_SYSDATE_IS_NOW, DEF_TIDB_ENABLE_NOOP_FUNCS, DEF_TIDB_REDACT_LOG, DEF_TIMESTAMP,
};
use tidb_vardef::tidb_vars::{
    DIV_PRECISION_INCREMENT, ON, TIDB_ENABLE_NOOP_FUNCS, TIDB_REDACT_LOG, TIDB_SYSDATE_IS_NOW, WARN,
};

use crate::exprctx::{
    OptionalEvalPropKey, OptionalEvalPropKeySet, ERR_PARAM_INDEX_EXCEED_PARAM_COUNTS,
};
use crate::expropt::{DynOptionalEvalPropProvider, EvalPropContext, OptionalEvalPropProviders};
use crate::user_vars::{UserVars, UserVarsReader};

/// boundary: Go `vardef.DefMaxAllowedPacket`, which is
/// `config.DefMaxAllowedPacket` (`64 << 20`). `tidb-vardef` ports only
/// `vardef/tidb_vars.go`, and `tidb-config` — where the ported constant lives
/// as `DEF_MAX_ALLOWED_PACKET` — is not a dependency of this crate.
pub const DEF_MAX_ALLOWED_PACKET: u64 = 64 << 20;

/// boundary: Go `vardef.TimeZone`, from the unported `vardef/sysvar.go`.
pub const TIME_ZONE: &str = "time_zone";
/// boundary: Go `vardef.SQLModeVar`, from the unported `vardef/sysvar.go`.
pub const SQL_MODE_VAR: &str = "sql_mode";
/// boundary: Go `vardef.Timestamp`, from the unported `vardef/sysvar.go`.
pub const TIMESTAMP: &str = "timestamp";
/// boundary: Go `vardef.MaxAllowedPacket`, from the unported `vardef/sysvar.go`.
pub const MAX_ALLOWED_PACKET: &str = "max_allowed_packet";
/// boundary: Go `vardef.DefaultWeekFormat`, from the unported `vardef/sysvar.go`.
pub const DEFAULT_WEEK_FORMAT: &str = "default_week_format";
/// boundary: Go `vardef.CharacterSetConnection`, from the unported `vardef/sysvar.go`.
pub const CHARACTER_SET_CONNECTION: &str = "character_set_connection";
/// boundary: Go `vardef.CollationConnection`, from the unported `vardef/sysvar.go`.
pub const COLLATION_CONNECTION: &str = "collation_connection";
/// boundary: Go `vardef.DefaultCollationForUTF8MB4`, from the unported `vardef/sysvar.go`.
pub const DEFAULT_COLLATION_FOR_UTF8MB4: &str = "default_collation_for_utf8mb4";
/// boundary: Go `vardef.BlockEncryptionMode`, from the unported `vardef/sysvar.go`.
pub const BLOCK_ENCRYPTION_MODE: &str = "block_encryption_mode";
/// boundary: Go `vardef.WindowingUseHighPrecision`, from the unported `vardef/sysvar.go`.
pub const WINDOWING_USE_HIGH_PRECISION: &str = "windowing_use_high_precision";
/// boundary: Go `vardef.GroupConcatMaxLen`, from the unported `vardef/sysvar.go`.
pub const GROUP_CONCAT_MAX_LEN: &str = "group_concat_max_len";

/// boundary: Go `variable.OffInt`.
pub const OFF_INT: i64 = 0;
/// boundary: Go `variable.OnInt`.
pub const ON_INT: i64 = 1;
/// boundary: Go `variable.WarnInt`.
pub const WARN_INT: i64 = 2;

/// boundary: Go `variable.TiDBOptOnOffWarn`. `tidb-session` ports it as
/// `varsutil::tidb_opt_on_off_warn`, but that crate sits above this one.
#[must_use]
pub fn tidb_opt_on_off_warn(opt: &str) -> i64 {
    match opt {
        _ if opt == WARN => WARN_INT,
        _ if opt == ON => ON_INT,
        _ => OFF_INT,
    }
}

/// The error carried by the fallible paths of this package: a current-time
/// function's failure and `LoadSystemVars`'s parse failures.
///
/// Go returns a bare `error` from both; the message is the whole contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvalCtxError(String);

impl EvalCtxError {
    /// Builds an error from its message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        EvalCtxError(message.into())
    }

    /// The error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EvalCtxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for EvalCtxError {}

/// The Go closure `func() (time.Time, error)` that supplies the context's
/// "current time". The instant is UTC; see the [`super`] header.
pub type CurrentTimeFn = Arc<dyn Fn() -> Result<DateTime<Utc>, EvalCtxError> + Send + Sync>;

/// Go `timeOnce`: evaluates the current time at most once, successfully.
///
/// Go pairs an `atomic.Pointer` fast path with a `sync.Mutex` slow path; one
/// mutex reaches the same contract, because a failed call caches nothing and
/// is retried exactly as Go retries it.
struct TimeOnce {
    time: Mutex<Option<DateTime<Utc>>>,
    time_fn: Option<CurrentTimeFn>,
}

impl TimeOnce {
    fn new(time_fn: Option<CurrentTimeFn>) -> Self {
        TimeOnce {
            time: Mutex::new(None),
            time_fn,
        }
    }

    /// Go `timeOnce.getTime`. Go's `tm.In(loc)` is dropped: it rebinds the
    /// location without moving the instant, and the location is the context's.
    fn get_time(&self) -> Result<DateTime<Utc>, EvalCtxError> {
        let mut cached = self
            .time
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(time) = *cached {
            return Ok(time);
        }

        let time = match &self.time_fn {
            Some(time_fn) => time_fn()?,
            None => Utc::now(),
        };
        *cached = Some(time);
        Ok(time)
    }
}

/// The one sink Go reaches by passing the `EvalContext` itself to
/// `types.NewContext` and `errctx.NewContext`: every warning raised through
/// the type context, the error context or the eval context lands in the
/// context's own [`WarnHandler`].
struct WarnBridge(Arc<dyn WarnHandler + Send + Sync>);

impl WarnAppender for WarnBridge {
    fn append_warning(&self, err: WarnErr) {
        self.0.append_warning(err);
    }

    fn append_note(&self, err: WarnErr) {
        self.0.append_note(err);
    }
}

impl ConversionWarningAppender for WarnBridge {
    fn append_conversion_warning(&self, warning: TerrorError) {
        self.0.append_warning(WarnErr::Terror(warning));
    }
}

impl ErrCtxWarnAppender for WarnBridge {
    /// `errctx` hands over an opaque `SharedError`, so a typed terror that
    /// arrived that way is flattened to its message. Go keeps the error value
    /// itself; only the `SQLWarn.Err` identity differs, never the text.
    fn append_warning(&self, err: SharedError) {
        self.0.append_warning(WarnErr::Message(err.to_string()));
    }

    fn append_note(&self, err: SharedError) {
        self.0.append_note(WarnErr::Message(err.to_string()));
    }
}

/// Go `evalCtxState`: the internal state of an [`EvalContext`], kept separate
/// so that an [`EvalCtxOption`] can only run inside a constructor.
///
/// Go's `typeCtx`/`errCtx` fields are replaced by the three values they carry
/// beyond the shared warning sink — flags, location and the error level map —
/// because both contexts are derived on access here.
#[derive(Clone)]
struct EvalCtxState {
    warn_handler: Arc<dyn WarnHandler + Send + Sync>,
    sql_mode: SqlMode,
    flags: ConversionFlags,
    location: TimeZone,
    level_map: LevelMap,
    current_db: String,
    current_time: Arc<TimeOnce>,
    max_allowed_packet: u64,
    enable_redact_log: String,
    default_week_format_mode: String,
    div_precision_increment: i64,
    param_list: Vec<Datum>,
    user_vars: Arc<dyn UserVarsReader + Send + Sync>,
    props: OptionalEvalPropProviders,
}

/// Go `EvalCtxOption`: one option of an [`EvalContext`].
pub struct EvalCtxOption(Box<dyn FnOnce(&mut EvalCtxState)>);

impl EvalCtxOption {
    fn new(f: impl FnOnce(&mut EvalCtxState) + 'static) -> Self {
        EvalCtxOption(Box::new(f))
    }
}

/// Go `WithWarnHandler`. Go's `intest.AssertNotNil(h)` plus its nil fallback
/// to `contextutil.IgnoreWarn` are dropped: `Arc` is non-nullable.
#[must_use]
pub fn with_warn_handler(handler: Arc<dyn WarnHandler + Send + Sync>) -> EvalCtxOption {
    EvalCtxOption::new(move |state| state.warn_handler = handler)
}

/// Go `WithSQLMode`.
#[must_use]
pub fn with_sql_mode(sql_mode: SqlMode) -> EvalCtxOption {
    EvalCtxOption::new(move |state| state.sql_mode = sql_mode)
}

/// Go `WithTypeFlags`, which is `typeCtx.WithFlags(flags)`.
#[must_use]
pub fn with_type_flags(flags: ConversionFlags) -> EvalCtxOption {
    EvalCtxOption::new(move |state| state.flags = flags)
}

/// Go `WithLocation`, which is `typeCtx.WithLocation(loc)`. Go's
/// `intest.AssertNotNil(loc)` and its UTC fallback are dropped as
/// unrepresentable.
#[must_use]
pub fn with_location(location: TimeZone) -> EvalCtxOption {
    EvalCtxOption::new(move |state| state.location = location)
}

/// Go `WithErrLevelMap`, which is `errCtx.WithErrGroupLevels(level)`.
#[must_use]
pub fn with_err_level_map(level_map: LevelMap) -> EvalCtxOption {
    EvalCtxOption::new(move |state| state.level_map = level_map)
}

/// Go `WithCurrentDB`.
#[must_use]
pub fn with_current_db(db: impl Into<String>) -> EvalCtxOption {
    let db = db.into();
    EvalCtxOption::new(move |state| state.current_db = db)
}

/// Go `WithCurrentTime`.
#[must_use]
pub fn with_current_time(time_fn: CurrentTimeFn) -> EvalCtxOption {
    EvalCtxOption::new(move |state| state.current_time = Arc::new(TimeOnce::new(Some(time_fn))))
}

/// Go `WithMaxAllowedPacket`: the value of `max_allowed_packet`.
#[must_use]
pub fn with_max_allowed_packet(size: u64) -> EvalCtxOption {
    EvalCtxOption::new(move |state| state.max_allowed_packet = size)
}

/// Go `WithDefaultWeekFormatMode`: the value of `default_week_format`.
#[must_use]
pub fn with_default_week_format_mode(mode: impl Into<String>) -> EvalCtxOption {
    let mode = mode.into();
    EvalCtxOption::new(move |state| state.default_week_format_mode = mode)
}

/// Go `WithDivPrecisionIncrement`: the value of `div_precision_increment`.
#[must_use]
pub fn with_div_precision_increment(increment: i64) -> EvalCtxOption {
    EvalCtxOption::new(move |state| state.div_precision_increment = increment)
}

/// Go `WithOptionalProperty`: replaces *all* optional property providers.
#[must_use]
pub fn with_optional_property(
    providers: Vec<Arc<dyn DynOptionalEvalPropProvider>>,
) -> EvalCtxOption {
    EvalCtxOption::new(move |state| {
        let mut props = OptionalEvalPropProviders::new();
        for provider in providers {
            props.add(provider);
        }
        state.props = props;
    })
}

/// Go `WithParamList`.
///
/// boundary: Go takes a `*variable.PlanCacheParamList` and copies
/// `AllParamValues()` out of it; the copy is what the context keeps, so the
/// slice is the whole input. See the [`super`] header.
#[must_use]
pub fn with_param_list(params: &[Datum]) -> EvalCtxOption {
    let params = params.to_vec();
    EvalCtxOption::new(move |state| state.param_list = params)
}

/// Go `WithEnableRedactLog`: the value of `tidb_redact_log`.
#[must_use]
pub fn with_enable_redact_log(enable_redact_log: impl Into<String>) -> EvalCtxOption {
    let enable_redact_log = enable_redact_log.into();
    EvalCtxOption::new(move |state| state.enable_redact_log = enable_redact_log)
}

/// Go `WithUserVarsReader`.
#[must_use]
pub fn with_user_vars_reader(vars: Arc<dyn UserVarsReader + Send + Sync>) -> EvalCtxOption {
    EvalCtxOption::new(move |state| state.user_vars = vars)
}

/// Go's package-level `defaultSQLMode`, `mysql.GetSQLMode(mysql.DefaultSQLMode)`.
/// Go panics on a parse failure at init; the expectation below is the same
/// contract at first use.
#[must_use]
pub fn default_sql_mode() -> SqlMode {
    get_sql_mode(DefaultSQLMode).expect("mysql.DefaultSQLMode always parses")
}

/// Go `EvalContext`: a static context for expression evaluation. "Static"
/// means, as upstream puts it, that its internal state does not rely on the
/// session and stays immutable for most fields.
pub struct EvalContext {
    id: u64,
    state: EvalCtxState,
    /// The shared sink of [`EvalContext::type_ctx`] and
    /// [`EvalContext::err_ctx`], rebuilt whenever the handler may have
    /// changed. Go instead passes the context itself to both.
    bridge: Arc<WarnBridge>,
}

impl fmt::Debug for EvalContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EvalContext")
            .field("id", &self.id)
            .field("sql_mode", &self.state.sql_mode)
            .field("location", &self.state.location)
            .field("current_db", &self.state.current_db)
            .finish_non_exhaustive()
    }
}

impl EvalContext {
    /// Go `NewEvalContext`.
    ///
    /// Go fills the warning handler and the user variables only if no option
    /// supplied them; installing the same defaults up front and letting an
    /// option overwrite them is observably identical.
    #[must_use]
    pub fn new(opts: impl IntoIterator<Item = EvalCtxOption>) -> EvalContext {
        let mut state = EvalCtxState {
            warn_handler: Arc::new(StaticWarnHandler::new(0)),
            sql_mode: default_sql_mode(),
            flags: STRICT_FLAGS,
            location: TimeZone::Named(chrono_tz::Tz::UTC),
            level_map: LevelMap::strict(),
            current_db: String::new(),
            current_time: Arc::new(TimeOnce::new(None)),
            max_allowed_packet: DEF_MAX_ALLOWED_PACKET,
            enable_redact_log: DEF_TIDB_REDACT_LOG.to_owned(),
            default_week_format_mode: DEF_DEFAULT_WEEK_FORMAT.to_owned(),
            div_precision_increment: DEF_DIV_PRECISION_INCREMENT,
            param_list: Vec::new(),
            user_vars: Arc::new(UserVars::new()),
            props: OptionalEvalPropProviders::new(),
        };

        for opt in opts {
            (opt.0)(&mut state);
        }

        EvalContext::seal(gen_context_id(), state)
    }

    fn seal(id: u64, state: EvalCtxState) -> EvalContext {
        let bridge = Arc::new(WarnBridge(Arc::clone(&state.warn_handler)));
        EvalContext { id, state, bridge }
    }

    /// Go `CtxID`.
    #[must_use]
    pub fn ctx_id(&self) -> u64 {
        self.id
    }

    /// Go `SQLMode`.
    #[must_use]
    pub fn sql_mode(&self) -> SqlMode {
        self.state.sql_mode
    }

    /// Go `TypeCtx`.
    #[must_use]
    pub fn type_ctx(&self) -> ConversionContext<'_> {
        ConversionContext::new(
            self.state.flags,
            ConversionLocation::named(zone_name(&self.state.location)),
            &*self.bridge,
        )
    }

    /// The flags of [`EvalContext::type_ctx`], Go `TypeCtx().Flags()`.
    #[must_use]
    pub fn type_flags(&self) -> ConversionFlags {
        self.state.flags
    }

    /// Go `ErrCtx`.
    #[must_use]
    pub fn err_ctx(&self) -> ErrCtxContext {
        new_context_with_levels(self.state.level_map, Arc::clone(&self.bridge) as _)
    }

    /// The level map of [`EvalContext::err_ctx`], Go `ErrCtx().LevelMap()`.
    #[must_use]
    pub fn err_level_map(&self) -> LevelMap {
        self.state.level_map
    }

    /// Go `Location`.
    #[must_use]
    pub fn location(&self) -> &TimeZone {
        &self.state.location
    }

    /// Go `AppendWarning`.
    pub fn append_warning(&self, err: WarnErr) {
        self.state.warn_handler.append_warning(err);
    }

    /// Go `AppendNote`.
    pub fn append_note(&self, err: WarnErr) {
        self.state.warn_handler.append_note(err);
    }

    /// Go `WarningCount`.
    #[must_use]
    pub fn warning_count(&self) -> usize {
        self.state.warn_handler.warning_count()
    }

    /// Go `TruncateWarnings`.
    #[must_use]
    pub fn truncate_warnings(&self, start: usize) -> Vec<SqlWarn> {
        self.state.warn_handler.truncate_warnings(start)
    }

    /// Go `CopyWarnings`. Go appends into the caller's slice; ownership makes
    /// the returned vector the same result without the aliasing contract.
    #[must_use]
    pub fn copy_warnings(&self) -> Vec<SqlWarn> {
        self.state.warn_handler.copy_warnings()
    }

    /// Go `CurrentDB`.
    #[must_use]
    pub fn current_db(&self) -> &str {
        &self.state.current_db
    }

    /// Go `CurrentTime`, as the instant only; see the [`super`] header.
    pub fn current_time(&self) -> Result<DateTime<Utc>, EvalCtxError> {
        self.state.current_time.get_time()
    }

    /// Go `GetMaxAllowedPacket`.
    #[must_use]
    pub fn get_max_allowed_packet(&self) -> u64 {
        self.state.max_allowed_packet
    }

    /// Go `GetTiDBRedactLog`.
    #[must_use]
    pub fn get_tidb_redact_log(&self) -> &str {
        &self.state.enable_redact_log
    }

    /// Go `GetDefaultWeekFormatMode`.
    #[must_use]
    pub fn get_default_week_format_mode(&self) -> &str {
        &self.state.default_week_format_mode
    }

    /// Go `GetDivPrecisionIncrement`.
    #[must_use]
    pub fn get_div_precision_increment(&self) -> i64 {
        self.state.div_precision_increment
    }

    /// Go `GetUserVarsReader`.
    #[must_use]
    pub fn get_user_vars_reader(&self) -> &Arc<dyn UserVarsReader + Send + Sync> {
        &self.state.user_vars
    }

    /// Go `GetOptionalPropSet`.
    #[must_use]
    pub fn get_optional_prop_set(&self) -> OptionalEvalPropKeySet {
        self.state.props.prop_key_set()
    }

    /// Go `GetOptionalPropProvider`.
    #[must_use]
    pub fn get_optional_prop_provider(
        &self,
        key: OptionalEvalPropKey,
    ) -> Option<Arc<dyn DynOptionalEvalPropProvider>> {
        self.state.props.get(key)
    }

    /// Go `Apply`: a new context with the options applied on top of this one.
    #[must_use]
    pub fn apply(&self, opts: impl IntoIterator<Item = EvalCtxOption>) -> EvalContext {
        let mut state = self.state.clone();

        // Go: "current time should use the previous one by default".
        let previous = Arc::clone(&self.state.current_time);
        state.current_time = Arc::new(TimeOnce::new(Some(Arc::new(move || previous.get_time()))));

        for opt in opts {
            (opt.0)(&mut state);
        }

        EvalContext::seal(gen_context_id(), state)
    }

    /// Go `GetParamValue`.
    pub fn get_param_value(&self, idx: usize) -> Result<Datum, EvalCtxError> {
        self.state
            .param_list
            .get(idx)
            .cloned()
            .ok_or_else(|| EvalCtxError::new(ERR_PARAM_INDEX_EXCEED_PARAM_COUNTS))
    }

    /// Go `AllParamValues`, implementing `StaticConvertibleEvalContext`.
    #[must_use]
    pub fn all_param_values(&self) -> &[Datum] {
        &self.state.param_list
    }

    /// Go `GetWarnHandler`, implementing `StaticConvertibleEvalContext`.
    #[must_use]
    pub fn get_warn_handler(&self) -> &Arc<dyn WarnHandler + Send + Sync> {
        &self.state.warn_handler
    }

    /// Go `LoadSystemVars`.
    pub fn load_system_vars(
        &self,
        sys_vars: &HashMap<String, String>,
    ) -> Result<EvalContext, EvalCtxError> {
        let session_vars = new_session_vars_with_system_variables(sys_vars)?;
        Ok(self.load_session_vars_internal(&session_vars, sys_vars))
    }

    pub(super) fn load_session_vars_internal(
        &self,
        session_vars: &dyn SessionVarsSnapshot,
        sys_vars: &HashMap<String, String>,
    ) -> EvalContext {
        let mut opts: Vec<EvalCtxOption> = Vec::with_capacity(8);
        for (name, val) in sys_vars {
            match name.to_lowercase().as_str() {
                TIME_ZONE => opts.push(with_location(session_vars.location())),
                SQL_MODE_VAR => opts.push(with_sql_mode(session_vars.sql_mode())),
                TIMESTAMP => opts.push(with_current_time(current_time_fn_from_string_val(val))),
                MAX_ALLOWED_PACKET => {
                    opts.push(with_max_allowed_packet(session_vars.max_allowed_packet()));
                }
                TIDB_REDACT_LOG => {
                    opts.push(with_enable_redact_log(session_vars.enable_redact_log()));
                }
                DEFAULT_WEEK_FORMAT => opts.push(with_default_week_format_mode(val.clone())),
                DIV_PRECISION_INCREMENT => opts.push(with_div_precision_increment(
                    session_vars.div_precision_increment(),
                )),
                _ => {}
            }
        }
        self.apply(opts)
    }
}

impl EvalPropContext for EvalContext {
    fn get_optional_prop_provider(
        &self,
        key: OptionalEvalPropKey,
    ) -> Option<Arc<dyn DynOptionalEvalPropProvider>> {
        EvalContext::get_optional_prop_provider(self, key)
    }
}

/// Go `EvalContext.currentTimeFuncFromStringVal`: reads `@@timestamp`.
fn current_time_fn_from_string_val(val: &str) -> CurrentTimeFn {
    let val = val.to_owned();
    Arc::new(move || {
        if val == DEF_TIMESTAMP {
            return Ok(Utc::now());
        }

        // Go uses `types.StrictContext`, under which a truncation is an error.
        let converted = str_to_float(&val, false);
        if converted.event.is_some() {
            return Err(EvalCtxError::new(format!(
                "[types:1292]Truncated incorrect DOUBLE value: '{val}'"
            )));
        }

        let seconds = converted.value.trunc();
        let fractional = converted.value - seconds;
        #[allow(clippy::cast_possible_truncation)]
        let nanos = (fractional * 1e9) as i64;
        #[allow(clippy::cast_possible_truncation)]
        DateTime::from_timestamp(seconds as i64, nanos.unsigned_abs() as u32)
            .ok_or_else(|| EvalCtxError::new(format!("timestamp out of range: '{val}'")))
    })
}

/// boundary: Go `variable.SessionVars`, narrowed to the fields the two files
/// of this package read after loading a system-variable map.
pub trait SessionVarsSnapshot {
    /// Go `SessionVars.Location()`.
    fn location(&self) -> TimeZone;
    /// Go `SessionVars.SQLMode`.
    fn sql_mode(&self) -> SqlMode;
    /// Go `SessionVars.MaxAllowedPacket`.
    fn max_allowed_packet(&self) -> u64;
    /// Go `SessionVars.EnableRedactLog`.
    fn enable_redact_log(&self) -> String;
    /// Go `SessionVars.DivPrecisionIncrement`.
    fn div_precision_increment(&self) -> i64;
    /// Go `SessionVars.GetCharsetInfo()`.
    fn charset_info(&self) -> (String, String);
    /// Go `SessionVars.DefaultCollationForUTF8MB4`.
    fn default_collation_for_utf8mb4(&self) -> String;
    /// Go `SessionVars.GetSystemVar(name)`.
    fn get_system_var(&self, name: &str) -> Option<String>;
    /// Go `SessionVars.SysdateIsNow`.
    fn sysdate_is_now(&self) -> bool;
    /// Go `SessionVars.NoopFuncsMode`.
    fn noop_funcs_mode(&self) -> i64;
    /// Go `SessionVars.WindowingUseHighPrecision`.
    fn windowing_use_high_precision(&self) -> bool;
    /// Go `SessionVars.GroupConcatMaxLen`.
    fn group_concat_max_len(&self) -> u64;
}

/// boundary: the `variable.SessionVars` that Go's
/// `newSessionVarsWithSystemVariables` builds, narrowed to exactly the
/// variables this package switches on.
///
/// Every other name is accepted and ignored: validating it needs the sysvar
/// catalog, which `tidb-session` owns. Go instead fails on an unknown name —
/// the one behavior difference, and it only widens what is accepted.
#[derive(Clone, Debug)]
pub struct StaticSessionVars {
    location: TimeZone,
    sql_mode: SqlMode,
    max_allowed_packet: u64,
    enable_redact_log: String,
    div_precision_increment: i64,
    charset: String,
    collation: String,
    default_collation_for_utf8mb4: String,
    sysdate_is_now: bool,
    noop_funcs_mode: i64,
    windowing_use_high_precision: bool,
    group_concat_max_len: u64,
    systems: HashMap<String, String>,
}

impl Default for StaticSessionVars {
    /// Go `variable.NewSessionVars(nil)`, in the fields this type carries.
    fn default() -> Self {
        let (charset, collation) = tidb_datatype::get_default_charset_and_collate();
        StaticSessionVars {
            location: TimeZone::Named(chrono_tz::Tz::UTC),
            sql_mode: default_sql_mode(),
            max_allowed_packet: DEF_MAX_ALLOWED_PACKET,
            enable_redact_log: DEF_TIDB_REDACT_LOG.to_owned(),
            div_precision_increment: DEF_DIV_PRECISION_INCREMENT,
            charset: charset.to_owned(),
            collation: collation.to_owned(),
            default_collation_for_utf8mb4: tidb_mysql::charset::DefaultCollationName.to_owned(),
            sysdate_is_now: DEF_SYSDATE_IS_NOW,
            noop_funcs_mode: tidb_opt_on_off_warn(DEF_TIDB_ENABLE_NOOP_FUNCS),
            windowing_use_high_precision: true,
            group_concat_max_len: DEF_GROUP_CONCAT_MAX_LEN,
            systems: HashMap::new(),
        }
    }
}

impl StaticSessionVars {
    /// Go `SessionVars.SetSystemVar`, over the variables of this package.
    ///
    /// The normalization Go's sysvar catalog performs before the `SetSession`
    /// hook runs (enum values folded to their registered spelling, booleans to
    /// `ON`/`OFF`) is reproduced here for the enum and boolean variables
    /// below, since the hooks observe the *normalized* value.
    pub fn set_system_var(&mut self, name: &str, val: &str) -> Result<(), EvalCtxError> {
        let lower = name.to_lowercase();
        self.systems.insert(lower.clone(), val.to_owned());
        match lower.as_str() {
            TIME_ZONE => {
                self.location =
                    parse_time_zone(val).map_err(|err| EvalCtxError::new(err.to_string()))?;
            }
            SQL_MODE_VAR => {
                self.sql_mode = get_sql_mode(&format_sql_mode_str(val))
                    .map_err(|err| EvalCtxError::new(err.to_string()))?;
            }
            MAX_ALLOWED_PACKET => {
                self.max_allowed_packet = parse_number(MAX_ALLOWED_PACKET, val)?;
            }
            TIDB_REDACT_LOG => {
                self.enable_redact_log =
                    normalize_enum(TIDB_REDACT_LOG, val, &["OFF", "ON", "MARKER"])?;
            }
            DIV_PRECISION_INCREMENT => {
                self.div_precision_increment = parse_number(DIV_PRECISION_INCREMENT, val)?;
            }
            CHARACTER_SET_CONNECTION => {
                let info = tidb_datatype::get_charset_info(val)
                    .map_err(|err| EvalCtxError::new(err.to_string()))?;
                self.charset = info.name;
                self.collation = info.default_collation;
            }
            COLLATION_CONNECTION => {
                let info = tidb_datatype::get_collation_by_name(val)
                    .map_err(|err| EvalCtxError::new(err.to_string()))?;
                self.charset = info.charset_name;
                self.collation = info.name;
            }
            DEFAULT_COLLATION_FOR_UTF8MB4 => {
                self.default_collation_for_utf8mb4 = val.to_owned();
            }
            TIDB_SYSDATE_IS_NOW => {
                self.sysdate_is_now = parse_bool(TIDB_SYSDATE_IS_NOW, val)?;
            }
            TIDB_ENABLE_NOOP_FUNCS => {
                let normalized =
                    normalize_enum(TIDB_ENABLE_NOOP_FUNCS, val, &["OFF", "ON", "WARN"])?;
                self.noop_funcs_mode = tidb_opt_on_off_warn(&normalized);
            }
            WINDOWING_USE_HIGH_PRECISION => {
                self.windowing_use_high_precision = parse_bool(WINDOWING_USE_HIGH_PRECISION, val)?;
            }
            GROUP_CONCAT_MAX_LEN => {
                self.group_concat_max_len = parse_number(GROUP_CONCAT_MAX_LEN, val)?;
            }
            // `timestamp`, `default_week_format` and `block_encryption_mode`
            // are read back through `get_system_var`; everything else is a
            // variable this package never reads. See the type's doc comment.
            _ => {}
        }
        Ok(())
    }
}

impl SessionVarsSnapshot for StaticSessionVars {
    fn location(&self) -> TimeZone {
        self.location.clone()
    }

    fn sql_mode(&self) -> SqlMode {
        self.sql_mode
    }

    fn max_allowed_packet(&self) -> u64 {
        self.max_allowed_packet
    }

    fn enable_redact_log(&self) -> String {
        self.enable_redact_log.clone()
    }

    fn div_precision_increment(&self) -> i64 {
        self.div_precision_increment
    }

    fn charset_info(&self) -> (String, String) {
        (self.charset.clone(), self.collation.clone())
    }

    fn default_collation_for_utf8mb4(&self) -> String {
        self.default_collation_for_utf8mb4.clone()
    }

    fn get_system_var(&self, name: &str) -> Option<String> {
        self.systems.get(&name.to_lowercase()).cloned()
    }

    fn sysdate_is_now(&self) -> bool {
        self.sysdate_is_now
    }

    fn noop_funcs_mode(&self) -> i64 {
        self.noop_funcs_mode
    }

    fn windowing_use_high_precision(&self) -> bool {
        self.windowing_use_high_precision
    }

    fn group_concat_max_len(&self) -> u64 {
        self.group_concat_max_len
    }
}

fn parse_number<T: std::str::FromStr>(name: &str, val: &str) -> Result<T, EvalCtxError> {
    val.trim().parse::<T>().map_err(|_| {
        EvalCtxError::new(format!(
            "[variable:1232]Incorrect argument type to variable '{name}'"
        ))
    })
}

/// The sysvar catalog's `TypeBool` normalization plus Go `TiDBOptOn`.
fn parse_bool(name: &str, val: &str) -> Result<bool, EvalCtxError> {
    match val.trim() {
        "1" => Ok(true),
        "0" => Ok(false),
        other if other.eq_ignore_ascii_case("ON") || other.eq_ignore_ascii_case("TRUE") => Ok(true),
        other if other.eq_ignore_ascii_case("OFF") || other.eq_ignore_ascii_case("FALSE") => {
            Ok(false)
        }
        other => Err(EvalCtxError::new(format!(
            "[variable:1231]Variable '{name}' can't be set to the value of '{other}'"
        ))),
    }
}

/// The sysvar catalog's `TypeEnum` normalization: the registered spelling of a
/// case-insensitive match.
fn normalize_enum(name: &str, val: &str, values: &[&str]) -> Result<String, EvalCtxError> {
    values
        .iter()
        .find(|candidate| candidate.eq_ignore_ascii_case(val.trim()))
        .map(|candidate| (*candidate).to_owned())
        .ok_or_else(|| {
            EvalCtxError::new(format!(
                "[variable:1231]Variable '{name}' can't be set to the value of '{val}'"
            ))
        })
}

/// Go `newSessionVarsWithSystemVariables`.
pub fn new_session_vars_with_system_variables(
    vars: &HashMap<String, String>,
) -> Result<StaticSessionVars, EvalCtxError> {
    let mut session_vars = StaticSessionVars::default();
    let mut charset: Option<(&str, &str)> = None;
    let mut collation: Option<(&str, &str)> = None;

    for (name, val) in vars {
        match name.to_lowercase().as_str() {
            // Go: `charset_connection` and `collation_connection` overwrite
            // each other, so they are applied last, charset first.
            CHARACTER_SET_CONNECTION => charset = Some((name, val)),
            COLLATION_CONNECTION => collation = Some((name, val)),
            // Go reaches `tidb_redact_log` through `SetGlobalFromHook`
            // because the variable has no session scope; the session field it
            // sets is the same one `set_system_var` writes here.
            _ => session_vars.set_system_var(name, val)?,
        }
    }

    if let Some((name, val)) = charset {
        session_vars.set_system_var(name, val)?;
    }

    if let Some((name, val)) = collation {
        session_vars.set_system_var(name, val)?;
    }

    Ok(session_vars)
}

/// boundary: Go `exprctx.StaticConvertibleEvalContext`, narrowed to the
/// methods [`make_eval_context_static`] calls. Go's version embeds the
/// `exprctx.EvalContext` umbrella interface, which [`crate::exprctx`] does not
/// carry yet; the accessors below stand for the `TypeCtx()`/`ErrCtx()` parts
/// of it that survive into a static snapshot.
pub trait StaticConvertibleEvalContext {
    /// Go `EvalContext.SQLMode`.
    fn sql_mode(&self) -> SqlMode;
    /// Go `EvalContext.TypeCtx().Flags()`.
    fn type_flags(&self) -> ConversionFlags;
    /// Go `EvalContext.TypeCtx().Location()`.
    fn location(&self) -> TimeZone;
    /// Go `EvalContext.ErrCtx().LevelMap()`.
    fn err_level_map(&self) -> LevelMap;
    /// Go `EvalContext.CurrentDB`.
    fn current_db(&self) -> String;
    /// Go `EvalContext.CurrentTime`.
    fn current_time(&self) -> Result<DateTime<Utc>, EvalCtxError>;
    /// Go `EvalContext.GetMaxAllowedPacket`.
    fn get_max_allowed_packet(&self) -> u64;
    /// Go `EvalContext.GetDefaultWeekFormatMode`.
    fn get_default_week_format_mode(&self) -> String;
    /// Go `EvalContext.GetDivPrecisionIncrement`.
    fn get_div_precision_increment(&self) -> i64;
    /// Go `EvalContext.GetTiDBRedactLog`.
    fn get_tidb_redact_log(&self) -> String;
    /// Go `EvalContext.GetUserVarsReader`.
    fn get_user_vars_reader(&self) -> Arc<dyn UserVarsReader + Send + Sync>;
    /// Go `StaticConvertibleEvalContext.AllParamValues`.
    fn all_param_values(&self) -> Vec<Datum>;
    /// Go `StaticConvertibleEvalContext.GetWarnHandler`.
    fn get_warn_handler(&self) -> Arc<dyn WarnHandler + Send + Sync>;
}

impl StaticConvertibleEvalContext for EvalContext {
    fn sql_mode(&self) -> SqlMode {
        EvalContext::sql_mode(self)
    }

    fn type_flags(&self) -> ConversionFlags {
        EvalContext::type_flags(self)
    }

    fn location(&self) -> TimeZone {
        EvalContext::location(self).clone()
    }

    fn err_level_map(&self) -> LevelMap {
        EvalContext::err_level_map(self)
    }

    fn current_db(&self) -> String {
        EvalContext::current_db(self).to_owned()
    }

    fn current_time(&self) -> Result<DateTime<Utc>, EvalCtxError> {
        EvalContext::current_time(self)
    }

    fn get_max_allowed_packet(&self) -> u64 {
        EvalContext::get_max_allowed_packet(self)
    }

    fn get_default_week_format_mode(&self) -> String {
        EvalContext::get_default_week_format_mode(self).to_owned()
    }

    fn get_div_precision_increment(&self) -> i64 {
        EvalContext::get_div_precision_increment(self)
    }

    fn get_tidb_redact_log(&self) -> String {
        EvalContext::get_tidb_redact_log(self).to_owned()
    }

    fn get_user_vars_reader(&self) -> Arc<dyn UserVarsReader + Send + Sync> {
        Arc::clone(EvalContext::get_user_vars_reader(self))
    }

    fn all_param_values(&self) -> Vec<Datum> {
        EvalContext::all_param_values(self).to_vec()
    }

    fn get_warn_handler(&self) -> Arc<dyn WarnHandler + Send + Sync> {
        Arc::clone(EvalContext::get_warn_handler(self))
    }
}

/// Go `MakeEvalContextStatic`: converts a `StaticConvertibleEvalContext` into
/// an [`EvalContext`].
#[must_use]
pub fn make_eval_context_static(ctx: &dyn StaticConvertibleEvalContext) -> EvalContext {
    // Go's TODO stands: no optional eval property provider is suitable for a
    // static context yet, so the snapshot carries none.
    let props: Vec<Arc<dyn DynOptionalEvalPropProvider>> = Vec::new();

    // Go wraps the current time in a closure that captures the value, not the
    // source context, so a later statement cannot move it.
    let captured = ctx.current_time();

    EvalContext::new([
        with_warn_handler(ctx.get_warn_handler()),
        with_sql_mode(ctx.sql_mode()),
        with_type_flags(ctx.type_flags()),
        with_location(ctx.location()),
        with_err_level_map(ctx.err_level_map()),
        with_current_db(ctx.current_db()),
        with_current_time(Arc::new(move || captured.clone())),
        with_max_allowed_packet(ctx.get_max_allowed_packet()),
        with_default_week_format_mode(ctx.get_default_week_format_mode()),
        with_div_precision_increment(ctx.get_div_precision_increment()),
        with_param_list(&ctx.all_param_values()),
        with_user_vars_reader(Arc::from(ctx.get_user_vars_reader().clone_reader())),
        with_optional_property(props),
        with_enable_redact_log(ctx.get_tidb_redact_log()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expropt::{
        CurrentUserPropProvider, CurrentUserPropReader, DdlOwnerInfoProvider, DdlOwnerPropReader,
        InfoSchemaPropProvider,
    };
    use crate::metabuild::MetaOnlyInfoSchema;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tidb_error::errctx::{ErrGroup, Level};
    use tidb_error::terror::TerrorCode;
    use tidb_mysql::consts::{
        ModeAllowInvalidDates, ModeNoZeroDate, ModeOnlyFullGroupBy, ModeStrictTransTables,
    };
    use tidb_parser::auth::{RoleIdentity, UserIdentity};

    struct MockInfoSchema;

    impl MetaOnlyInfoSchema for MockInfoSchema {
        fn schema_meta_version(&self) -> i64 {
            0
        }
    }

    fn warn_texts(warnings: &[SqlWarn]) -> Vec<(String, String)> {
        warnings
            .iter()
            .map(|w| (w.level.clone(), w.err.to_string()))
            .collect()
    }

    fn tz(name: &str) -> TimeZone {
        tidb_util::timeutil::load_location(name).unwrap()
    }

    // Go `checkDefaultStaticEvalCtx`.
    fn check_default_static_eval_ctx(ctx: &EvalContext) {
        assert_eq!(ctx.sql_mode(), get_sql_mode(DefaultSQLMode).unwrap());
        assert_eq!(*ctx.location(), TimeZone::Named(chrono_tz::Tz::UTC));
        assert_eq!(ctx.type_flags(), STRICT_FLAGS);
        assert_eq!(ctx.type_ctx().flags(), STRICT_FLAGS);
        assert_eq!(ctx.type_ctx().location().name(), "UTC");
        assert_eq!(ctx.err_level_map(), LevelMap::strict());
        assert_eq!(ctx.err_ctx().level_map(), LevelMap::strict());
        assert_eq!(ctx.current_db(), "");
        assert_eq!(ctx.get_max_allowed_packet(), DEF_MAX_ALLOWED_PACKET);
        assert_eq!(ctx.get_default_week_format_mode(), DEF_DEFAULT_WEEK_FORMAT);
        assert_eq!(
            ctx.get_div_precision_increment(),
            DEF_DIV_PRECISION_INCREMENT
        );
        assert!(ctx.all_param_values().is_empty());
        assert!(ctx.get_user_vars_reader().get_user_var_val("a").is_none());
        assert!(ctx.get_optional_prop_set().is_empty());
        assert!(ctx
            .get_optional_prop_provider(OptionalEvalPropKey::AdvisoryLock)
            .is_none());

        let now = ctx.current_time().unwrap();
        assert!((now.timestamp() - Utc::now().timestamp()).abs() <= 5);

        // Go asserts the default handler is a `*StaticWarnHandler`; the
        // observable half of that is its empty, countable warning store.
        assert_eq!(ctx.warning_count(), 0);
    }

    // Go `evalCtxOptionsTestState`.
    struct EvalCtxOptionsTestState {
        now: DateTime<Utc>,
        loc: TimeZone,
        warn_handler: Arc<StaticWarnHandler>,
        user_vars: Arc<UserVars>,
        ddl_owner: Arc<AtomicBool>,
    }

    // Go `getEvalCtxOptionsForTest`.
    fn eval_ctx_options_for_test() -> (Vec<EvalCtxOption>, EvalCtxOptionsTestState) {
        let state = EvalCtxOptionsTestState {
            now: Utc::now(),
            loc: tz("America/New_York"),
            warn_handler: Arc::new(StaticWarnHandler::new(8)),
            user_vars: Arc::new(UserVars::new()),
            ddl_owner: Arc::new(AtomicBool::new(false)),
        };

        let provider1 = CurrentUserPropProvider::new(|| {
            (
                Some(Arc::new(UserIdentity {
                    username: "user1".to_owned(),
                    hostname: "host1".to_owned(),
                    ..UserIdentity::default()
                })),
                vec![Arc::new(RoleIdentity {
                    username: "role1".to_owned(),
                    hostname: "host2".to_owned(),
                })],
            )
        });

        let ddl_owner = Arc::clone(&state.ddl_owner);
        let provider2 = DdlOwnerInfoProvider::new(move || ddl_owner.load(Ordering::SeqCst));

        let now = state.now;
        let opts = vec![
            with_warn_handler(Arc::clone(&state.warn_handler) as _),
            with_sql_mode(ModeNoZeroDate | ModeStrictTransTables),
            with_type_flags(
                STRICT_FLAGS
                    .with_allow_negative_to_unsigned(true)
                    .with_skip_ascii_check(true),
            ),
            with_err_level_map(
                LevelMap::strict()
                    .with_level(ErrGroup::BadNull, Level::Error)
                    .with_level(ErrGroup::NoDefault, Level::Error)
                    .with_level(ErrGroup::DividedByZero, Level::Warn),
            ),
            with_location(state.loc.clone()),
            with_current_db("db1"),
            with_current_time(Arc::new(move || Ok(now))),
            with_max_allowed_packet(12345),
            with_default_week_format_mode("3"),
            with_div_precision_increment(5),
            with_user_vars_reader(Arc::clone(&state.user_vars) as _),
            with_optional_property(vec![Arc::new(provider1), Arc::new(provider2)]),
        ];
        (opts, state)
    }

    // Go `checkOptionsStaticEvalCtx`.
    fn check_options_static_eval_ctx(ctx: &EvalContext, state: &EvalCtxOptionsTestState) {
        assert!(Arc::ptr_eq(
            ctx.get_warn_handler(),
            &(Arc::clone(&state.warn_handler) as Arc<dyn WarnHandler + Send + Sync>)
        ));
        assert_eq!(ctx.sql_mode(), ModeNoZeroDate | ModeStrictTransTables);
        assert_eq!(
            ctx.type_flags(),
            STRICT_FLAGS
                .with_allow_negative_to_unsigned(true)
                .with_skip_ascii_check(true)
        );
        assert_eq!(
            ctx.err_level_map(),
            LevelMap::strict()
                .with_level(ErrGroup::BadNull, Level::Error)
                .with_level(ErrGroup::NoDefault, Level::Error)
                .with_level(ErrGroup::DividedByZero, Level::Warn)
        );
        assert_eq!(*ctx.location(), state.loc);
        assert_eq!(ctx.current_db(), "db1");
        let current = ctx.current_time().unwrap();
        assert_eq!(
            current.timestamp_nanos_opt(),
            state.now.timestamp_nanos_opt()
        );
        assert_eq!(ctx.get_max_allowed_packet(), 12345);
        assert_eq!(ctx.get_default_week_format_mode(), "3");
        assert_eq!(ctx.get_div_precision_increment(), 5);
        assert!(Arc::ptr_eq(
            ctx.get_user_vars_reader(),
            &(Arc::clone(&state.user_vars) as Arc<dyn UserVarsReader + Send + Sync>)
        ));

        let opt_set = OptionalEvalPropKeySet::default()
            .add(OptionalEvalPropKey::CurrentUser)
            .add(OptionalEvalPropKey::DdlOwnerInfo);
        assert_eq!(ctx.get_optional_prop_set(), opt_set);

        let user = CurrentUserPropReader.current_user(ctx).unwrap().unwrap();
        assert_eq!(user.username, "user1");
        assert_eq!(user.hostname, "host1");
        let roles = CurrentUserPropReader.active_roles(ctx).unwrap();
        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0].username, "role1");
        assert_eq!(roles[0].hostname, "host2");

        state.ddl_owner.store(true, Ordering::SeqCst);
        assert!(DdlOwnerPropReader.is_ddl_owner(ctx).unwrap());
        state.ddl_owner.store(false, Ordering::SeqCst);
        assert!(!DdlOwnerPropReader.is_ddl_owner(ctx).unwrap());

        assert!(ctx
            .get_optional_prop_provider(OptionalEvalPropKey::InfoSchema)
            .is_none());
    }

    // Go `TestNewStaticEvalCtx`.
    #[test]
    fn new_static_eval_ctx() {
        // Go asserts each id is exactly `prev+1`; the counter is process-wide
        // and Rust runs tests in parallel threads, so the assertion here is
        // that each new context took a LATER id.
        let prev_id = gen_context_id();
        let ctx = EvalContext::new([]);
        assert!(ctx.ctx_id() > prev_id);
        check_default_static_eval_ctx(&ctx);

        let prev_id = ctx.ctx_id();
        let (options, state) = eval_ctx_options_for_test();
        let ctx = EvalContext::new(options);
        assert!(ctx.ctx_id() > prev_id);
        check_options_static_eval_ctx(&ctx, &state);
    }

    // Go `TestStaticEvalCtxCurrentTime`.
    #[test]
    fn static_eval_ctx_current_time() {
        let loc1 = tz("America/New_York");
        let time = DateTime::from_timestamp_micros(123_456_789).unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in_fn = Arc::clone(&calls);
        let get_time: CurrentTimeFn = Arc::new(move || {
            let call = calls_in_fn.fetch_add(1, Ordering::SeqCst);
            match call {
                0 | 1 => Err(EvalCtxError::new(format!("err{call}"))),
                2 => Ok(time),
                _ => Err(EvalCtxError::new("should not reach here")),
            }
        });

        let ctx = EvalContext::new([with_current_time(get_time)]);

        // The first two calls fail.
        assert_eq!(ctx.current_time().unwrap_err(), EvalCtxError::new("err0"));
        assert_eq!(ctx.current_time().unwrap_err(), EvalCtxError::new("err1"));

        // The third succeeds.
        let got = ctx.current_time().unwrap();
        assert_eq!(got.timestamp_nanos_opt(), time.timestamp_nanos_opt());
        assert_eq!(*ctx.location(), TimeZone::Named(chrono_tz::Tz::UTC));
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        // And is cached: the inner function is not called again.
        let got = ctx.current_time().unwrap();
        assert_eq!(got.timestamp_nanos_opt(), time.timestamp_nanos_opt());
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        // The current time is reported in the context's own location.
        let loc2 = tz("Australia/Sydney");
        let ctx = EvalContext::new([
            with_location(loc2.clone()),
            with_current_time(Arc::new(move || Ok(time))),
        ]);
        let got = ctx.current_time().unwrap();
        assert_eq!(got.timestamp_nanos_opt(), time.timestamp_nanos_opt());
        assert_eq!(*ctx.location(), loc2);

        // Apply copies the current time.
        let ctx2 = ctx.apply([]);
        let got = ctx2.current_time().unwrap();
        assert_eq!(got.timestamp_nanos_opt(), time.timestamp_nanos_opt());
        assert_eq!(*ctx2.location(), loc2);

        // Apply with a location changes where that same instant is reported.
        let ctx2 = ctx.apply([with_location(loc1.clone())]);
        let got = ctx2.current_time().unwrap();
        assert_eq!(got.timestamp_nanos_opt(), time.timestamp_nanos_opt());
        assert_eq!(*ctx2.location(), loc1);

        // Apply does not affect the previous context.
        let got = ctx.current_time().unwrap();
        assert_eq!(got.timestamp_nanos_opt(), time.timestamp_nanos_opt());
        assert_eq!(*ctx.location(), loc2);

        // Apply with a different current-time function.
        let other = DateTime::from_timestamp_micros(987_654_321).unwrap();
        let ctx2 = ctx.apply([with_current_time(Arc::new(move || Ok(other)))]);
        let got = ctx2.current_time().unwrap();
        assert_eq!(got.timestamp_micros(), 987_654_321);
        assert_eq!(*ctx2.location(), loc2);

        let got = ctx.current_time().unwrap();
        assert_eq!(got.timestamp_nanos_opt(), time.timestamp_nanos_opt());
    }

    // Go `TestStaticEvalCtxWarnings`.
    #[test]
    fn static_eval_ctx_warnings() {
        use tidb_util::context::{WARN_LEVEL_NOTE, WARN_LEVEL_WARNING};

        // The default context has an empty static warning handler.
        let ctx = EvalContext::new([]);
        assert_eq!(ctx.warning_count(), 0);

        // `with_warn_handler` installs the given handler.
        let ignore: Arc<dyn WarnHandler + Send + Sync> = Arc::new(tidb_util::context::IgnoreWarn);
        let ctx = EvalContext::new([with_warn_handler(Arc::clone(&ignore))]);
        assert!(Arc::ptr_eq(ctx.get_warn_handler(), &ignore));

        // Every context derived from one handler reaches that one handler.
        let handler = Arc::new(StaticWarnHandler::new(8));
        let ctx = EvalContext::new([with_warn_handler(Arc::clone(&handler) as _)]);
        let type_ctx = ctx.type_ctx();
        let err_ctx = ctx.err_ctx();
        handler.append_warning(WarnErr::from("warn0"));
        ctx.append_warning(WarnErr::from("warn1"));
        ctx.append_note(WarnErr::from("note1"));
        type_ctx.append_warning(terror("warn2"));
        err_ctx.append_warning(shared_error("warn3"));
        assert_eq!(handler.warning_count(), 5);
        assert_eq!(ctx.warning_count(), handler.warning_count());

        let warnings = ctx.copy_warnings();
        assert_eq!(
            warn_texts(&warnings),
            vec![
                (WARN_LEVEL_WARNING.to_owned(), "warn0".to_owned()),
                (WARN_LEVEL_WARNING.to_owned(), "warn1".to_owned()),
                (WARN_LEVEL_NOTE.to_owned(), "note1".to_owned()),
                (WARN_LEVEL_WARNING.to_owned(), terror("warn2").to_string()),
                (WARN_LEVEL_WARNING.to_owned(), "warn3".to_owned()),
            ]
        );
        assert_eq!(handler.warning_count(), 5);

        let warnings = ctx.truncate_warnings(2);
        assert_eq!(
            warn_texts(&warnings),
            vec![
                (WARN_LEVEL_NOTE.to_owned(), "note1".to_owned()),
                (WARN_LEVEL_WARNING.to_owned(), terror("warn2").to_string()),
                (WARN_LEVEL_WARNING.to_owned(), "warn3".to_owned()),
            ]
        );
        assert_eq!(handler.warning_count(), 2);
        assert_eq!(ctx.warning_count(), 2);
        assert_eq!(
            warn_texts(&ctx.copy_warnings()),
            vec![
                (WARN_LEVEL_WARNING.to_owned(), "warn0".to_owned()),
                (WARN_LEVEL_WARNING.to_owned(), "warn1".to_owned()),
            ]
        );

        // Apply keeps the old handler by default.
        let ctx2 = ctx.apply([]);
        assert!(Arc::ptr_eq(ctx.get_warn_handler(), ctx2.get_warn_handler()));

        // Apply with `with_warn_handler` replaces it for the new context only.
        let handler2 = Arc::new(StaticWarnHandler::new(16));
        let ctx2 = ctx.apply([with_warn_handler(Arc::clone(&handler2) as _)]);
        assert!(Arc::ptr_eq(
            ctx2.get_warn_handler(),
            &(Arc::clone(&handler2) as Arc<dyn WarnHandler + Send + Sync>)
        ));
        assert!(Arc::ptr_eq(
            ctx.get_warn_handler(),
            &(Arc::clone(&handler) as Arc<dyn WarnHandler + Send + Sync>)
        ));

        // The type and error contexts of each use their own handler.
        let _ = ctx.truncate_warnings(0);
        let (type_ctx, err_ctx) = (ctx.type_ctx(), ctx.err_ctx());
        let (type_ctx2, err_ctx2) = (ctx2.type_ctx(), ctx2.err_ctx());
        type_ctx2.append_warning(terror("warn4"));
        err_ctx2.append_warning(shared_error("warn5"));
        type_ctx.append_warning(terror("warn6"));
        err_ctx.append_warning(shared_error("warn7"));
        assert_eq!(
            warn_texts(&ctx2.copy_warnings()),
            vec![
                (WARN_LEVEL_WARNING.to_owned(), terror("warn4").to_string()),
                (WARN_LEVEL_WARNING.to_owned(), "warn5".to_owned()),
            ]
        );
        assert_eq!(
            warn_texts(&ctx.copy_warnings()),
            vec![
                (WARN_LEVEL_WARNING.to_owned(), terror("warn6").to_string()),
                (WARN_LEVEL_WARNING.to_owned(), "warn7".to_owned()),
            ]
        );
    }

    /// A warning raised through the *type* context, which carries a typed
    /// terror rather than Go's open `error`; its rendered text is
    /// `[0]<message>`.
    fn terror(message: &str) -> TerrorError {
        TerrorError::compatible(TerrorCode::new(0), message)
    }

    fn shared_error(message: &str) -> SharedError {
        Arc::new(EvalCtxError::new(message))
    }

    // Go `TestStaticEvalContextOptionalProps`.
    #[test]
    fn static_eval_context_optional_props() {
        let ctx = EvalContext::new([]);
        assert!(ctx.get_optional_prop_set().is_empty());

        let ctx2 = ctx.apply([with_optional_property(vec![Arc::new(
            CurrentUserPropProvider::new(|| (None, Vec::new())),
        )])]);
        let empty = OptionalEvalPropKeySet::default();
        assert_eq!(ctx.get_optional_prop_set(), empty);
        assert_eq!(
            ctx2.get_optional_prop_set(),
            empty.add(OptionalEvalPropKey::CurrentUser)
        );

        // Apply overrides all optional properties.
        let ctx3 = ctx2.apply([with_optional_property(vec![
            Arc::new(DdlOwnerInfoProvider::new(|| true)),
            Arc::new(InfoSchemaPropProvider::new(|_is_domain| {
                Arc::new(MockInfoSchema) as Arc<dyn MetaOnlyInfoSchema + Send + Sync>
            })),
        ])]);
        assert_eq!(
            ctx3.get_optional_prop_set(),
            empty
                .add(OptionalEvalPropKey::DdlOwnerInfo)
                .add(OptionalEvalPropKey::InfoSchema)
        );
        assert_eq!(ctx.get_optional_prop_set(), empty);
        assert_eq!(
            ctx2.get_optional_prop_set(),
            empty.add(OptionalEvalPropKey::CurrentUser)
        );
    }

    // Go `TestUpdateStaticEvalContext`. Go's `deeptest` reflection walk over
    // `evalCtxState` has no Rust counterpart; the fields it compares are
    // asserted directly.
    #[test]
    fn update_static_eval_context() {
        let old_ctx = EvalContext::new([]);
        let ctx = old_ctx.apply([]);

        // A different context, with a greater id.
        assert!(ctx.ctx_id() > old_ctx.ctx_id());

        // Every field except the ones `Apply` rebuilds is carried over.
        assert_eq!(ctx.sql_mode(), old_ctx.sql_mode());
        assert_eq!(ctx.type_flags(), old_ctx.type_flags());
        assert_eq!(ctx.location(), old_ctx.location());
        assert_eq!(ctx.err_level_map(), old_ctx.err_level_map());
        assert_eq!(ctx.current_db(), old_ctx.current_db());
        assert_eq!(
            ctx.get_max_allowed_packet(),
            old_ctx.get_max_allowed_packet()
        );
        assert_eq!(ctx.get_tidb_redact_log(), old_ctx.get_tidb_redact_log());
        assert_eq!(
            ctx.get_default_week_format_mode(),
            old_ctx.get_default_week_format_mode()
        );
        assert_eq!(
            ctx.get_div_precision_increment(),
            old_ctx.get_div_precision_increment()
        );
        assert_eq!(ctx.all_param_values(), old_ctx.all_param_values());
        assert!(Arc::ptr_eq(
            ctx.get_warn_handler(),
            old_ctx.get_warn_handler()
        ));
        assert!(Arc::ptr_eq(
            ctx.get_user_vars_reader(),
            old_ctx.get_user_vars_reader()
        ));
        assert_eq!(ctx.get_optional_prop_set(), old_ctx.get_optional_prop_set());

        check_default_static_eval_ctx(&ctx);

        // Apply options.
        let (opts, opt_state) = eval_ctx_options_for_test();
        let ctx2 = old_ctx.apply(opts);
        assert!(ctx2.ctx_id() > ctx.ctx_id());
        check_options_static_eval_ctx(&ctx2, &opt_state);

        // The old context is unaffected.
        check_default_static_eval_ctx(&old_ctx);

        // The same options through the constructor.
        let (opts, opt_state) = eval_ctx_options_for_test();
        let ctx3 = EvalContext::new(opts);
        assert!(ctx3.ctx_id() > ctx2.ctx_id());
        check_options_static_eval_ctx(&ctx3, &opt_state);
    }

    // Go `TestParamList`. Go's `variable.PlanCacheParamList` is the boundary
    // named in the module header; the datum slice it hands over is the input
    // here, and mutating the slice afterwards stands for `Reset`/`Append`.
    #[test]
    fn param_list() {
        let mut params = vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)];
        let ctx = EvalContext::new([with_param_list(&params)]);
        for i in 0..3 {
            assert_eq!(ctx.get_param_value(i).unwrap(), Datum::Int(i as i64 + 1));
        }

        // After resetting the source list, the context still holds its copy.
        params.clear();
        params.push(Datum::Int(4));
        for i in 0..3 {
            assert_eq!(ctx.get_param_value(i).unwrap(), Datum::Int(i as i64 + 1));
        }

        assert_eq!(
            ctx.get_param_value(3).unwrap_err().message(),
            ERR_PARAM_INDEX_EXCEED_PARAM_COUNTS
        );
    }

    // Go `TestMakeEvalContextStatic`. Go drives the comparison with
    // `deeptest.AssertDeepClonedEqual`; the fields that walk covers are
    // compared field by field here.
    #[test]
    fn make_eval_context_static_copies_every_field() {
        let params = vec![Datum::Int(1)];

        let user_vars = Arc::new(UserVars::new());
        user_vars.set_user_var_val("a", Datum::Bytes(b"v1".to_vec()));
        user_vars.set_user_var_val("b", Datum::Int(2));

        let obj = EvalContext::new([
            with_warn_handler(Arc::new(StaticWarnHandler::new(16))),
            with_sql_mode(ModeNoZeroDate | ModeStrictTransTables),
            with_type_flags(
                STRICT_FLAGS
                    .with_allow_negative_to_unsigned(true)
                    .with_skip_ascii_check(true),
            ),
            with_err_level_map(LevelMap::strict()),
            with_location(TimeZone::Named(chrono_tz::Tz::UTC)),
            with_current_db("db1"),
            with_current_time(Arc::new(|| Ok(Utc::now()))),
            with_max_allowed_packet(12345),
            with_default_week_format_mode("3"),
            with_div_precision_increment(5),
            with_param_list(&params),
            with_user_vars_reader(Arc::clone(&user_vars) as _),
            with_optional_property(vec![Arc::new(DdlOwnerInfoProvider::new(|| true))]),
            with_enable_redact_log("test"),
        ]);
        obj.append_warning(WarnErr::from("test warning"));

        // Go first proves every field differs from a default context.
        let default_ctx = EvalContext::new([]);
        assert_ne!(obj.sql_mode(), default_ctx.sql_mode());
        assert_ne!(obj.type_flags(), default_ctx.type_flags());
        assert_ne!(obj.current_db(), default_ctx.current_db());
        assert_ne!(
            obj.get_max_allowed_packet(),
            default_ctx.get_max_allowed_packet()
        );
        assert_ne!(
            obj.get_default_week_format_mode(),
            default_ctx.get_default_week_format_mode()
        );
        assert_ne!(
            obj.get_div_precision_increment(),
            default_ctx.get_div_precision_increment()
        );
        assert_ne!(obj.get_tidb_redact_log(), default_ctx.get_tidb_redact_log());
        assert_ne!(
            obj.all_param_values().len(),
            default_ctx.all_param_values().len()
        );

        let static_obj = make_eval_context_static(&obj);

        assert_eq!(static_obj.sql_mode(), obj.sql_mode());
        assert_eq!(static_obj.type_flags(), obj.type_flags());
        assert_eq!(static_obj.err_level_map(), obj.err_level_map());
        assert_eq!(static_obj.location(), obj.location());
        assert_eq!(static_obj.current_db(), obj.current_db());
        assert_eq!(
            static_obj.get_max_allowed_packet(),
            obj.get_max_allowed_packet()
        );
        assert_eq!(
            static_obj.get_default_week_format_mode(),
            obj.get_default_week_format_mode()
        );
        assert_eq!(
            static_obj.get_div_precision_increment(),
            obj.get_div_precision_increment()
        );
        assert_eq!(static_obj.get_tidb_redact_log(), obj.get_tidb_redact_log());
        assert_eq!(static_obj.all_param_values(), obj.all_param_values());
        assert_ne!(static_obj.ctx_id(), obj.ctx_id());

        // The warning handler is shared, not cloned.
        assert!(Arc::ptr_eq(
            static_obj.get_warn_handler(),
            obj.get_warn_handler()
        ));

        // The user variables are cloned, not shared.
        assert!(!Arc::ptr_eq(
            static_obj.get_user_vars_reader(),
            obj.get_user_vars_reader()
        ));
        assert_eq!(
            static_obj.get_user_vars_reader().get_user_var_val("a"),
            Some(Datum::Bytes(b"v1".to_vec()))
        );
        assert_eq!(
            static_obj.get_user_vars_reader().get_user_var_val("b"),
            Some(Datum::Int(2))
        );

        let old_time = obj.current_time().unwrap();
        let new_time = static_obj.current_time().unwrap();
        assert_eq!(old_time.timestamp(), new_time.timestamp());

        // No optional property is copied yet.
        assert_ne!(
            static_obj.get_optional_prop_set(),
            obj.get_optional_prop_set()
        );
        assert_eq!(
            static_obj.get_optional_prop_set(),
            OptionalEvalPropKeySet(0)
        );
    }

    // Go `TestEvalCtxLoadSystemVars`.
    #[test]
    fn eval_ctx_load_system_vars() {
        let vars: Vec<(&str, &str)> = vec![
            ("time_zone", "Europe/Berlin"),
            ("sql_mode", "ALLOW_INVALID_DATES,ONLY_FULL_GROUP_BY"),
            ("timestamp", "1234567890.123456"),
            // Upper case on purpose: the name is folded.
            ("MAX_ALLOWED_PACKET", "524288"),
            ("TIDB_REDACT_LOG", "ON"),
            ("default_week_format", "5"),
            ("div_precision_increment", "12"),
        ];

        let mut vars_map = HashMap::new();
        for (name, val) in &vars {
            vars_map.insert((*name).to_owned(), (*val).to_owned());
        }
        let session_vars = new_session_vars_with_system_variables(&vars_map).unwrap();

        let default_eval_ctx = EvalContext::new([]);
        let ctx = default_eval_ctx.load_system_vars(&vars_map).unwrap();
        assert!(ctx.ctx_id() > default_eval_ctx.ctx_id());

        // Every variable-related field changed...
        assert_ne!(ctx.location(), default_eval_ctx.location());
        assert_ne!(ctx.sql_mode(), default_eval_ctx.sql_mode());
        assert_ne!(
            ctx.get_max_allowed_packet(),
            default_eval_ctx.get_max_allowed_packet()
        );
        assert_ne!(
            ctx.get_tidb_redact_log(),
            default_eval_ctx.get_tidb_redact_log()
        );
        assert_ne!(
            ctx.get_default_week_format_mode(),
            default_eval_ctx.get_default_week_format_mode()
        );
        assert_ne!(
            ctx.get_div_precision_increment(),
            default_eval_ctx.get_div_precision_increment()
        );
        assert_ne!(
            ctx.current_time().unwrap().timestamp(),
            default_eval_ctx.current_time().unwrap().timestamp()
        );

        // ...and every unrelated field did not.
        assert_eq!(ctx.type_flags(), default_eval_ctx.type_flags());
        assert_eq!(ctx.err_level_map(), default_eval_ctx.err_level_map());
        assert_eq!(ctx.current_db(), default_eval_ctx.current_db());
        assert_eq!(ctx.all_param_values(), default_eval_ctx.all_param_values());
        assert!(Arc::ptr_eq(
            ctx.get_warn_handler(),
            default_eval_ctx.get_warn_handler()
        ));
        assert!(Arc::ptr_eq(
            ctx.get_user_vars_reader(),
            default_eval_ctx.get_user_vars_reader()
        ));
        assert_eq!(
            ctx.get_optional_prop_set(),
            default_eval_ctx.get_optional_prop_set()
        );

        // Each variable's own assertion, against the session snapshot.
        assert_eq!(zone_name(ctx.location()), "Europe/Berlin");
        assert_eq!(
            zone_name(ctx.location()),
            zone_name(&session_vars.location())
        );

        assert_eq!(ctx.sql_mode(), ModeAllowInvalidDates | ModeOnlyFullGroupBy);
        assert_eq!(ctx.sql_mode(), session_vars.sql_mode());

        assert_eq!(
            ctx.current_time().unwrap().timestamp_micros(),
            1_234_567_890_123_456
        );

        assert_eq!(ctx.get_max_allowed_packet(), 524_288);
        assert_eq!(
            ctx.get_max_allowed_packet(),
            session_vars.max_allowed_packet()
        );

        assert_eq!(ctx.get_tidb_redact_log(), "ON");
        assert_eq!(ctx.get_tidb_redact_log(), session_vars.enable_redact_log());

        assert_eq!(ctx.get_default_week_format_mode(), "5");
        assert_eq!(
            ctx.get_default_week_format_mode(),
            session_vars.get_system_var(DEFAULT_WEEK_FORMAT).unwrap()
        );

        assert_eq!(ctx.get_div_precision_increment(), 12);
        assert_eq!(
            ctx.get_div_precision_increment(),
            session_vars.div_precision_increment()
        );

        // `@@timestamp` set to its default means "now".
        let mut default_timestamp = HashMap::new();
        default_timestamp.insert("timestamp".to_owned(), DEF_TIMESTAMP.to_owned());
        let ctx = default_eval_ctx
            .load_system_vars(&default_timestamp)
            .unwrap();
        let now = ctx.current_time().unwrap();
        assert!((now.timestamp() - Utc::now().timestamp()).abs() <= 5);
    }
}
