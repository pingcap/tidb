//! Concrete local, mock, and PD-backed timestamp oracles.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use futures::future::{BoxFuture, FutureExt, Shared};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;

use super::{
    add_wrapping_nanoseconds, extract_physical, get_physical, get_time_from_timestamp,
    system_time_to_timestamp, Oracle, OracleError, OracleOption, OracleResult,
    ReadTimestampValidator, TimestampFuture,
};

/// Enables source-compatible validation of user-supplied read timestamps by
/// [`PdOracle`]. It is disabled by default as in client-go.
pub static ENABLE_TS_VALIDATION: AtomicBool = AtomicBool::new(false);

const MIN_ADAPTIVE_UPDATE_INTERVAL: Duration = Duration::from_millis(500);
const ADAPTIVE_SHRINKING_PRESERVE: Duration = Duration::from_millis(100);
const ADAPTIVE_BLOCK_RECOVER_THRESHOLD: Duration = Duration::from_millis(200);
const ADAPTIVE_RECOVER_PER_SECOND: Duration = Duration::from_millis(20);
const ADAPTIVE_DELAY_BEFORE_RECOVERING: Duration = Duration::from_secs(5 * 60);
const SLOW_TIMESTAMP_FETCH: Duration = Duration::from_millis(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdaptiveUpdateIntervalState {
    None,
    Normal,
    Adapting,
    Recovering,
    Unadjustable,
}

#[derive(Debug)]
struct AdaptiveState {
    last_short_staleness_read: Option<SystemTime>,
    last_tick: SystemTime,
    state: AdaptiveUpdateIntervalState,
}

impl Default for AdaptiveState {
    fn default() -> Self {
        Self {
            last_short_staleness_read: None,
            last_tick: SystemTime::now(),
            state: AdaptiveUpdateIntervalState::None,
        }
    }
}

/// The timestamp operations that client-go's PD oracle needs from a PD client.
///
/// This deliberately stays separate from client-rust's region-routing `PdClient`
/// trait: the latter does not yet expose PD's all-keyspace minimum and external
/// timestamp RPCs. The eventual transport adapter belongs to the PD/tikvrpc
/// package receipts; the oracle owns the cache, staleness, and validation logic.
#[async_trait]
pub trait PdTimestampSource: Send + Sync + 'static {
    async fn get_timestamp(&self) -> OracleResult<(i64, i64)>;

    async fn get_min_timestamp(&self) -> OracleResult<(i64, i64)>;

    async fn set_external_timestamp(&self, timestamp: u64) -> OracleResult<()>;

    async fn get_external_timestamp(&self) -> OracleResult<u64>;
}

/// Adapts client-rust's region-routing PD client to the concrete oracle's
/// timestamp-source interface.
#[derive(Clone, Debug)]
pub struct PdClientTimestampSource<P: crate::pd::PdClient> {
    client: Arc<P>,
}

impl<P: crate::pd::PdClient> PdClientTimestampSource<P> {
    pub fn new(client: Arc<P>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl<P: crate::pd::PdClient> PdTimestampSource for PdClientTimestampSource<P> {
    async fn get_timestamp(&self) -> OracleResult<(i64, i64)> {
        let timestamp = self
            .client
            .clone()
            .get_timestamp()
            .await
            .map_err(|error| Box::new(error) as OracleError)?;
        Ok((timestamp.physical, timestamp.logical))
    }

    async fn get_min_timestamp(&self) -> OracleResult<(i64, i64)> {
        let timestamp = self
            .client
            .clone()
            .get_min_timestamp()
            .await
            .map_err(|error| Box::new(error) as OracleError)?;
        Ok((timestamp.physical, timestamp.logical))
    }

    async fn set_external_timestamp(&self, timestamp: u64) -> OracleResult<()> {
        self.client
            .clone()
            .set_external_timestamp(timestamp)
            .await
            .map_err(|error| Box::new(error) as OracleError)
    }

    async fn get_external_timestamp(&self) -> OracleResult<u64> {
        self.client
            .clone()
            .get_external_timestamp()
            .await
            .map_err(|error| Box::new(error) as OracleError)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PdOracleOptions {
    pub update_interval: Duration,
    pub no_update_timestamp: bool,
}

impl Default for PdOracleOptions {
    fn default() -> Self {
        Self {
            update_interval: Duration::from_secs(2),
            no_update_timestamp: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("updateInterval must be > 0")]
pub struct InvalidUpdateIntervalError;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PdOracleError {
    #[error("get low resolution timestamp fail, invalid txnScope = {0}")]
    InvalidLowResolutionScope(String),
    #[error("get low resolution timestamp async fail, invalid txnScope = {0}")]
    InvalidLowResolutionAsyncScope(String),
    #[error("get stale timestamp fail, txnScope: {0}")]
    MissingStaleScope(String),
    #[error("invalid prevSecond {0}")]
    InvalidPreviousSeconds(u64),
    #[error("MaxInt64 <= readTS < MaxUint64, readTS={0}")]
    InvalidReadTimestamp(u64),
    #[error("fail to validate read timestamp: {0}")]
    Validation(String),
}

#[derive(Clone, Copy, Debug)]
struct LastTimestamp {
    timestamp: u64,
    arrival: SystemTime,
}

struct PdOracleState {
    source: Arc<dyn PdTimestampSource>,
    last_timestamps: RwLock<HashMap<String, LastTimestamp>>,
    configured_update_interval: Mutex<Duration>,
    adaptive_update_interval: Mutex<Duration>,
    adaptive_state: Mutex<AdaptiveState>,
    validation_flights: tokio::sync::Mutex<HashMap<String, ValidationFlight>>,
    next_validation_flight_id: AtomicU64,
    shrink_sender: mpsc::Sender<Duration>,
    deferred_shrink_receiver: Mutex<Option<mpsc::Receiver<Duration>>>,
    close_notify: Notify,
    update_task: Mutex<Option<JoinHandle<()>>>,
    closed: AtomicBool,
}

type ValidationResult = Result<u64, String>;
type SharedValidationResult = Shared<BoxFuture<'static, ValidationResult>>;

#[derive(Clone)]
struct ValidationFlight {
    id: u64,
    result: SharedValidationResult,
}

/// An oracle backed by a PD timestamp source.
///
/// Construction is asynchronous because it seeds the global low-resolution
/// timestamp exactly as client-go does. The refresh scheduler and adaptive
/// interval controller are added below this core state machine.
#[derive(Clone)]
pub struct PdOracle {
    state: Arc<PdOracleState>,
}

impl std::fmt::Debug for PdOracle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("PdOracle").finish_non_exhaustive()
    }
}

impl PdOracle {
    pub async fn from_pd_client<P>(client: Arc<P>, options: PdOracleOptions) -> OracleResult<Self>
    where
        P: crate::pd::PdClient,
    {
        Self::new(Arc::new(PdClientTimestampSource::new(client)), options).await
    }

    pub async fn new<S>(source: Arc<S>, options: PdOracleOptions) -> OracleResult<Self>
    where
        S: PdTimestampSource,
    {
        if options.update_interval.is_zero() {
            return Err(Box::new(InvalidUpdateIntervalError));
        }
        let source: Arc<dyn PdTimestampSource> = source;
        let (shrink_sender, shrink_receiver) = mpsc::channel(1);
        let oracle = Self {
            state: Arc::new(PdOracleState {
                source,
                last_timestamps: RwLock::new(HashMap::new()),
                configured_update_interval: Mutex::new(options.update_interval),
                adaptive_update_interval: Mutex::new(options.update_interval),
                adaptive_state: Mutex::new(AdaptiveState::default()),
                validation_flights: tokio::sync::Mutex::new(HashMap::new()),
                next_validation_flight_id: AtomicU64::new(0),
                shrink_sender,
                deferred_shrink_receiver: Mutex::new(Some(shrink_receiver)),
                close_notify: Notify::new(),
                update_task: Mutex::new(None),
                closed: AtomicBool::new(false),
            }),
        };
        oracle.get_timestamp(&OracleOption::default()).await?;
        if !options.no_update_timestamp {
            oracle.start_low_resolution_timestamp_update_loop();
        }
        Ok(oracle)
    }

    fn scope(option: &OracleOption) -> &str {
        if option.txn_scope.is_empty() {
            super::GLOBAL_TXN_SCOPE
        } else {
            &option.txn_scope
        }
    }

    fn set_last_timestamp(&self, timestamp: u64, scope: &str) {
        let scope = if scope.is_empty() {
            super::GLOBAL_TXN_SCOPE
        } else {
            scope
        };
        let current = LastTimestamp {
            timestamp,
            arrival: SystemTime::now(),
        };
        let mut timestamps = self.state.last_timestamps.write().unwrap();
        match timestamps.get_mut(scope) {
            Some(last) if current.timestamp <= last.timestamp => {}
            Some(last) => {
                *last = LastTimestamp {
                    timestamp: current.timestamp,
                    arrival: last.arrival.max(current.arrival),
                };
            }
            None => {
                timestamps.insert(scope.to_owned(), current);
            }
        }
    }

    fn last_timestamp(&self, scope: &str) -> Option<LastTimestamp> {
        let scope = if scope.is_empty() {
            super::GLOBAL_TXN_SCOPE
        } else {
            scope
        };
        self.state
            .last_timestamps
            .read()
            .unwrap()
            .get(scope)
            .copied()
    }

    async fn fetch_timestamp(&self) -> OracleResult<u64> {
        let started = Instant::now();
        let (physical, logical) = self.state.source.get_timestamp().await?;
        let elapsed = started.elapsed();
        if elapsed > SLOW_TIMESTAMP_FETCH {
            log::warn!("get timestamp too slow: {elapsed:?}");
        }
        Ok(super::compose_timestamp(physical, logical))
    }

    async fn current_timestamp_for_validation(&self, option: OracleOption) -> OracleResult<u64> {
        let scope = Self::scope(&option).to_owned();
        let (flight, sender) = {
            let mut flights = self.state.validation_flights.lock().await;
            if let Some(flight) = flights.get(&scope) {
                (flight.clone(), None)
            } else {
                let id = self
                    .state
                    .next_validation_flight_id
                    .fetch_add(1, Ordering::AcqRel);
                let (sender, receiver) = oneshot::channel::<ValidationResult>();
                let result = async move {
                    receiver
                        .await
                        .unwrap_or_else(|error| Err(error.to_string()))
                }
                .boxed()
                .shared();
                let flight = ValidationFlight { id, result };
                flights.insert(scope.clone(), flight.clone());
                (flight, Some(sender))
            }
        };
        if let Some(sender) = sender {
            let oracle = self.clone();
            let source_option = option.clone();
            let scope = scope.clone();
            let flight_id = flight.id;
            tokio::spawn(async move {
                crate::stats::increment_validate_read_ts_from_pd();
                let result = oracle
                    .get_timestamp(&source_option)
                    .await
                    .map_err(|error| error.to_string());
                let _ = fail::eval("getCurrentTSForValidationBeforeReturn", |_| ());
                let mut flights = oracle.state.validation_flights.lock().await;
                if flights
                    .get(&scope)
                    .is_some_and(|current| current.id == flight_id)
                {
                    flights.remove(&scope);
                }
                drop(flights);
                let _ = sender.send(result);
            });
        }
        flight
            .result
            .clone()
            .await
            .map_err(|error| Box::new(PdOracleError::Validation(error)) as OracleError)
    }

    fn stale_timestamp_from_last(last: LastTimestamp, previous_seconds: u64) -> OracleResult<u64> {
        let physical_time = get_time_from_timestamp(last.timestamp);
        let physical_seconds = extract_physical(last.timestamp).div_euclid(1_000);
        if physical_seconds < 0 || physical_seconds as u64 <= previous_seconds {
            return Err(Box::new(PdOracleError::InvalidPreviousSeconds(
                previous_seconds,
            )));
        }
        let elapsed_since_arrival = signed_duration_nanoseconds(SystemTime::now(), last.arrival);
        let stale_time = add_wrapping_nanoseconds(
            physical_time,
            elapsed_since_arrival
                .wrapping_sub((previous_seconds as i64).wrapping_mul(1_000_000_000)),
        );
        Ok(system_time_to_timestamp(stale_time))
    }

    fn configured_update_interval(&self) -> Duration {
        *self.state.configured_update_interval.lock().unwrap()
    }

    fn adaptive_update_interval(&self) -> Duration {
        *self.state.adaptive_update_interval.lock().unwrap()
    }

    /// Start the deferred low-resolution refresh loop. This exists primarily
    /// for deterministic tests, matching client-go's `export_test.go` hook.
    pub fn start_low_resolution_timestamp_update_loop(&self) {
        let Some(shrink_receiver) = self.state.deferred_shrink_receiver.lock().unwrap().take()
        else {
            return;
        };
        self.start_update_loop(shrink_receiver);
    }

    fn start_update_loop(&self, mut shrink_receiver: mpsc::Receiver<Duration>) {
        let oracle = self.clone();
        let task = tokio::spawn(async move {
            let mut interval = oracle.adaptive_update_interval();
            let mut deadline = tokio::time::Instant::now() + interval;
            loop {
                if oracle.state.closed.load(Ordering::Acquire) {
                    return;
                }
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => {
                        let now = SystemTime::now();
                        let next_interval = oracle.next_update_interval(now, None);
                        oracle.refresh_timestamps(now).await;
                        if next_interval == interval {
                            deadline += interval;
                            let current = tokio::time::Instant::now();
                            if deadline <= current {
                                deadline = current + interval;
                            }
                        } else {
                            interval = next_interval;
                            deadline = tokio::time::Instant::now() + interval;
                        }
                    }
                    required = shrink_receiver.recv() => {
                        let Some(required) = required else { return; };
                        let now = SystemTime::now();
                        let next_interval = oracle.next_update_interval(now, Some(required));
                        if next_interval != interval {
                            interval = next_interval;
                            let last_tick = oracle.state.adaptive_state.lock().unwrap().last_tick;
                            if signed_duration_nanoseconds(now, last_tick)
                                >= interval.as_nanos().min(i64::MAX as u128) as i64
                            {
                                oracle.refresh_timestamps(SystemTime::now()).await;
                            }
                            deadline = tokio::time::Instant::now() + interval;
                        }
                    }
                    _ = oracle.state.close_notify.notified() => return,
                }
            }
        });
        *self.state.update_task.lock().unwrap() = Some(task);
    }

    async fn refresh_timestamps(&self, tick_time: SystemTime) {
        let scopes: Vec<String> = self
            .state
            .last_timestamps
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        for scope in scopes {
            if let Ok(timestamp) = self.fetch_timestamp().await {
                self.set_last_timestamp(timestamp, &scope);
            }
        }
        self.state.adaptive_state.lock().unwrap().last_tick = tick_time;
    }

    fn next_update_interval(
        &self,
        now: SystemTime,
        required_staleness: Option<Duration>,
    ) -> Duration {
        let configured = self.configured_update_interval();
        let mut adaptive = self.state.adaptive_update_interval.lock().unwrap();
        let mut state = self.state.adaptive_state.lock().unwrap();
        let current = *adaptive;
        let next = if configured <= MIN_ADAPTIVE_UPDATE_INTERVAL {
            state.state = AdaptiveUpdateIntervalState::Unadjustable;
            configured
        } else if let Some(required) = required_staleness
            .filter(|required| *required < current && current > MIN_ADAPTIVE_UPDATE_INTERVAL)
        {
            state.state = AdaptiveUpdateIntervalState::Adapting;
            required
                .checked_sub(ADAPTIVE_SHRINKING_PRESERVE)
                .unwrap_or(Duration::ZERO)
                .max(MIN_ADAPTIVE_UPDATE_INTERVAL)
        } else if current != configured
            && state.last_short_staleness_read.is_some_and(|last| {
                signed_duration_nanoseconds(now, last)
                    < ADAPTIVE_DELAY_BEFORE_RECOVERING
                        .as_nanos()
                        .min(i64::MAX as u128) as i64
            })
        {
            state.state = AdaptiveUpdateIntervalState::Adapting;
            current
        } else if current == configured {
            state.state = AdaptiveUpdateIntervalState::Normal;
            current
        } else {
            let elapsed_seconds =
                signed_duration_nanoseconds(now, state.last_tick).max(0) as f64 / 1_000_000_000.0;
            let growth_nanoseconds =
                (elapsed_seconds * ADAPTIVE_RECOVER_PER_SECOND.as_nanos() as f64) as u64;
            let recovered = current
                .saturating_add(Duration::from_nanos(growth_nanoseconds))
                .min(configured);
            state.state = if recovered == configured {
                AdaptiveUpdateIntervalState::Normal
            } else {
                AdaptiveUpdateIntervalState::Recovering
            };
            recovered
        };
        *adaptive = next;
        crate::stats::set_low_resolution_tso_update_interval(next);
        next
    }

    fn adjust_update_interval_for_staleness(
        &self,
        read_timestamp: u64,
        current_timestamp: u64,
        now: SystemTime,
    ) {
        let required_millis =
            extract_physical(current_timestamp).wrapping_sub(extract_physical(read_timestamp));
        let current = self.adaptive_update_interval();
        let current_millis = current.as_millis().min(i64::MAX as u128) as i64;
        if required_millis
            <= current_millis.saturating_add(
                ADAPTIVE_BLOCK_RECOVER_THRESHOLD
                    .as_millis()
                    .min(i64::MAX as u128) as i64,
            )
        {
            self.state
                .adaptive_state
                .lock()
                .unwrap()
                .last_short_staleness_read = Some(now);
        }
        if required_millis <= current_millis && current > MIN_ADAPTIVE_UPDATE_INTERVAL {
            let required = Duration::from_millis(required_millis.max(1) as u64);
            let _ = self.state.shrink_sender.try_send(required);
        }
    }

    #[cfg(test)]
    fn set_last_timestamp_for_test(&self, timestamp: u64, scope: &str) {
        self.set_last_timestamp(timestamp, scope);
    }

    #[cfg(test)]
    fn take_shrink_request_for_test(&self) -> Option<Duration> {
        self.state
            .deferred_shrink_receiver
            .lock()
            .unwrap()
            .as_mut()?
            .try_recv()
            .ok()
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("timestamp future was already waited")]
struct TimestampFutureAlreadyWaited;

struct PdFuture(Mutex<Option<JoinHandle<OracleResult<u64>>>>);

struct AbortTaskOnDrop<T>(Option<JoinHandle<T>>);

impl<T> Drop for AbortTaskOnDrop<T> {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

impl Drop for PdFuture {
    fn drop(&mut self) {
        if let Some(task) = self.0.lock().unwrap().take() {
            task.abort();
        }
    }
}

#[async_trait]
impl TimestampFuture for PdFuture {
    async fn wait(&self) -> OracleResult<u64> {
        let started = Instant::now();
        let task = self
            .0
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| Box::new(TimestampFutureAlreadyWaited) as OracleError)?;
        let mut task = AbortTaskOnDrop(Some(task));
        let result = task.0.as_mut().unwrap().await;
        task.0.take();
        crate::stats::observe_tso_future_wait(started.elapsed());
        result.map_err(|error| {
            Box::new(PdOracleError::Validation(error.to_string())) as OracleError
        })?
    }
}

#[async_trait]
impl ReadTimestampValidator for PdOracle {
    async fn validate_read_timestamp(
        &self,
        read_timestamp: u64,
        is_stale_read: bool,
        option: &OracleOption,
    ) -> OracleResult<()> {
        if !ENABLE_TS_VALIDATION.load(Ordering::Acquire) {
            return Ok(());
        }
        if read_timestamp >= i64::MAX as u64 && read_timestamp < u64::MAX {
            return Err(Box::new(PdOracleError::InvalidReadTimestamp(
                read_timestamp,
            )));
        }
        if read_timestamp == u64::MAX {
            if is_stale_read {
                return Err(Box::new(super::LatestStaleReadError));
            }
            return Ok(());
        }

        let scope = Self::scope(option);
        let mut retrying = false;
        loop {
            let last = self.last_timestamp(scope);
            if last.is_none_or(|last| read_timestamp > last.timestamp) {
                let current = self
                    .current_timestamp_for_validation(option.clone())
                    .await?;
                if is_stale_read && !retrying {
                    self.adjust_update_interval_for_staleness(
                        read_timestamp,
                        current,
                        SystemTime::now(),
                    );
                }
                if read_timestamp > current {
                    let skip_retry = fail::eval("validateReadTSRetryGetTS", |value| {
                        value.as_deref() == Some("skip")
                    })
                    .unwrap_or(false);
                    if !retrying && !skip_retry {
                        retrying = true;
                        continue;
                    }
                    return Err(Box::new(super::FutureTimestampReadError {
                        read_timestamp,
                        current_timestamp: current,
                    }));
                }
            } else if is_stale_read && !retrying {
                if let Ok(estimated_current) = Self::stale_timestamp_from_last(last.unwrap(), 0) {
                    self.adjust_update_interval_for_staleness(
                        read_timestamp,
                        estimated_current,
                        SystemTime::now(),
                    );
                }
            }
            return Ok(());
        }
    }
}

#[async_trait]
impl Oracle for PdOracle {
    async fn get_timestamp(&self, option: &OracleOption) -> OracleResult<u64> {
        let timestamp = self.fetch_timestamp().await?;
        self.set_last_timestamp(timestamp, Self::scope(option));
        Ok(timestamp)
    }

    fn get_timestamp_async(&self, option: &OracleOption) -> Box<dyn TimestampFuture> {
        let oracle = self.clone();
        let option = option.clone();
        let task = tokio::spawn(async move { oracle.get_timestamp(&option).await });
        Box::new(PdFuture(Mutex::new(Some(task))))
    }

    async fn get_low_resolution_timestamp(&self, option: &OracleOption) -> OracleResult<u64> {
        self.last_timestamp(Self::scope(option))
            .map(|last| last.timestamp)
            .ok_or_else(|| {
                Box::new(PdOracleError::InvalidLowResolutionScope(
                    option.txn_scope.clone(),
                )) as OracleError
            })
    }

    fn get_low_resolution_timestamp_async(
        &self,
        option: &OracleOption,
    ) -> Box<dyn TimestampFuture> {
        Box::new(LowResolutionFuture(
            self.last_timestamp(Self::scope(option))
                .map(|last| last.timestamp)
                .ok_or_else(|| {
                    PdOracleError::InvalidLowResolutionAsyncScope(option.txn_scope.clone())
                }),
        ))
    }

    fn set_low_resolution_timestamp_update_interval(
        &self,
        update_interval: Duration,
    ) -> OracleResult<()> {
        if update_interval.is_zero() {
            return Err(Box::new(InvalidUpdateIntervalError));
        }
        let mut configured = self.state.configured_update_interval.lock().unwrap();
        let mut adaptive = self.state.adaptive_update_interval.lock().unwrap();
        let previous = *configured;
        *configured = update_interval;
        if *adaptive == previous || update_interval < *adaptive {
            *adaptive = update_interval;
        }
        Ok(())
    }

    async fn get_stale_timestamp(
        &self,
        transaction_scope: &str,
        previous_seconds: u64,
    ) -> OracleResult<u64> {
        match self.last_timestamp(transaction_scope) {
            Some(last) => Self::stale_timestamp_from_last(last, previous_seconds),
            None => {
                let initial = Box::new(PdOracleError::MissingStaleScope(
                    transaction_scope.to_owned(),
                ));
                let _ = self
                    .get_timestamp(&OracleOption {
                        txn_scope: transaction_scope.to_owned(),
                    })
                    .await?;
                Err(initial)
            }
        }
    }

    fn is_expired(&self, lock_timestamp: u64, ttl: u64, option: &OracleOption) -> bool {
        self.last_timestamp(Self::scope(option)).is_none_or(|last| {
            extract_physical(last.timestamp)
                >= extract_physical(lock_timestamp).wrapping_add(ttl as i64)
        })
    }

    fn until_expired(&self, lock_timestamp: u64, ttl: u64, option: &OracleOption) -> i64 {
        self.last_timestamp(Self::scope(option))
            .map(|last| {
                extract_physical(lock_timestamp)
                    .wrapping_add(ttl as i64)
                    .wrapping_sub(extract_physical(last.timestamp))
            })
            .unwrap_or(0)
    }

    fn close(&self) {
        if !self.state.closed.swap(true, Ordering::AcqRel) {
            self.state.close_notify.notify_one();
        }
    }

    async fn get_external_timestamp(&self) -> OracleResult<u64> {
        self.state.source.get_external_timestamp().await
    }

    async fn set_external_timestamp(&self, timestamp: u64) -> OracleResult<()> {
        self.state.source.set_external_timestamp(timestamp).await
    }

    async fn get_all_tso_keyspace_group_min_timestamp(&self) -> OracleResult<u64> {
        let started = Instant::now();
        let (physical, logical) = self.state.source.get_min_timestamp().await?;
        let elapsed = started.elapsed();
        if elapsed > SLOW_TIMESTAMP_FETCH {
            log::warn!("get minimum timestamp too slow: {elapsed:?}");
        }
        Ok(super::compose_timestamp(physical, logical))
    }
}

struct LowResolutionFuture(Result<u64, PdOracleError>);

#[async_trait]
impl TimestampFuture for LowResolutionFuture {
    async fn wait(&self) -> OracleResult<u64> {
        match &self.0 {
            Ok(timestamp) => Ok(*timestamp),
            Err(error) => Err(Box::new(error.clone())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("stopped")]
pub struct StoppedError;

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExternalTimestampError {
    #[error("external timestamp is greater than global tso")]
    GreaterThanGlobalTimestamp,
    #[error("cannot decrease the external timestamp")]
    Decrease,
}

#[derive(Debug, Default)]
struct LocalExternalTimestamp(AtomicU64);

impl LocalExternalTimestamp {
    fn get(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    fn set(&self, new_timestamp: u64, current_timestamp: u64) -> OracleResult<()> {
        if new_timestamp > current_timestamp {
            return Err(Box::new(ExternalTimestampError::GreaterThanGlobalTimestamp));
        }
        loop {
            let external_timestamp = self.0.load(Ordering::Acquire);
            if external_timestamp > new_timestamp {
                return Err(Box::new(ExternalTimestampError::Decrease));
            }
            if external_timestamp == new_timestamp {
                return Ok(());
            }
            if self
                .0
                .compare_exchange(
                    external_timestamp,
                    new_timestamp,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Ok(());
            }
        }
    }
}

#[derive(Debug, Default)]
struct LocalState {
    last_timestamp: u64,
    logical: u64,
    current_time_hook: Option<SystemTime>,
}

#[derive(Clone, Debug, Default)]
pub struct LocalOracle {
    state: Arc<Mutex<LocalState>>,
    external_timestamp: Arc<LocalExternalTimestamp>,
}

impl LocalOracle {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    fn set_current_time(&self, current_time: SystemTime) {
        self.state.lock().unwrap().current_time_hook = Some(current_time);
    }

    fn current_time(state: &LocalState) -> SystemTime {
        state.current_time_hook.unwrap_or_else(SystemTime::now)
    }
}

struct LocalFuture(LocalOracle);

#[async_trait]
impl TimestampFuture for LocalFuture {
    async fn wait(&self) -> OracleResult<u64> {
        self.0.get_timestamp(&OracleOption::default()).await
    }
}

#[async_trait]
impl ReadTimestampValidator for LocalOracle {
    async fn validate_read_timestamp(
        &self,
        _read_timestamp: u64,
        _is_stale_read: bool,
        _option: &OracleOption,
    ) -> OracleResult<()> {
        Ok(())
    }
}

#[async_trait]
impl Oracle for LocalOracle {
    async fn get_timestamp(&self, _option: &OracleOption) -> OracleResult<u64> {
        let mut state = self.state.lock().unwrap();
        let timestamp = system_time_to_timestamp(Self::current_time(&state));
        if state.last_timestamp == timestamp {
            state.logical = state.logical.wrapping_add(1);
            return Ok(timestamp.wrapping_add(state.logical));
        }
        state.last_timestamp = timestamp;
        state.logical = 0;
        Ok(timestamp)
    }

    fn get_timestamp_async(&self, _option: &OracleOption) -> Box<dyn TimestampFuture> {
        Box::new(LocalFuture(self.clone()))
    }

    async fn get_low_resolution_timestamp(&self, option: &OracleOption) -> OracleResult<u64> {
        self.get_timestamp(option).await
    }

    fn get_low_resolution_timestamp_async(
        &self,
        option: &OracleOption,
    ) -> Box<dyn TimestampFuture> {
        self.get_timestamp_async(option)
    }

    fn set_low_resolution_timestamp_update_interval(
        &self,
        _update_interval: Duration,
    ) -> OracleResult<()> {
        Ok(())
    }

    async fn get_stale_timestamp(
        &self,
        _transaction_scope: &str,
        previous_seconds: u64,
    ) -> OracleResult<u64> {
        Ok(system_time_to_timestamp(subtract_wrapping_seconds(
            SystemTime::now(),
            previous_seconds,
        )))
    }

    fn is_expired(&self, lock_timestamp: u64, ttl: u64, _option: &OracleOption) -> bool {
        let state = self.state.lock().unwrap();
        let now = Self::current_time(&state);
        let expiration = add_wrapping_millis(get_time_from_timestamp(lock_timestamp), ttl);
        now >= expiration
    }

    fn until_expired(&self, lock_timestamp: u64, ttl: u64, _option: &OracleOption) -> i64 {
        let state = self.state.lock().unwrap();
        extract_physical(lock_timestamp)
            .wrapping_add(ttl as i64)
            .wrapping_sub(get_physical(Self::current_time(&state)))
    }

    fn close(&self) {}

    async fn get_external_timestamp(&self) -> OracleResult<u64> {
        Ok(self.external_timestamp.get())
    }

    async fn set_external_timestamp(&self, timestamp: u64) -> OracleResult<()> {
        let current = self.get_timestamp(&OracleOption::default()).await?;
        self.external_timestamp.set(timestamp, current)
    }

    async fn get_all_tso_keyspace_group_min_timestamp(&self) -> OracleResult<u64> {
        let state = self.state.lock().unwrap();
        Ok(system_time_to_timestamp(Self::current_time(&state)))
    }
}

#[derive(Debug, Default)]
struct MockState {
    stopped: bool,
    offset_nanoseconds: i64,
    last_timestamp: u64,
}

#[derive(Clone, Debug, Default)]
pub struct MockOracle {
    state: Arc<RwLock<MockState>>,
    external_timestamp: Arc<LocalExternalTimestamp>,
}

impl MockOracle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enable(&self) {
        self.state.write().unwrap().stopped = false;
    }

    pub fn disable(&self) {
        self.state.write().unwrap().stopped = true;
    }

    /// Add a signed millisecond offset to the mock clock.
    pub fn add_offset_milliseconds(&self, offset: i64) {
        self.add_offset_nanoseconds(offset.wrapping_mul(1_000_000));
    }

    /// Add a signed nanosecond offset to the mock clock.
    pub fn add_offset_nanoseconds(&self, offset: i64) {
        let mut state = self.state.write().unwrap();
        state.offset_nanoseconds = state.offset_nanoseconds.wrapping_add(offset);
    }

    fn now(state: &MockState) -> SystemTime {
        add_wrapping_nanoseconds(SystemTime::now(), state.offset_nanoseconds)
    }
}

struct MockFuture(MockOracle);

#[async_trait]
impl TimestampFuture for MockFuture {
    async fn wait(&self) -> OracleResult<u64> {
        self.0.get_timestamp(&OracleOption::default()).await
    }
}

#[async_trait]
impl ReadTimestampValidator for MockOracle {
    async fn validate_read_timestamp(
        &self,
        _read_timestamp: u64,
        _is_stale_read: bool,
        _option: &OracleOption,
    ) -> OracleResult<()> {
        Ok(())
    }
}

#[async_trait]
impl Oracle for MockOracle {
    async fn get_timestamp(&self, _option: &OracleOption) -> OracleResult<u64> {
        let mut state = self.state.write().unwrap();
        if state.stopped {
            return Err(Box::new(StoppedError));
        }
        let mut timestamp = system_time_to_timestamp(Self::now(&state));
        if extract_physical(state.last_timestamp) == extract_physical(timestamp) {
            timestamp = state.last_timestamp.wrapping_add(1);
        }
        state.last_timestamp = timestamp;
        Ok(timestamp)
    }

    fn get_timestamp_async(&self, _option: &OracleOption) -> Box<dyn TimestampFuture> {
        Box::new(MockFuture(self.clone()))
    }

    async fn get_low_resolution_timestamp(&self, option: &OracleOption) -> OracleResult<u64> {
        self.get_timestamp(option).await
    }

    fn get_low_resolution_timestamp_async(
        &self,
        option: &OracleOption,
    ) -> Box<dyn TimestampFuture> {
        self.get_timestamp_async(option)
    }

    fn set_low_resolution_timestamp_update_interval(
        &self,
        _update_interval: Duration,
    ) -> OracleResult<()> {
        Ok(())
    }

    async fn get_stale_timestamp(
        &self,
        _transaction_scope: &str,
        previous_seconds: u64,
    ) -> OracleResult<u64> {
        Ok(system_time_to_timestamp(subtract_wrapping_seconds(
            SystemTime::now(),
            previous_seconds,
        )))
    }

    fn is_expired(&self, lock_timestamp: u64, ttl: u64, _option: &OracleOption) -> bool {
        let state = self.state.read().unwrap();
        let expiration = add_wrapping_millis(get_time_from_timestamp(lock_timestamp), ttl);
        Self::now(&state) >= expiration
    }

    fn until_expired(&self, lock_timestamp: u64, ttl: u64, _option: &OracleOption) -> i64 {
        let state = self.state.read().unwrap();
        let expiration = add_wrapping_millis(get_time_from_timestamp(lock_timestamp), ttl);
        signed_duration_between(expiration, Self::now(&state))
    }

    fn close(&self) {}

    async fn get_external_timestamp(&self) -> OracleResult<u64> {
        Ok(self.external_timestamp.get())
    }

    async fn set_external_timestamp(&self, timestamp: u64) -> OracleResult<()> {
        let current = self.get_timestamp(&OracleOption::default()).await?;
        self.external_timestamp.set(timestamp, current)
    }

    async fn get_all_tso_keyspace_group_min_timestamp(&self) -> OracleResult<u64> {
        let state = self.state.read().unwrap();
        if state.stopped {
            return Err(Box::new(StoppedError));
        }
        Ok(system_time_to_timestamp(Self::now(&state)))
    }
}

fn add_signed_millis(time: SystemTime, milliseconds: i64) -> SystemTime {
    let duration = Duration::from_millis(milliseconds.unsigned_abs());
    if milliseconds >= 0 {
        time.checked_add(duration)
    } else {
        time.checked_sub(duration)
    }
    .expect("mock timestamp is outside the range supported by SystemTime")
}

fn add_wrapping_millis(time: SystemTime, milliseconds: u64) -> SystemTime {
    add_wrapping_nanoseconds(time, (milliseconds as i64).wrapping_mul(1_000_000))
}

fn subtract_wrapping_seconds(time: SystemTime, seconds: u64) -> SystemTime {
    add_wrapping_nanoseconds(time, (seconds as i64).wrapping_mul(-1_000_000_000))
}

fn signed_duration_between(later: SystemTime, earlier: SystemTime) -> i64 {
    signed_duration_nanoseconds(later, earlier) / 1_000_000
}

fn signed_duration_nanoseconds(later: SystemTime, earlier: SystemTime) -> i64 {
    match later.duration_since(earlier) {
        Ok(duration) => duration.as_nanos().min(i64::MAX as u128) as i64,
        Err(error) => {
            let nanoseconds = error.duration().as_nanos();
            if nanoseconds >= (i64::MAX as u128) + 1 {
                i64::MIN
            } else {
                -(nanoseconds as i64)
            }
        }
    }
}

fn _assert_error_is_send_sync(_: OracleError) {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize};
    use std::time::UNIX_EPOCH;

    use serial_test::serial;

    use super::*;

    #[derive(Debug)]
    struct TestPdSource {
        physical: i64,
        logical: AtomicI64,
        external: AtomicU64,
    }

    impl TestPdSource {
        fn new(physical: i64) -> Self {
            Self {
                physical,
                logical: AtomicI64::new(0),
                external: AtomicU64::new(0),
            }
        }
    }

    #[async_trait]
    impl PdTimestampSource for TestPdSource {
        async fn get_timestamp(&self) -> OracleResult<(i64, i64)> {
            Ok((
                self.physical,
                self.logical.fetch_add(1, Ordering::AcqRel) + 1,
            ))
        }

        async fn get_min_timestamp(&self) -> OracleResult<(i64, i64)> {
            Ok((self.physical - 1, 7))
        }

        async fn set_external_timestamp(&self, timestamp: u64) -> OracleResult<()> {
            self.external.store(timestamp, Ordering::Release);
            Ok(())
        }

        async fn get_external_timestamp(&self) -> OracleResult<u64> {
            Ok(self.external.load(Ordering::Acquire))
        }
    }

    #[derive(Debug)]
    struct GatePdSource {
        logical: AtomicI64,
        calls: AtomicUsize,
        completed: AtomicUsize,
        block: AtomicBool,
        release: Notify,
    }

    impl GatePdSource {
        fn new() -> Self {
            Self {
                logical: AtomicI64::new(0),
                calls: AtomicUsize::new(0),
                completed: AtomicUsize::new(0),
                block: AtomicBool::new(false),
                release: Notify::new(),
            }
        }
    }

    #[async_trait]
    impl PdTimestampSource for GatePdSource {
        async fn get_timestamp(&self) -> OracleResult<(i64, i64)> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            // PD allocates the timestamp before a delayed response reaches the
            // caller. Capturing it here lets the tests model an older
            // singleflight result while another client advances PD.
            let logical = self.logical.fetch_add(1, Ordering::AcqRel) + 1;
            if self.block.load(Ordering::Acquire) {
                self.release.notified().await;
            }
            self.completed.fetch_add(1, Ordering::AcqRel);
            Ok((1_700_000_000_000, logical))
        }

        async fn get_min_timestamp(&self) -> OracleResult<(i64, i64)> {
            self.get_timestamp().await
        }

        async fn set_external_timestamp(&self, _timestamp: u64) -> OracleResult<()> {
            Ok(())
        }

        async fn get_external_timestamp(&self) -> OracleResult<u64> {
            Ok(0)
        }
    }

    async fn wait_for_low_resolution_change(oracle: &PdOracle, check_frequency: Duration) {
        let option = OracleOption::default();
        let current = oracle.get_low_resolution_timestamp(&option).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::time::sleep(check_frequency).await;
                if oracle.get_low_resolution_timestamp(&option).await.unwrap() > current {
                    break;
                }
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn source_test_local_oracle() {
        let oracle = LocalOracle::new();
        let fixed = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
        oracle.set_current_time(fixed);
        let first = oracle
            .get_timestamp(&OracleOption::default())
            .await
            .unwrap();
        let second = oracle
            .get_timestamp(&OracleOption::default())
            .await
            .unwrap();
        let third = oracle
            .get_timestamp_async(&OracleOption::default())
            .wait()
            .await
            .unwrap();
        assert_eq!(first, system_time_to_timestamp(fixed));
        assert_eq!(second, first + 1);
        assert_eq!(third, first + 2);
        assert_eq!(
            oracle
                .get_all_tso_keyspace_group_min_timestamp()
                .await
                .unwrap(),
            first
        );

        let mut generated = std::collections::HashSet::new();
        for _ in 0..100_000 {
            generated.insert(
                oracle
                    .get_timestamp(&OracleOption::default())
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(generated.len(), 100_000);
    }

    #[test]
    fn source_test_is_expired() {
        let oracle = LocalOracle::new();
        let now = UNIX_EPOCH + Duration::from_millis(10_000);
        oracle.set_current_time(now);
        let lock = system_time_to_timestamp(now);
        oracle.set_current_time(now + Duration::from_millis(10));
        assert!(oracle.is_expired(lock, 5, &OracleOption::default()));
        assert!(!oracle.is_expired(lock, 200, &OracleOption::default()));
    }

    #[test]
    fn source_test_local_oracle_until_expired() {
        let oracle = LocalOracle::new();
        let start = UNIX_EPOCH + Duration::from_millis(10_000);
        oracle.set_current_time(start);
        let lock = system_time_to_timestamp(start);
        oracle.set_current_time(start + Duration::from_millis(10));
        assert_eq!(oracle.until_expired(lock, 6, &OracleOption::default()), -4);
        assert_eq!(oracle.until_expired(lock, 14, &OracleOption::default()), 4);
    }

    #[tokio::test]
    async fn external_timestamp_is_monotonic_and_bounded_by_tso() {
        let oracle = LocalOracle::new();
        let fixed = UNIX_EPOCH + Duration::from_millis(10_000);
        oracle.set_current_time(fixed);
        let current = system_time_to_timestamp(fixed);
        oracle.set_external_timestamp(current).await.unwrap();
        oracle.set_external_timestamp(current).await.unwrap();
        assert_eq!(oracle.get_external_timestamp().await.unwrap(), current);
        assert!(oracle.set_external_timestamp(current - 1).await.is_err());
        assert!(oracle.set_external_timestamp(current + 10).await.is_err());
    }

    #[tokio::test]
    async fn source_uncovered_go_duration_overflow_matches_local_and_mock_oracles() {
        let local = LocalOracle::new();
        let fixed = UNIX_EPOCH + Duration::from_secs(10);
        local.set_current_time(fixed);
        let lock = system_time_to_timestamp(fixed);
        assert!(local.is_expired(lock, i64::MAX as u64, &OracleOption::default()));

        let stale = local
            .get_stale_timestamp(super::super::GLOBAL_TXN_SCOPE, u64::MAX)
            .await
            .unwrap();
        let stale_time = get_time_from_timestamp(stale);
        assert!(
            signed_duration_between(stale_time, SystemTime::now() + Duration::from_secs(1))
                .unsigned_abs()
                <= 100
        );

        let mock = MockOracle::new();
        let lock = system_time_to_timestamp(SystemTime::now());
        let remaining = mock.until_expired(lock, i64::MAX as u64, &OracleOption::default());
        assert!((-5..=0).contains(&remaining), "remaining={remaining}");
    }

    #[tokio::test]
    async fn mock_oracle_stop_offset_and_monotonicity() {
        let oracle = MockOracle::new();
        oracle.add_offset_nanoseconds(123);
        oracle.add_offset_milliseconds(-2);
        assert_eq!(oracle.state.read().unwrap().offset_nanoseconds, -1_999_877);
        let first = oracle
            .get_timestamp(&OracleOption::default())
            .await
            .unwrap();
        let second = oracle
            .get_timestamp(&OracleOption::default())
            .await
            .unwrap();
        assert!(second > first);
        oracle.add_offset_milliseconds(2_000);
        let shifted = oracle
            .get_timestamp(&OracleOption::default())
            .await
            .unwrap();
        assert!(extract_physical(shifted) >= extract_physical(second) + 1_900);
        oracle.disable();
        assert!(oracle
            .get_timestamp(&OracleOption::default())
            .await
            .is_err());
        assert!(oracle
            .get_all_tso_keyspace_group_min_timestamp()
            .await
            .is_err());
        oracle.enable();
        assert!(oracle.get_timestamp(&OracleOption::default()).await.is_ok());
    }

    #[tokio::test]
    async fn source_test_pd_oracle_until_expired() {
        let oracle = PdOracle::new(
            Arc::new(TestPdSource::new(1_700_000_000_000)),
            PdOracleOptions {
                no_update_timestamp: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let start = SystemTime::now();
        let start_timestamp = system_time_to_timestamp(start);
        oracle.set_last_timestamp_for_test(start_timestamp, super::super::GLOBAL_TXN_SCOPE);
        let lock_timestamp = system_time_to_timestamp(start + Duration::from_millis(10)) + 1;
        assert_eq!(
            oracle.until_expired(
                lock_timestamp,
                15,
                &OracleOption {
                    txn_scope: super::super::GLOBAL_TXN_SCOPE.to_owned(),
                },
            ),
            25
        );
        oracle.close();
    }

    #[tokio::test]
    async fn source_test_pd_oracle_get_stale_timestamp() {
        let oracle = PdOracle::new(
            Arc::new(TestPdSource::new(1_700_000_000_000)),
            PdOracleOptions {
                no_update_timestamp: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let start = SystemTime::now();
        oracle.set_last_timestamp_for_test(
            system_time_to_timestamp(start),
            super::super::GLOBAL_TXN_SCOPE,
        );
        let timestamp = oracle
            .get_stale_timestamp(super::super::GLOBAL_TXN_SCOPE, 10)
            .await
            .unwrap();
        let expected = start - Duration::from_secs(10);
        let observed = get_time_from_timestamp(timestamp);
        assert!(
            signed_duration_between(observed, expected).unsigned_abs()
                <= Duration::from_secs(2).as_millis() as u64
        );
        for previous_seconds in [1_000_000_000_000, u64::MAX] {
            let error = oracle
                .get_stale_timestamp(super::super::GLOBAL_TXN_SCOPE, previous_seconds)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("invalid prevSecond"));
        }
        oracle.close();
    }

    #[tokio::test]
    async fn source_test_pd_oracle_set_low_resolution_timestamp_update_interval() {
        let oracle = PdOracle::new(
            Arc::new(TestPdSource::new(1_700_000_000_000)),
            PdOracleOptions {
                update_interval: Duration::from_millis(50),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();
        let option = OracleOption::default();
        let low_resolution = oracle.get_low_resolution_timestamp(&option).await.unwrap();
        let fresh = oracle.get_timestamp(&option).await.unwrap();
        assert!(fresh > low_resolution);

        oracle.start_low_resolution_timestamp_update_loop();
        for update_interval in [
            Duration::from_millis(50),
            Duration::from_millis(150),
            Duration::from_millis(500),
        ] {
            oracle
                .set_low_resolution_timestamp_update_interval(update_interval)
                .unwrap();
            let started = Instant::now();
            wait_for_low_resolution_change(&oracle, Duration::from_millis(10)).await;
            wait_for_low_resolution_change(&oracle, Duration::from_millis(10)).await;
            let elapsed = started.elapsed();
            assert!(
                elapsed > update_interval,
                "{elapsed:?} <= {update_interval:?}"
            );
            assert!(
                elapsed <= update_interval.saturating_mul(3),
                "{elapsed:?} > 3 * {update_interval:?}"
            );
        }
        oracle.close();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !oracle
                .state
                .update_task
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .is_finished()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn source_test_non_future_stale_tso() {
        let oracle = PdOracle::new(
            Arc::new(TestPdSource::new(1_700_000_000_000)),
            PdOracleOptions {
                no_update_timestamp: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        oracle.set_last_timestamp_for_test(
            system_time_to_timestamp(SystemTime::now()),
            super::super::GLOBAL_TXN_SCOPE,
        );
        for iteration in 0..100 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let now = SystemTime::now();
            let upper_bound = now + Duration::from_millis(5);
            let updater = {
                let oracle = oracle.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_micros(100)).await;
                    oracle.set_last_timestamp_for_test(
                        system_time_to_timestamp(now),
                        super::super::GLOBAL_TXN_SCOPE,
                    );
                })
            };
            while !updater.is_finished() {
                let timestamp = oracle
                    .get_stale_timestamp(super::super::GLOBAL_TXN_SCOPE, 0)
                    .await
                    .unwrap();
                let stale_time = get_time_from_timestamp(timestamp);
                if now.elapsed().unwrap_or_default() < Duration::from_millis(1) {
                    assert!(
                        stale_time < upper_bound,
                        "iteration {iteration}: {stale_time:?} >= {upper_bound:?}"
                    );
                }
                tokio::task::yield_now().await;
            }
            updater.await.unwrap();
        }
        oracle.close();
    }

    #[tokio::test]
    async fn pd_oracle_seeds_scopes_and_preserves_core_contracts() {
        let source = Arc::new(TestPdSource::new(1_700_000_000_000));
        let oracle = PdOracle::new(
            source.clone(),
            PdOracleOptions {
                update_interval: Duration::from_secs(2),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();
        let global = OracleOption::default();
        let seeded = oracle.get_low_resolution_timestamp(&global).await.unwrap();
        let fetched = oracle.get_timestamp(&global).await.unwrap();
        assert!(fetched > seeded);
        let forced = super::super::compose_timestamp(1_700_000_000_001, 9);
        oracle.set_last_timestamp_for_test(forced, "test-scope");
        assert_eq!(
            oracle
                .get_low_resolution_timestamp(&OracleOption {
                    txn_scope: "test-scope".to_owned(),
                })
                .await
                .unwrap(),
            forced
        );
        assert!(oracle.is_expired(seeded, 0, &global));
        assert_eq!(oracle.until_expired(fetched, 15, &global), 15);
        assert!(oracle
            .get_low_resolution_timestamp(&OracleOption {
                txn_scope: "missing".to_owned(),
            })
            .await
            .is_err());
        assert!(oracle.get_stale_timestamp("missing", 1).await.is_err());
        assert!(oracle
            .get_low_resolution_timestamp(&OracleOption {
                txn_scope: "missing".to_owned(),
            })
            .await
            .is_ok());
        let minimum = oracle
            .get_all_tso_keyspace_group_min_timestamp()
            .await
            .unwrap();
        assert_eq!(
            minimum,
            super::super::compose_timestamp(1_699_999_999_999, 7)
        );
        oracle.set_external_timestamp(42).await.unwrap();
        assert_eq!(oracle.get_external_timestamp().await.unwrap(), 42);
        assert!(oracle
            .set_low_resolution_timestamp_update_interval(Duration::ZERO)
            .is_err());
        oracle.close();
    }

    #[tokio::test]
    async fn source_test_adaptive_update_ts_interval() {
        let oracle = PdOracle::new(
            Arc::new(TestPdSource::new(1_700_000_000_000)),
            PdOracleOptions {
                update_interval: Duration::from_secs(2),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();
        let mut now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let timestamp_before = |now: SystemTime, before: Duration| {
            super::super::compose_timestamp(get_physical(now - before), 1)
        };
        let state = || oracle.state.adaptive_state.lock().unwrap().state;

        now += Duration::from_secs(2);
        assert_eq!(
            oracle.next_update_interval(now, None),
            Duration::from_secs(2)
        );
        now += Duration::from_secs(2);
        assert_eq!(
            oracle.next_update_interval(now, None),
            Duration::from_secs(2)
        );
        assert_eq!(state(), AdaptiveUpdateIntervalState::Normal);

        now += Duration::from_secs(1);
        oracle.adjust_update_interval_for_staleness(
            timestamp_before(now, Duration::from_secs(3)),
            timestamp_before(now, Duration::ZERO),
            now,
        );
        assert!(oracle.take_shrink_request_for_test().is_none());
        assert_eq!(
            oracle.next_update_interval(now, None),
            Duration::from_secs(2)
        );

        now += Duration::from_secs(1);
        oracle.adjust_update_interval_for_staleness(
            timestamp_before(now, Duration::from_secs(1)),
            timestamp_before(now, Duration::ZERO),
            now,
        );
        assert_eq!(
            oracle.take_shrink_request_for_test(),
            Some(Duration::from_secs(1))
        );
        let mut expected = Duration::from_secs(1) - ADAPTIVE_SHRINKING_PRESERVE;
        assert_eq!(
            oracle.next_update_interval(now, Some(Duration::from_secs(1))),
            expected
        );
        assert_eq!(state(), AdaptiveUpdateIntervalState::Adapting);
        assert_eq!(
            oracle
                .state
                .adaptive_state
                .lock()
                .unwrap()
                .last_short_staleness_read,
            Some(now)
        );

        now += ADAPTIVE_DELAY_BEFORE_RECOVERING / 2;
        oracle.adjust_update_interval_for_staleness(
            timestamp_before(now, Duration::from_secs(1)),
            timestamp_before(now, Duration::ZERO),
            now,
        );
        assert!(oracle.take_shrink_request_for_test().is_none());
        assert_eq!(
            oracle
                .state
                .adaptive_state
                .lock()
                .unwrap()
                .last_short_staleness_read,
            Some(now)
        );

        now += ADAPTIVE_DELAY_BEFORE_RECOVERING / 2 + Duration::from_secs(1);
        oracle.state.adaptive_state.lock().unwrap().last_tick = now - Duration::from_secs(1);
        assert_eq!(oracle.next_update_interval(now, None), expected);
        assert_eq!(state(), AdaptiveUpdateIntervalState::Adapting);

        now += ADAPTIVE_DELAY_BEFORE_RECOVERING / 2;
        oracle.state.adaptive_state.lock().unwrap().last_tick = now - Duration::from_secs(1);
        expected += ADAPTIVE_RECOVER_PER_SECOND;
        assert_eq!(oracle.next_update_interval(now, None), expected);
        assert_eq!(state(), AdaptiveUpdateIntervalState::Recovering);

        oracle.state.adaptive_state.lock().unwrap().last_tick = now;
        now += Duration::from_secs(2);
        oracle.adjust_update_interval_for_staleness(
            timestamp_before(
                now,
                expected + ADAPTIVE_BLOCK_RECOVER_THRESHOLD.saturating_mul(2),
            ),
            timestamp_before(now, Duration::ZERO),
            now,
        );
        assert!(oracle.take_shrink_request_for_test().is_none());
        expected += ADAPTIVE_RECOVER_PER_SECOND.saturating_mul(2);
        assert_eq!(oracle.next_update_interval(now, None), expected);
        assert_eq!(state(), AdaptiveUpdateIntervalState::Recovering);

        oracle.state.adaptive_state.lock().unwrap().last_tick = now;
        now += Duration::from_secs(1);
        oracle.adjust_update_interval_for_staleness(
            timestamp_before(now, expected + ADAPTIVE_BLOCK_RECOVER_THRESHOLD / 2),
            timestamp_before(now, Duration::ZERO),
            now,
        );
        assert!(oracle.take_shrink_request_for_test().is_none());
        assert_eq!(oracle.next_update_interval(now, None), expected);
        assert_eq!(state(), AdaptiveUpdateIntervalState::Adapting);
        oracle.state.adaptive_state.lock().unwrap().last_tick = now;
        now += Duration::from_secs(1);
        assert_eq!(oracle.next_update_interval(now, None), expected);
        assert_eq!(state(), AdaptiveUpdateIntervalState::Adapting);

        now += ADAPTIVE_DELAY_BEFORE_RECOVERING;
        oracle.state.adaptive_state.lock().unwrap().last_tick = now - Duration::from_secs(1);
        expected += ADAPTIVE_RECOVER_PER_SECOND;
        assert_eq!(oracle.next_update_interval(now, None), expected);
        assert_eq!(state(), AdaptiveUpdateIntervalState::Recovering);

        while expected < Duration::from_secs(2) {
            oracle.state.adaptive_state.lock().unwrap().last_tick = now;
            now += Duration::from_secs(1);
            expected = expected
                .saturating_add(ADAPTIVE_RECOVER_PER_SECOND)
                .min(Duration::from_secs(2));
            assert_eq!(oracle.next_update_interval(now, None), expected);
            assert_eq!(
                state(),
                if expected == Duration::from_secs(2) {
                    AdaptiveUpdateIntervalState::Normal
                } else {
                    AdaptiveUpdateIntervalState::Recovering
                }
            );
        }

        for configured in [Duration::from_secs(1), Duration::from_secs(2)] {
            oracle
                .set_low_resolution_timestamp_update_interval(configured)
                .unwrap();
            assert_eq!(oracle.adaptive_update_interval(), configured);
            assert_eq!(oracle.next_update_interval(now, None), configured);
        }

        now += Duration::from_secs(1);
        oracle.adjust_update_interval_for_staleness(
            timestamp_before(now, Duration::from_secs(1)),
            timestamp_before(now, Duration::ZERO),
            now,
        );
        assert_eq!(
            oracle.take_shrink_request_for_test(),
            Some(Duration::from_secs(1))
        );
        expected = Duration::from_secs(1) - ADAPTIVE_SHRINKING_PRESERVE;
        assert_eq!(
            oracle.next_update_interval(now, Some(Duration::from_secs(1))),
            expected
        );
        assert_eq!(state(), AdaptiveUpdateIntervalState::Adapting);
        oracle
            .set_low_resolution_timestamp_update_interval(Duration::from_secs(3))
            .unwrap();
        assert_eq!(oracle.adaptive_update_interval(), expected);
        assert_eq!(oracle.next_update_interval(now, None), expected);
        oracle
            .set_low_resolution_timestamp_update_interval(Duration::from_secs(1))
            .unwrap();
        assert_eq!(oracle.adaptive_update_interval(), expected);
        assert_eq!(oracle.next_update_interval(now, None), expected);

        oracle
            .set_low_resolution_timestamp_update_interval(Duration::from_millis(800))
            .unwrap();
        assert_eq!(
            oracle.adaptive_update_interval(),
            Duration::from_millis(800)
        );
        assert_eq!(
            oracle.next_update_interval(now, None),
            Duration::from_millis(800)
        );
        assert_eq!(state(), AdaptiveUpdateIntervalState::Normal);

        oracle
            .set_low_resolution_timestamp_update_interval(MIN_ADAPTIVE_UPDATE_INTERVAL / 2)
            .unwrap();
        assert_eq!(
            oracle.adaptive_update_interval(),
            MIN_ADAPTIVE_UPDATE_INTERVAL / 2
        );
        assert_eq!(
            oracle.next_update_interval(now, None),
            MIN_ADAPTIVE_UPDATE_INTERVAL / 2
        );
        assert_eq!(state(), AdaptiveUpdateIntervalState::Unadjustable);
        oracle.close();
    }

    #[tokio::test]
    async fn pd_oracle_adaptive_interval_transitions_match_source_constants() {
        let source = Arc::new(TestPdSource::new(1_700_000_000_000));
        let oracle = PdOracle::new(
            source,
            PdOracleOptions {
                update_interval: Duration::from_secs(2),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();
        let mut now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        assert_eq!(
            oracle.next_update_interval(now, None),
            Duration::from_secs(2)
        );
        assert_eq!(
            oracle.state.adaptive_state.lock().unwrap().state,
            AdaptiveUpdateIntervalState::Normal
        );
        now += Duration::from_secs(1);
        let adapted = oracle.next_update_interval(now, Some(Duration::from_secs(1)));
        assert_eq!(adapted, Duration::from_millis(900));
        assert_eq!(
            oracle.state.adaptive_state.lock().unwrap().state,
            AdaptiveUpdateIntervalState::Adapting
        );
        now += Duration::from_secs(1);
        {
            let mut state = oracle.state.adaptive_state.lock().unwrap();
            state.last_short_staleness_read =
                Some(now - ADAPTIVE_DELAY_BEFORE_RECOVERING - Duration::from_secs(1));
            state.last_tick = now - Duration::from_secs(1);
        }
        assert_eq!(
            oracle.next_update_interval(now, None),
            Duration::from_millis(920)
        );
        assert_eq!(
            oracle.state.adaptive_state.lock().unwrap().state,
            AdaptiveUpdateIntervalState::Recovering
        );
        oracle
            .set_low_resolution_timestamp_update_interval(Duration::from_millis(400))
            .unwrap();
        assert_eq!(
            oracle.next_update_interval(now, None),
            Duration::from_millis(400)
        );
        assert_eq!(
            oracle.state.adaptive_state.lock().unwrap().state,
            AdaptiveUpdateIntervalState::Unadjustable
        );
        oracle.close();
    }

    #[tokio::test]
    async fn pd_oracle_adaptive_short_reads_block_recovery_and_manual_updates() {
        let source = Arc::new(TestPdSource::new(1_700_000_000_000));
        let oracle = PdOracle::new(
            source,
            PdOracleOptions {
                update_interval: Duration::from_secs(2),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let timestamp =
            |before: Duration| super::super::compose_timestamp(get_physical(now - before), 1);

        oracle.adjust_update_interval_for_staleness(
            timestamp(Duration::from_secs(3)),
            timestamp(Duration::ZERO),
            now,
        );
        assert!(oracle.take_shrink_request_for_test().is_none());
        assert_eq!(
            oracle.next_update_interval(now, None),
            Duration::from_secs(2)
        );

        oracle.adjust_update_interval_for_staleness(
            timestamp(Duration::from_secs(1)),
            timestamp(Duration::ZERO),
            now,
        );
        assert_eq!(
            oracle.take_shrink_request_for_test(),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            oracle.next_update_interval(now, Some(Duration::from_secs(1))),
            Duration::from_millis(900)
        );
        assert_eq!(
            oracle
                .state
                .adaptive_state
                .lock()
                .unwrap()
                .last_short_staleness_read,
            Some(now)
        );

        let later = now + ADAPTIVE_DELAY_BEFORE_RECOVERING / 2;
        oracle.adjust_update_interval_for_staleness(
            timestamp(Duration::from_secs(1)),
            timestamp(Duration::ZERO),
            later,
        );
        assert!(oracle.take_shrink_request_for_test().is_none());
        assert_eq!(
            oracle
                .state
                .adaptive_state
                .lock()
                .unwrap()
                .last_short_staleness_read,
            Some(later)
        );

        oracle
            .set_low_resolution_timestamp_update_interval(Duration::from_secs(3))
            .unwrap();
        assert_eq!(
            oracle.adaptive_update_interval(),
            Duration::from_millis(900)
        );
        oracle
            .set_low_resolution_timestamp_update_interval(Duration::from_millis(800))
            .unwrap();
        assert_eq!(
            oracle.adaptive_update_interval(),
            Duration::from_millis(800)
        );
        assert_eq!(
            oracle.next_update_interval(later, None),
            Duration::from_millis(800)
        );
        oracle.close();
    }

    #[tokio::test]
    #[serial]
    async fn source_test_validate_read_ts() {
        ENABLE_TS_VALIDATION.store(true, Ordering::Release);
        let source = Arc::new(TestPdSource::new(1_700_000_000_000));
        let oracle = PdOracle::new(
            source.clone(),
            PdOracleOptions {
                update_interval: Duration::from_secs(2),
                no_update_timestamp: false,
            },
        )
        .await
        .unwrap();
        let option = OracleOption {
            txn_scope: super::super::GLOBAL_TXN_SCOPE.to_owned(),
        };

        assert!(oracle
            .validate_read_timestamp(u64::MAX, true, &option)
            .await
            .is_err());
        let timestamp = oracle.get_timestamp(&option).await.unwrap();
        assert!(timestamp >= 1);
        oracle
            .validate_read_timestamp(1, true, &option)
            .await
            .unwrap();

        let timestamp = oracle.get_timestamp(&option).await.unwrap();
        oracle
            .validate_read_timestamp(timestamp + 1, true, &option)
            .await
            .unwrap();
        let timestamp = oracle.get_timestamp(&option).await.unwrap();
        oracle
            .validate_read_timestamp(timestamp + 2, true, &option)
            .await
            .unwrap();
        let timestamp = oracle.get_timestamp(&option).await.unwrap();
        assert!(oracle
            .validate_read_timestamp(timestamp + 3, true, &option)
            .await
            .is_err());

        let timestamp = oracle.get_timestamp(&option).await.unwrap();
        source.logical.fetch_add(2, Ordering::AcqRel);
        oracle
            .validate_read_timestamp(timestamp + 3, true, &option)
            .await
            .unwrap();
        oracle.close();
        ENABLE_TS_VALIDATION.store(false, Ordering::Release);
    }

    #[tokio::test]
    #[serial]
    async fn source_test_validate_read_ts_for_normal_read_do_not_affect_update_interval() {
        ENABLE_TS_VALIDATION.store(true, Ordering::Release);
        let oracle = PdOracle::new(
            Arc::new(TestPdSource::new(1_700_000_000_000)),
            PdOracleOptions {
                update_interval: Duration::from_secs(2),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();
        let option = OracleOption {
            txn_scope: super::super::GLOBAL_TXN_SCOPE.to_owned(),
        };
        let timestamp = oracle.get_timestamp(&option).await.unwrap();
        assert!(timestamp >= 1);

        oracle
            .validate_read_timestamp(timestamp, false, &option)
            .await
            .unwrap();
        assert!(oracle.take_shrink_request_for_test().is_none());
        oracle
            .validate_read_timestamp(timestamp + 2, false, &option)
            .await
            .unwrap();
        assert!(oracle.take_shrink_request_for_test().is_none());
        assert!(oracle
            .validate_read_timestamp(timestamp + 5, false, &option)
            .await
            .is_err());
        assert!(oracle.take_shrink_request_for_test().is_none());
        oracle
            .validate_read_timestamp(timestamp + 5, false, &option)
            .await
            .unwrap();
        assert!(oracle.take_shrink_request_for_test().is_none());
        oracle.close();
        ENABLE_TS_VALIDATION.store(false, Ordering::Release);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn source_test_set_last_ts_always_push_ts() {
        let oracle = PdOracle::new(
            Arc::new(TestPdSource::new(1_700_000_000_000)),
            PdOracleOptions {
                update_interval: Duration::from_secs(2),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let oracle = oracle.clone();
            tasks.push(tokio::spawn(async move {
                let option = OracleOption {
                    txn_scope: super::super::GLOBAL_TXN_SCOPE.to_owned(),
                };
                for _ in 0..1_000 {
                    let timestamp = oracle.get_timestamp(&option).await.unwrap();
                    let last = oracle
                        .last_timestamp(super::super::GLOBAL_TXN_SCOPE)
                        .unwrap()
                        .timestamp;
                    assert!(last >= timestamp);
                    tokio::task::yield_now().await;
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        oracle.close();
    }

    #[tokio::test]
    #[serial]
    async fn pd_oracle_validation_and_refresh_worker() {
        ENABLE_TS_VALIDATION.store(true, Ordering::Release);
        let source = Arc::new(TestPdSource::new(1_700_000_000_000));
        let oracle = PdOracle::new(
            source,
            PdOracleOptions {
                update_interval: Duration::from_millis(10),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();
        oracle.start_low_resolution_timestamp_update_loop();
        let option = OracleOption::default();
        let before = oracle.get_low_resolution_timestamp(&option).await.unwrap();
        tokio::time::sleep(Duration::from_millis(35)).await;
        let after = oracle.get_low_resolution_timestamp(&option).await.unwrap();
        assert!(after > before);
        assert!(oracle
            .validate_read_timestamp(u64::MAX, true, &option)
            .await
            .is_err());
        assert!(oracle
            .validate_read_timestamp(u64::MAX, false, &option)
            .await
            .is_ok());
        assert!(oracle
            .validate_read_timestamp(i64::MAX as u64, false, &option)
            .await
            .is_err());
        let current = oracle.get_timestamp(&option).await.unwrap();
        assert!(oracle
            .validate_read_timestamp(current + 1, false, &option)
            .await
            .is_ok());
        oracle.close();
        ENABLE_TS_VALIDATION.store(false, Ordering::Release);
    }

    #[tokio::test]
    async fn pd_oracle_refresh_worker_stops_after_close() {
        let oracle = PdOracle::new(
            Arc::new(TestPdSource::new(1_700_000_000_000)),
            PdOracleOptions {
                update_interval: Duration::from_millis(10),
                no_update_timestamp: false,
            },
        )
        .await
        .unwrap();
        oracle.close();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !oracle
                .state
                .update_task
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .is_finished()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn source_test_validate_read_ts_for_stale_read_reusing_get_ts_result() {
        let _scenario = fail::FailScenario::setup();
        fail::cfg("validateReadTSRetryGetTS", "return(skip)").unwrap();
        ENABLE_TS_VALIDATION.store(true, Ordering::Release);
        let source = Arc::new(GatePdSource::new());
        let oracle = PdOracle::new(
            source.clone(),
            PdOracleOptions {
                update_interval: Duration::from_secs(2),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();
        let base = super::super::compose_timestamp(1_700_000_000_000, 0);
        let cancel_indices = [None, None, Some(0), Some(1)];

        for (case_index, logical) in [100_u64, 200, 300, 400].into_iter().enumerate() {
            source.block.store(true, Ordering::Release);
            source
                .logical
                .store(i64::try_from(logical - 1).unwrap(), Ordering::Release);
            let read_timestamps = [logical - 2, logical + 2, logical - 1, logical + 1, logical];
            let expected_success = [true, false, true, false, true];
            let mut tasks = read_timestamps
                .into_iter()
                .map(|read_timestamp| {
                    let oracle = oracle.clone();
                    tokio::spawn(async move {
                        oracle
                            .validate_read_timestamp(
                                base + read_timestamp,
                                true,
                                &OracleOption {
                                    txn_scope: super::super::GLOBAL_TXN_SCOPE.to_owned(),
                                },
                            )
                            .await
                    })
                })
                .map(Some)
                .collect::<Vec<_>>();

            let expected_calls = case_index + 2;
            tokio::time::timeout(Duration::from_secs(1), async {
                while source.calls.load(Ordering::Acquire) != expected_calls {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(tasks.iter().flatten().all(|task| !task.is_finished()));

            if let Some(cancel_index) = cancel_indices[case_index] {
                let task = tasks[cancel_index].take().unwrap();
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            }

            source.block.store(false, Ordering::Release);
            source.release.notify_waiters();
            for (index, task) in tasks.into_iter().enumerate() {
                let Some(task) = task else { continue };
                let result = tokio::time::timeout(Duration::from_secs(1), task)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    result.is_ok(),
                    expected_success[index],
                    "case {case_index}, validation {index}: {result:?}"
                );
            }
            tokio::time::timeout(Duration::from_secs(1), async {
                while !oracle.state.validation_flights.lock().await.is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        oracle.close();
        ENABLE_TS_VALIDATION.store(false, Ordering::Release);
    }

    #[tokio::test]
    #[serial]
    async fn pd_oracle_validation_shares_one_inflight_timestamp_request() {
        ENABLE_TS_VALIDATION.store(true, Ordering::Release);
        let source = Arc::new(GatePdSource::new());
        let oracle = PdOracle::new(
            source.clone(),
            PdOracleOptions {
                update_interval: Duration::from_secs(2),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();
        source.block.store(true, Ordering::Release);
        let option = OracleOption::default();
        let read_timestamp = oracle.get_low_resolution_timestamp(&option).await.unwrap() + 1;
        let first = {
            let oracle = oracle.clone();
            let option = option.clone();
            tokio::spawn(async move {
                oracle
                    .validate_read_timestamp(read_timestamp, false, &option)
                    .await
            })
        };
        let second = {
            let oracle = oracle.clone();
            tokio::spawn(async move {
                oracle
                    .validate_read_timestamp(read_timestamp, false, &OracleOption::default())
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while source.calls.load(Ordering::Acquire) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(source.calls.load(Ordering::Acquire), 2);
        source.release.notify_waiters();
        assert!(first.await.unwrap().is_ok());
        assert!(second.await.unwrap().is_ok());
        oracle.close();
        ENABLE_TS_VALIDATION.store(false, Ordering::Release);
    }

    #[tokio::test]
    #[serial]
    async fn cancelling_one_validation_waiter_does_not_cancel_the_shared_request() {
        ENABLE_TS_VALIDATION.store(true, Ordering::Release);
        let source = Arc::new(GatePdSource::new());
        let oracle = PdOracle::new(
            source.clone(),
            PdOracleOptions {
                update_interval: Duration::from_secs(2),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();
        source.block.store(true, Ordering::Release);
        let read_timestamp = oracle
            .get_low_resolution_timestamp(&OracleOption::default())
            .await
            .unwrap()
            + 1;
        let cancelled = {
            let oracle = oracle.clone();
            tokio::spawn(async move {
                oracle
                    .validate_read_timestamp(read_timestamp, true, &OracleOption::default())
                    .await
            })
        };
        let survivor = {
            let oracle = oracle.clone();
            tokio::spawn(async move {
                oracle
                    .validate_read_timestamp(read_timestamp, true, &OracleOption::default())
                    .await
            })
        };
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        cancelled.abort();
        source.release.notify_waiters();
        assert!(survivor.await.unwrap().is_ok());
        assert_eq!(source.calls.load(Ordering::Acquire), 2);
        oracle.close();
        ENABLE_TS_VALIDATION.store(false, Ordering::Release);
    }

    #[tokio::test]
    #[serial]
    async fn source_test_validate_read_ts_from_different_source() {
        ENABLE_TS_VALIDATION.store(true, Ordering::Release);
        let source = Arc::new(GatePdSource::new());
        let oracle = PdOracle::new(
            source.clone(),
            PdOracleOptions {
                update_interval: Duration::from_secs(2),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();
        let option = OracleOption::default();
        let seeded = oracle.get_low_resolution_timestamp(&option).await.unwrap();

        // The first validation obtains timestamp 2 but cannot return it yet.
        source.block.store(true, Ordering::Release);
        let first = {
            let oracle = oracle.clone();
            let option = option.clone();
            tokio::spawn(async move {
                oracle
                    .validate_read_timestamp(seeded + 1, false, &option)
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while source.calls.load(Ordering::Acquire) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(source.calls.load(Ordering::Acquire), 2);

        // Simulate a different client consuming timestamps through 13 while
        // the first shared result is still pending. The second validation
        // initially sees the old result and must use its one retry to pass.
        source.logical.store(12, Ordering::Release);
        let second = {
            let oracle = oracle.clone();
            let option = option.clone();
            tokio::spawn(async move {
                oracle
                    .validate_read_timestamp(seeded + 12, false, &option)
                    .await
            })
        };
        tokio::task::yield_now().await;
        source.block.store(false, Ordering::Release);
        source.release.notify_waiters();

        assert!(first.await.unwrap().is_ok());
        assert!(second.await.unwrap().is_ok());
        assert_eq!(source.calls.load(Ordering::Acquire), 3);
        oracle.close();
        ENABLE_TS_VALIDATION.store(false, Ordering::Release);
    }

    #[tokio::test]
    async fn pd_oracle_async_timestamp_starts_immediately_and_keeps_the_maximum_cache_value() {
        let source = Arc::new(GatePdSource::new());
        let oracle = PdOracle::new(
            source.clone(),
            PdOracleOptions {
                update_interval: Duration::from_secs(2),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();

        source.block.store(true, Ordering::Release);
        let future = oracle.get_timestamp_async(&OracleOption::default());
        tokio::time::timeout(Duration::from_secs(1), async {
            while source.calls.load(Ordering::Acquire) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(source.calls.load(Ordering::Acquire), 2);
        source.block.store(false, Ordering::Release);
        source.release.notify_waiters();
        assert_eq!(
            future.wait().await.unwrap(),
            super::super::compose_timestamp(1_700_000_000_000, 2)
        );

        let newer = super::super::compose_timestamp(1_700_000_000_001, 1);
        let older = super::super::compose_timestamp(1_700_000_000_000, 99);
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let oracle = oracle.clone();
            tasks.push(tokio::spawn(async move {
                oracle.set_last_timestamp_for_test(newer, super::super::GLOBAL_TXN_SCOPE);
                oracle.set_last_timestamp_for_test(older, super::super::GLOBAL_TXN_SCOPE);
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(
            oracle
                .get_low_resolution_timestamp(&OracleOption::default())
                .await
                .unwrap(),
            newer
        );
        oracle.close();
    }

    #[tokio::test]
    async fn source_uncovered_async_low_resolution_error_is_exact_and_work_is_cancellable() {
        let source = Arc::new(GatePdSource::new());
        let oracle = PdOracle::new(
            source.clone(),
            PdOracleOptions {
                update_interval: Duration::from_secs(2),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();
        let missing = OracleOption {
            txn_scope: "missing".to_owned(),
        };
        assert_eq!(
            oracle
                .get_low_resolution_timestamp_async(&missing)
                .wait()
                .await
                .unwrap_err()
                .to_string(),
            "get low resolution timestamp async fail, invalid txnScope = missing"
        );

        source.block.store(true, Ordering::Release);
        let future = oracle.get_timestamp_async(&OracleOption::default());
        tokio::time::timeout(Duration::from_secs(1), async {
            while source.calls.load(Ordering::Acquire) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(future);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        source.block.store(false, Ordering::Release);
        source.release.notify_waiters();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(source.completed.load(Ordering::Acquire), 1);

        source.block.store(true, Ordering::Release);
        let future: Arc<dyn TimestampFuture> =
            Arc::from(oracle.get_timestamp_async(&OracleOption::default()));
        let waiter = {
            let future = Arc::clone(&future);
            tokio::spawn(async move { future.wait().await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while source.calls.load(Ordering::Acquire) != 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        source.block.store(false, Ordering::Release);
        source.release.notify_waiters();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(source.completed.load(Ordering::Acquire), 1);
        drop(future);
        oracle.close();
    }

    #[tokio::test]
    #[serial]
    async fn source_uncovered_cancelled_validation_still_finishes_and_cleans_singleflight() {
        ENABLE_TS_VALIDATION.store(true, Ordering::Release);
        let source = Arc::new(GatePdSource::new());
        let oracle = PdOracle::new(
            source.clone(),
            PdOracleOptions {
                update_interval: Duration::from_secs(2),
                no_update_timestamp: true,
            },
        )
        .await
        .unwrap();
        source.block.store(true, Ordering::Release);
        let read_timestamp = oracle
            .get_low_resolution_timestamp(&OracleOption::default())
            .await
            .unwrap()
            + 1;
        let waiter = {
            let oracle = oracle.clone();
            tokio::spawn(async move {
                oracle
                    .validate_read_timestamp(read_timestamp, false, &OracleOption::default())
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while source.calls.load(Ordering::Acquire) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());

        source.block.store(false, Ordering::Release);
        source.release.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if source.completed.load(Ordering::Acquire) == 2
                    && oracle.state.validation_flights.lock().await.is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            oracle
                .get_low_resolution_timestamp(&OracleOption::default())
                .await
                .unwrap(),
            read_timestamp
        );
        oracle.close();
        ENABLE_TS_VALIDATION.store(false, Ordering::Release);
    }
}
