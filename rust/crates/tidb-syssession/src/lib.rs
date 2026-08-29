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

//! Go `pkg/session/syssession`.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tidb_ast::Stmt;
use tidb_datatype::Datum;
use tidb_resolve::ResultFieldRef;
use tidb_sqlexec::{
    ExecutionContext, OptionFuncAlias, RecordSet, RestrictedSqlExecutor, SqlExecutor,
};
use tidb_util::sqlescape::SqlArg;

/// Maximum size of a system-session pool.
pub const POOL_MAX_SIZE: usize = 1024 * 1024 * 1024;

/// Package error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SysSessionError(String);

impl SysSessionError {
    /// Creates an error with the source message.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for SysSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for SysSessionError {}

/// Result returned by this package.
pub type Result<T> = std::result::Result<T, SysSessionError>;

/// The `sessionctx.Context` operations used by this package.
pub trait SessionContext: Send + Sync {
    /// Closes the underlying context.
    fn close(&self);
    /// Rolls back its current transaction.
    fn rollback_txn(&self, context: &dyn ExecutionContext);
    /// Whether a transaction future is pending.
    fn has_prepared_txn_future(&self) -> bool;
    /// Whether the current transaction is valid.
    fn txn_valid(&self) -> std::result::Result<bool, tidb_sqlexec::SqlExecError>;
    /// Returns the ordinary SQL executor owned by this context.
    fn sql_executor(&self) -> Arc<dyn SqlExecutor>;
    /// Returns the restricted SQL executor owned by this context.
    fn restricted_sql_executor(&self) -> Arc<dyn RestrictedSqlExecutor>;
    /// Registers the context as an internal session.
    fn register_internal_session(&self);
    /// Removes the context from the internal-session registry.
    fn unregister_internal_session(&self);
    /// Whether the internal-session registry currently contains this context.
    fn contains_internal_session(&self) -> bool;
    /// Attempts to register the context, returning whether it is registered.
    fn store_internal_session(&self) -> bool;
}

trait OwnerHook<C: SessionContext + ?Sized>: Send + Sync {
    fn on_became_owner(&self, context: &C) -> Result<()>;
    fn on_resign_owner(&self, context: &C) -> Result<()>;
}

struct NoopOwner;

impl<C: SessionContext + ?Sized> OwnerHook<C> for NoopOwner {
    fn on_became_owner(&self, _context: &C) -> Result<()> {
        Ok(())
    }

    fn on_resign_owner(&self, _context: &C) -> Result<()> {
        Ok(())
    }
}

struct Owner<C: SessionContext + ?Sized> {
    id: u64,
    hook: Arc<dyn OwnerHook<C>>,
}

impl<C: SessionContext + ?Sized> Clone for Owner<C> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            hook: Arc::clone(&self.hook),
        }
    }
}

impl<C: SessionContext + ?Sized> Owner<C> {
    fn same(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

static NEXT_OWNER_ID: AtomicU64 = AtomicU64::new(1);

fn owner<C: SessionContext + ?Sized>(hook: Arc<dyn OwnerHook<C>>) -> Owner<C> {
    Owner {
        id: NEXT_OWNER_ID.fetch_add(1, Ordering::Relaxed),
        hook,
    }
}

struct InternalState<C: SessionContext + ?Sized> {
    owner: Option<Owner<C>>,
    sequence: u64,
    in_use: u64,
    unsafe_count: u64,
    avoid_reuse: bool,
    close_owner: Option<Owner<C>>,
}

struct InternalSession<C: SessionContext + ?Sized> {
    context: Mutex<Arc<C>>,
    state: Mutex<InternalState<C>>,
}

impl<C: SessionContext + ?Sized + 'static> InternalSession<C> {
    fn new(context: Arc<C>, owner: Owner<C>) -> Result<Arc<Self>> {
        owner.hook.on_became_owner(context.as_ref())?;
        Ok(Arc::new(Self {
            context: Mutex::new(context),
            state: Mutex::new(InternalState {
                owner: Some(owner),
                sequence: 0,
                in_use: 0,
                unsafe_count: 0,
                avoid_reuse: false,
                close_owner: None,
            }),
        }))
    }

    fn context(&self) -> Arc<C> {
        Arc::clone(
            &self
                .context
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    fn owner_is(&self, caller: &Owner<C>) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .owner
            .as_ref()
            .is_some_and(|current| current.same(caller))
    }

    fn is_closed(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .owner
            .is_none()
    }

    fn is_avoid_reuse(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .avoid_reuse
    }

    fn transfer_owner(&self, from: &Owner<C>, to: Owner<C>) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.sequence += 1;
        let prefix = format!("TransferOwner error, opSeq: {}, ", state.sequence);
        let Some(current) = state.owner.as_ref() else {
            return Err(SysSessionError::new(format!("{prefix}session is closed")));
        };
        if !current.same(from) {
            return Err(SysSessionError::new(format!(
                "{prefix}caller is not the owner"
            )));
        }
        if current.same(&to) {
            return Ok(());
        }
        if state.in_use > 0 {
            return Err(SysSessionError::new(format!(
                "{prefix}session is still inUse: {}",
                state.in_use
            )));
        }

        let previous = current.clone();
        state.owner = Some(Owner {
            id: 0,
            hook: Arc::new(NoopOwner),
        });
        let context = self.context();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            previous.hook.on_resign_owner(context.as_ref())
        })) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let temporary = state.owner.clone();
                self.close_locked(&mut state, temporary);
                return Err(error);
            }
            Err(payload) => {
                let temporary = state.owner.clone();
                self.close_locked(&mut state, temporary);
                std::panic::resume_unwind(payload);
            }
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            to.hook.on_became_owner(context.as_ref())
        })) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let temporary = state.owner.clone();
                self.close_locked(&mut state, temporary);
                return Err(error);
            }
            Err(payload) => {
                let temporary = state.owner.clone();
                self.close_locked(&mut state, temporary);
                std::panic::resume_unwind(payload);
            }
        }
        state.owner = Some(to);
        Ok(())
    }

    fn enter(self: &Arc<Self>, caller: &Owner<C>, thread_safe: bool) -> Result<OperationGuard<C>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.sequence += 1;
        let Some(current) = state.owner.as_ref() else {
            return Err(SysSessionError::new(
                "EnterOperation error: session is closed",
            ));
        };
        if !current.same(caller) {
            return Err(SysSessionError::new(
                "EnterOperation error: caller is not the owner",
            ));
        }
        if !thread_safe {
            state.unsafe_count += 1;
            if state.unsafe_count > 1 {
                return Err(SysSessionError::new(
                    "EnterOperation error: race detected for concurrent thread-unsafe operations",
                ));
            }
        }
        state.in_use += 1;
        Ok(OperationGuard {
            internal: Arc::clone(self),
            caller: caller.clone(),
            thread_safe,
            exited: false,
        })
    }

    fn mark_avoid_reuse(&self, caller: &Owner<C>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .owner
            .as_ref()
            .is_some_and(|current| current.same(caller))
        {
            state.avoid_reuse = true;
        }
    }

    fn owner_close(&self, caller: &Owner<C>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .owner
            .as_ref()
            .is_some_and(|current| current.same(caller))
        {
            let previous = state.owner.clone();
            self.close_locked(&mut state, previous);
        }
    }

    fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = state.owner.clone();
        self.close_locked(&mut state, previous);
    }

    fn close_locked(&self, state: &mut InternalState<C>, previous: Option<Owner<C>>) {
        let Some(previous) = previous else {
            return;
        };
        state.owner = None;
        if state.in_use > 0 {
            state.close_owner = Some(previous);
            return;
        }
        let context = self.context();
        let _ = previous.hook.on_resign_owner(context.as_ref());
        context.close();
    }

    fn check_no_pending_txn(&self) -> Result<()> {
        let context = self.context();
        if context.has_prepared_txn_future() {
            return Err(SysSessionError::new("txn is pending for TSO"));
        }
        if context
            .txn_valid()
            .map_err(|error| SysSessionError::new(error.to_string()))?
        {
            return Err(SysSessionError::new("txn is still valid"));
        }
        Ok(())
    }
}

struct OperationGuard<C: SessionContext + ?Sized + 'static> {
    internal: Arc<InternalSession<C>>,
    caller: Owner<C>,
    thread_safe: bool,
    exited: bool,
}

impl<C: SessionContext + ?Sized + 'static> Drop for OperationGuard<C> {
    fn drop(&mut self) {
        if self.exited {
            return;
        }
        self.exited = true;
        if std::thread::panicking() {
            self.internal.mark_avoid_reuse(&self.caller);
        }
        let close_owner = {
            let mut state = self
                .internal
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.sequence += 1;
            state.in_use -= 1;
            if !self.thread_safe {
                state.unsafe_count = 0;
            }
            if state.owner.is_none() && state.in_use == 0 {
                state.close_owner.take()
            } else {
                None
            }
        };
        if let Some(owner) = close_owner {
            let context = self.internal.context();
            let _ = owner.hook.on_resign_owner(context.as_ref());
            context.close();
        }
    }
}

struct SessionOwner;

impl<C: SessionContext + ?Sized> OwnerHook<C> for SessionOwner {
    fn on_became_owner(&self, context: &C) -> Result<()> {
        context.register_internal_session();
        Ok(())
    }

    fn on_resign_owner(&self, context: &C) -> Result<()> {
        context.unregister_internal_session();
        Ok(())
    }
}

/// Public proxy for one system-internal session.
pub struct Session<C: SessionContext + ?Sized = dyn SessionContext> {
    internal: Arc<InternalSession<C>>,
    owner: Owner<C>,
}

impl<C: SessionContext + ?Sized> Clone for Session<C> {
    fn clone(&self) -> Self {
        Self {
            internal: Arc::clone(&self.internal),
            owner: self.owner.clone(),
        }
    }
}

impl<C: SessionContext + ?Sized + 'static> Session<C> {
    /// Creates a session owned by the returned proxy.
    pub fn new_for_test(context: Arc<C>) -> Result<Self> {
        let owner = owner(Arc::new(SessionOwner));
        let internal = InternalSession::new(context, owner.clone())?;
        Ok(Self { internal, owner })
    }

    /// Closes the session if this proxy remains its owner.
    pub fn close(&self) {
        self.internal.owner_close(&self.owner);
    }

    /// Whether this proxy owns its internal session.
    pub fn is_owner(&self) -> bool {
        self.internal.owner_is(&self.owner)
    }

    /// Prevents the session from returning to its pool.
    pub fn avoid_reuse(&self) {
        self.internal.mark_avoid_reuse(&self.owner);
    }

    /// Whether the internal session is closed.
    pub fn is_internal_closed(&self) -> bool {
        self.internal.is_closed()
    }

    /// Whether the internal session is quarantined from reuse.
    pub fn is_avoid_reuse(&self) -> bool {
        self.internal.is_avoid_reuse()
    }

    /// Executes a callback with the protected session context.
    pub fn with_session_context<T, E>(
        &self,
        callback: impl FnOnce(&C) -> std::result::Result<T, E>,
    ) -> std::result::Result<T, E>
    where
        E: From<SysSessionError>,
    {
        let _operation = self.internal.enter(&self.owner, false).map_err(E::from)?;
        let context = self.internal.context();
        callback(context.as_ref())
    }

    /// Returns the internal context for package-source tests.
    pub fn internal_context_for_test(&self) -> Arc<C> {
        self.internal.context()
    }

    /// Replaces the context while this proxy owns the session.
    pub fn reset_context_for_test(&self, replace: impl FnOnce(Arc<C>) -> Arc<C>) -> Result<()> {
        if !self.is_owner() {
            return Err(SysSessionError::new(
                "ResetSctxForTestcaller is not the owner",
            ));
        }
        let mut context = self
            .internal
            .context
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *context = replace(Arc::clone(&context));
        Ok(())
    }

    fn from_pool(internal: Arc<InternalSession<C>>, pool_owner: &Owner<C>) -> Result<Self> {
        let owner = owner(Arc::new(SessionOwner));
        internal.transfer_owner(pool_owner, owner.clone())?;
        Ok(Self { internal, owner })
    }
}

impl<C: SessionContext + ?Sized + 'static> SqlExecutor for Session<C> {
    fn execute(
        &self,
        context: &dyn ExecutionContext,
        sql: &str,
    ) -> tidb_sqlexec::Result<Vec<Box<dyn RecordSet>>> {
        let _operation = self
            .internal
            .enter(&self.owner, false)
            .map_err(|error| -> tidb_sqlexec::SqlExecError { Box::new(error) })?;
        self.internal.context().sql_executor().execute(context, sql)
    }

    fn execute_internal(
        &self,
        context: &dyn ExecutionContext,
        sql: &str,
        arguments: &[SqlArg<'_>],
    ) -> tidb_sqlexec::Result<Option<Box<dyn RecordSet>>> {
        let _operation = self
            .internal
            .enter(&self.owner, false)
            .map_err(|error| -> tidb_sqlexec::SqlExecError { Box::new(error) })?;
        self.internal
            .context()
            .sql_executor()
            .execute_internal(context, sql, arguments)
    }

    fn execute_stmt(
        &self,
        context: &dyn ExecutionContext,
        statement: &Stmt,
    ) -> tidb_sqlexec::Result<Option<Box<dyn RecordSet>>> {
        let _operation = self
            .internal
            .enter(&self.owner, false)
            .map_err(|error| -> tidb_sqlexec::SqlExecError { Box::new(error) })?;
        self.internal
            .context()
            .sql_executor()
            .execute_stmt(context, statement)
    }
}

impl<C: SessionContext + ?Sized + 'static> RestrictedSqlExecutor for Session<C> {
    fn parse_with_params(
        &self,
        context: &dyn ExecutionContext,
        sql: &str,
        arguments: &[SqlArg<'_>],
    ) -> tidb_sqlexec::Result<Stmt> {
        let _operation = self
            .internal
            .enter(&self.owner, false)
            .map_err(|error| -> tidb_sqlexec::SqlExecError { Box::new(error) })?;
        self.internal
            .context()
            .restricted_sql_executor()
            .parse_with_params(context, sql, arguments)
    }

    fn exec_restricted_stmt(
        &self,
        context: &dyn ExecutionContext,
        statement: &Stmt,
        options: &[OptionFuncAlias],
    ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>)> {
        let _operation = self
            .internal
            .enter(&self.owner, false)
            .map_err(|error| -> tidb_sqlexec::SqlExecError { Box::new(error) })?;
        self.internal
            .context()
            .restricted_sql_executor()
            .exec_restricted_stmt(context, statement, options)
    }

    fn exec_restricted_sql(
        &self,
        context: &dyn ExecutionContext,
        options: &[OptionFuncAlias],
        sql: &str,
        arguments: &[SqlArg<'_>],
    ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>)> {
        let _operation = self
            .internal
            .enter(&self.owner, false)
            .map_err(|error| -> tidb_sqlexec::SqlExecError { Box::new(error) })?;
        self.internal
            .context()
            .restricted_sql_executor()
            .exec_restricted_sql(context, options, sql, arguments)
    }
}

struct PoolOwner;

impl<C: SessionContext + ?Sized> OwnerHook<C> for PoolOwner {
    fn on_became_owner(&self, _context: &C) -> Result<()> {
        Ok(())
    }

    fn on_resign_owner(&self, _context: &C) -> Result<()> {
        Ok(())
    }
}

struct PoolState<C: SessionContext + ?Sized> {
    sessions: VecDeque<Arc<InternalSession<C>>>,
    closed: bool,
}

/// Recyclable pool of system-internal sessions.
pub struct AdvancedSessionPool<C: SessionContext + ?Sized + 'static = dyn SessionContext> {
    capacity: usize,
    factory: Arc<dyn Fn() -> Result<Arc<C>> + Send + Sync>,
    owner: Owner<C>,
    state: Mutex<PoolState<C>>,
}

/// Object-safe Go `Pool` surface used by package consumers.
pub trait Pool<C: SessionContext + ?Sized = dyn SessionContext>: Send + Sync {
    /// Gets a session from the pool.
    fn get(&self) -> Result<Session<C>>;
    /// Returns a session to the pool.
    fn put(&self, session: &Session<C>);
    /// Runs a callback and automatically returns a successful session.
    fn with_session(
        &self,
        callback: &mut dyn FnMut(&Session<C>) -> tidb_sqlexec::Result<()>,
    ) -> tidb_sqlexec::Result<()>;
    /// Runs a callback with a session registered strongly enough to block GC.
    fn with_force_block_gc_session(
        &self,
        cancelled: &dyn Fn() -> bool,
        callback: &mut dyn FnMut(&Session<C>) -> tidb_sqlexec::Result<()>,
    ) -> tidb_sqlexec::Result<()>;
    /// Closes the pool.
    fn close(&self);
}

impl<C: SessionContext + ?Sized + 'static> AdvancedSessionPool<C> {
    /// Constructs a pool with Go's capacity normalization.
    pub fn new(
        capacity: i64,
        factory: impl Fn() -> Result<Arc<C>> + Send + Sync + 'static,
    ) -> Self {
        let capacity = if capacity <= 0 || capacity > POOL_MAX_SIZE as i64 {
            POOL_MAX_SIZE
        } else {
            capacity as usize
        };
        Self {
            capacity,
            factory: Arc::new(factory),
            owner: owner(Arc::new(PoolOwner)),
            state: Mutex::new(PoolState {
                sessions: VecDeque::new(),
                closed: false,
            }),
        }
    }

    /// Gets a session, reusing an idle entry before calling the factory.
    pub fn get(&self) -> Result<Session<C>> {
        let cached = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed {
                return Err(SysSessionError::new("session pool closed"));
            }
            state.sessions.pop_front()
        };
        let internal = match cached {
            Some(internal) => internal,
            None => {
                let context = (self.factory)()?;
                let created = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    InternalSession::new(Arc::clone(&context), self.owner.clone())
                }));
                match created {
                    Ok(Ok(internal)) => internal,
                    Ok(Err(error)) => {
                        context.close();
                        return Err(error);
                    }
                    Err(payload) => {
                        context.close();
                        std::panic::resume_unwind(payload);
                    }
                }
            }
        };
        Session::from_pool(internal, &self.owner)
    }

    /// Puts a clean, reusable session back into the pool.
    pub fn put(&self, session: &Session<C>) {
        if !session.is_owner() {
            return;
        }
        if session
            .internal
            .transfer_owner(&session.owner, self.owner.clone())
            .is_err()
        {
            session.close();
            return;
        }
        let mut cleanup = CloseUnlessReturned {
            internal: Some(Arc::clone(&session.internal)),
        };
        if session.internal.is_avoid_reuse() || session.internal.check_no_pending_txn().is_err() {
            session.internal.close();
            return;
        }
        let background = tidb_sqlexec::BackgroundContext;
        let context = session.internal.context();
        let reset = session
            .internal
            .enter(&self.owner, false)
            .map(|_operation| context.rollback_txn(&background));
        if reset.is_err() {
            session.internal.close();
            return;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed || state.sessions.len() == self.capacity {
            drop(state);
            session.internal.close();
            return;
        }
        state.sessions.push_back(Arc::clone(&session.internal));
        cleanup.internal = None;
    }

    /// Runs a callback and returns only a successful, clean session.
    pub fn with_session<T>(&self, callback: impl FnOnce(&Session<C>) -> Result<T>) -> Result<T> {
        let session = self.get()?;
        let mut lease = CheckedOutSession {
            session: &session,
            returned: false,
        };
        let result = match callback(&session) {
            Ok(value) => {
                self.put(&session);
                lease.returned = true;
                Ok(value)
            }
            Err(error) => Err(error),
        };
        result
    }

    /// Go `WithForceBlockGCSession`.
    pub fn with_force_block_gc_session<T>(
        &self,
        cancelled: &dyn Fn() -> bool,
        callback: impl FnOnce(&Session<C>) -> Result<T>,
    ) -> Result<T> {
        self.with_session(|session| {
            let context = session.internal.context();
            if !context.contains_internal_session() {
                while !context.store_internal_session() {
                    if cancelled() {
                        return Err(SysSessionError::new("context canceled"));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
            callback(session)
        })
    }

    /// Closes the pool and every idle session.
    pub fn close(&self) {
        let sessions = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed {
                return;
            }
            state.closed = true;
            std::mem::take(&mut state.sessions)
        };
        for session in sessions {
            session.close();
        }
    }

    /// Whether the pool is closed.
    pub fn is_closed(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed
    }

    /// Number of idle sessions in the pool.
    pub fn size(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sessions
            .len()
    }

    /// Configured channel capacity after Go normalization.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

struct CloseUnlessReturned<C: SessionContext + ?Sized + 'static> {
    internal: Option<Arc<InternalSession<C>>>,
}

struct CheckedOutSession<'session, C: SessionContext + ?Sized + 'static> {
    session: &'session Session<C>,
    returned: bool,
}

impl<C: SessionContext + ?Sized + 'static> Drop for CheckedOutSession<'_, C> {
    fn drop(&mut self) {
        if !self.returned {
            self.session.close();
        }
    }
}

impl<C: SessionContext + ?Sized + 'static> Drop for CloseUnlessReturned<C> {
    fn drop(&mut self) {
        if let Some(internal) = self.internal.take() {
            internal.close();
        }
    }
}

impl<C: SessionContext + ?Sized + 'static> Pool<C> for AdvancedSessionPool<C> {
    fn get(&self) -> Result<Session<C>> {
        AdvancedSessionPool::get(self)
    }

    fn put(&self, session: &Session<C>) {
        AdvancedSessionPool::put(self, session);
    }

    fn with_session(
        &self,
        callback: &mut dyn FnMut(&Session<C>) -> tidb_sqlexec::Result<()>,
    ) -> tidb_sqlexec::Result<()> {
        let session = self
            .get()
            .map_err(|error| Box::new(error) as tidb_sqlexec::SqlExecError)?;
        let mut lease = CheckedOutSession {
            session: &session,
            returned: false,
        };
        callback(&session)?;
        self.put(&session);
        lease.returned = true;
        Ok(())
    }

    fn with_force_block_gc_session(
        &self,
        cancelled: &dyn Fn() -> bool,
        callback: &mut dyn FnMut(&Session<C>) -> tidb_sqlexec::Result<()>,
    ) -> tidb_sqlexec::Result<()> {
        <Self as Pool<C>>::with_session(self, &mut |session: &Session<C>| {
            let context = session.internal.context();
            if !context.contains_internal_session() {
                while !context.store_internal_session() {
                    if cancelled() {
                        return Err(Box::new(SysSessionError::new("context canceled"))
                            as tidb_sqlexec::SqlExecError);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
            callback(session)
        })
    }

    fn close(&self) {
        AdvancedSessionPool::close(self);
    }
}

impl<C: SessionContext + ?Sized + 'static> Drop for AdvancedSessionPool<C> {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use tidb_sqlexec::BackgroundContext;

    #[derive(Default)]
    struct NoopExecutor {
        calls: Mutex<Vec<String>>,
    }

    impl SqlExecutor for NoopExecutor {
        fn execute(
            &self,
            _context: &dyn ExecutionContext,
            _sql: &str,
        ) -> tidb_sqlexec::Result<Vec<Box<dyn RecordSet>>> {
            self.calls.lock().unwrap().push(format!("execute:{_sql}"));
            Ok(Vec::new())
        }

        fn execute_internal(
            &self,
            _context: &dyn ExecutionContext,
            _sql: &str,
            _arguments: &[SqlArg<'_>],
        ) -> tidb_sqlexec::Result<Option<Box<dyn RecordSet>>> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("internal:{_sql}:{}", _arguments.len()));
            Ok(None)
        }

        fn execute_stmt(
            &self,
            _context: &dyn ExecutionContext,
            _statement: &Stmt,
        ) -> tidb_sqlexec::Result<Option<Box<dyn RecordSet>>> {
            self.calls.lock().unwrap().push("statement".to_owned());
            Ok(None)
        }
    }

    impl RestrictedSqlExecutor for NoopExecutor {
        fn parse_with_params(
            &self,
            _context: &dyn ExecutionContext,
            sql: &str,
            arguments: &[SqlArg<'_>],
        ) -> tidb_sqlexec::Result<Stmt> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("parse:{sql}:{}", arguments.len()));
            let escaped = tidb_util::sqlescape::escape_sql(sql, arguments)?;
            let sql = String::from_utf8(escaped)?;
            tidb_parser::parse(&sql).map_err(|error| {
                Box::new(std::io::Error::other(format!("{error:?}"))) as tidb_sqlexec::SqlExecError
            })
        }

        fn exec_restricted_stmt(
            &self,
            _context: &dyn ExecutionContext,
            _statement: &Stmt,
            _options: &[OptionFuncAlias],
        ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>)> {
            self.calls
                .lock()
                .unwrap()
                .push("restricted-statement".to_owned());
            Ok((Vec::new(), Vec::new()))
        }

        fn exec_restricted_sql(
            &self,
            _context: &dyn ExecutionContext,
            _options: &[OptionFuncAlias],
            _sql: &str,
            _arguments: &[SqlArg<'_>],
        ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<ResultFieldRef>)> {
            self.calls.lock().unwrap().push("restricted-sql".to_owned());
            Ok((Vec::new(), Vec::new()))
        }
    }

    struct MockContext {
        closed: AtomicUsize,
        registered: AtomicBool,
        pending: AtomicBool,
        valid: AtomicBool,
        panic_register: AtomicBool,
        rollbacks: AtomicUsize,
        executor: Arc<NoopExecutor>,
    }

    impl MockContext {
        fn new() -> Self {
            Self {
                closed: AtomicUsize::new(0),
                registered: AtomicBool::new(false),
                pending: AtomicBool::new(false),
                valid: AtomicBool::new(false),
                panic_register: AtomicBool::new(false),
                rollbacks: AtomicUsize::new(0),
                executor: Arc::new(NoopExecutor::default()),
            }
        }
    }

    impl SessionContext for MockContext {
        fn close(&self) {
            self.closed.fetch_add(1, Ordering::SeqCst);
        }

        fn rollback_txn(&self, _context: &dyn ExecutionContext) {
            self.rollbacks.fetch_add(1, Ordering::SeqCst);
        }

        fn has_prepared_txn_future(&self) -> bool {
            self.pending.load(Ordering::SeqCst)
        }

        fn txn_valid(&self) -> std::result::Result<bool, tidb_sqlexec::SqlExecError> {
            Ok(self.valid.load(Ordering::SeqCst))
        }

        fn sql_executor(&self) -> Arc<dyn SqlExecutor> {
            self.executor.clone()
        }

        fn restricted_sql_executor(&self) -> Arc<dyn RestrictedSqlExecutor> {
            self.executor.clone()
        }

        fn register_internal_session(&self) {
            if self.panic_register.load(Ordering::SeqCst) {
                panic!("register panic");
            }
            self.registered.store(true, Ordering::SeqCst);
        }

        fn unregister_internal_session(&self) {
            self.registered.store(false, Ordering::SeqCst);
        }

        fn contains_internal_session(&self) -> bool {
            self.registered.load(Ordering::SeqCst)
        }

        fn store_internal_session(&self) -> bool {
            self.registered.store(true, Ordering::SeqCst);
            true
        }
    }

    #[test]
    fn new_pool_normalizes_capacity() {
        let factory = || Ok(Arc::new(MockContext::new()) as Arc<dyn SessionContext>);
        assert_eq!(AdvancedSessionPool::new(128, factory).capacity(), 128);
        assert_eq!(
            AdvancedSessionPool::new(0, || Ok(Arc::new(MockContext::new()))).capacity(),
            POOL_MAX_SIZE
        );
        assert_eq!(
            AdvancedSessionPool::new(-1, || Ok(Arc::new(MockContext::new()))).capacity(),
            POOL_MAX_SIZE
        );
    }

    #[test]
    fn get_put_reuses_internal_session_and_registry_tracks_owner() {
        let contexts = Arc::new(Mutex::new(Vec::<Arc<MockContext>>::new()));
        let factory_contexts = Arc::clone(&contexts);
        let pool = AdvancedSessionPool::new(4, move || {
            let context = Arc::new(MockContext::new());
            factory_contexts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(Arc::clone(&context));
            Ok(context)
        });

        let first = pool.get().unwrap();
        let internal = Arc::as_ptr(&first.internal);
        assert!(first.is_owner());
        assert!(contexts.lock().unwrap()[0]
            .registered
            .load(Ordering::SeqCst));
        pool.put(&first);
        assert!(!first.is_owner());
        assert_eq!(pool.size(), 1);
        assert!(!contexts.lock().unwrap()[0]
            .registered
            .load(Ordering::SeqCst));

        let second = pool.get().unwrap();
        assert_eq!(Arc::as_ptr(&second.internal), internal);
        assert!(second.is_owner());
        assert_eq!(pool.size(), 0);
    }

    #[test]
    fn get_closes_new_context_when_owner_registration_panics() {
        let context = Arc::new(MockContext::new());
        context.panic_register.store(true, Ordering::SeqCst);
        let factory_context = Arc::clone(&context);
        let pool = AdvancedSessionPool::new(1, move || Ok(Arc::clone(&factory_context)));

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| pool.get()));
        assert!(result.is_err());
        assert_eq!(context.closed.load(Ordering::SeqCst), 1);
        assert_eq!(pool.size(), 0);
    }

    #[test]
    fn dirty_sessions_are_closed_instead_of_reused() {
        let context = Arc::new(MockContext::new());
        let factory_context = Arc::clone(&context);
        let pool = AdvancedSessionPool::new(1, move || Ok(factory_context.clone()));
        let session = pool.get().unwrap();
        session.avoid_reuse();
        pool.put(&session);
        assert!(session.is_internal_closed());
        assert_eq!(pool.size(), 0);
        assert_eq!(context.closed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn close_during_operation_defers_context_close() {
        let context = Arc::new(MockContext::new());
        let session = Session::new_for_test(context.clone()).unwrap();
        let operation = session.internal.enter(&session.owner, true).unwrap();
        session.close();
        assert!(session.is_internal_closed());
        assert_eq!(context.closed.load(Ordering::SeqCst), 0);
        drop(operation);
        assert_eq!(context.closed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn panic_marks_session_avoid_reuse_and_releases_operation() {
        let context = Arc::new(MockContext::new());
        let session = Session::new_for_test(context).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _operation = session.internal.enter(&session.owner, false).unwrap();
            panic!("panicTest");
        }));
        assert!(result.is_err());
        assert!(session.is_avoid_reuse());
        assert_eq!(session.internal.state.lock().unwrap().in_use, 0);
    }

    #[test]
    fn concurrent_thread_unsafe_entry_is_rejected_until_first_exit() {
        let context = Arc::new(MockContext::new());
        let session = Session::new_for_test(context).unwrap();
        let first = session.internal.enter(&session.owner, false).unwrap();
        let error = match session.internal.enter(&session.owner, false) {
            Ok(_) => panic!("second thread-unsafe operation unexpectedly entered"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "EnterOperation error: race detected for concurrent thread-unsafe operations"
        );
        drop(first);
        assert!(session.internal.enter(&session.owner, false).is_ok());
    }

    #[test]
    fn with_session_closes_on_error_and_returns_on_success() {
        let contexts = Arc::new(Mutex::new(Vec::<Arc<MockContext>>::new()));
        let factory_contexts = Arc::clone(&contexts);
        let pool = AdvancedSessionPool::new(2, move || {
            let context = Arc::new(MockContext::new());
            factory_contexts.lock().unwrap().push(context.clone());
            Ok(context)
        });
        assert_eq!(pool.with_session(|_| Ok(())).unwrap(), ());
        assert_eq!(pool.size(), 1);
        let error = pool
            .with_session::<()>(|_| Err(SysSessionError::new("mockErr")))
            .unwrap_err();
        assert_eq!(error.to_string(), "mockErr");
        assert_eq!(pool.size(), 0);
        assert_eq!(contexts.lock().unwrap()[0].closed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn with_session_closes_on_panic() {
        let context = Arc::new(MockContext::new());
        let factory_context = Arc::clone(&context);
        let pool = AdvancedSessionPool::new(1, move || Ok(factory_context.clone()));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = pool.with_session::<()>(|_| panic!("mockPanic"));
        }));
        assert!(result.is_err());
        assert_eq!(context.closed.load(Ordering::SeqCst), 1);
        assert_eq!(pool.size(), 0);
    }

    #[test]
    fn pool_close_is_idempotent_and_rejects_get() {
        let pool = AdvancedSessionPool::new(1, || Ok(Arc::new(MockContext::new())));
        let session = pool.get().unwrap();
        pool.put(&session);
        pool.close();
        pool.close();
        assert!(pool.is_closed());
        assert!(matches!(
            pool.get(),
            Err(error) if error == SysSessionError::new("session pool closed")
        ));
    }

    #[test]
    fn pending_or_valid_transaction_prevents_pool_reuse() {
        let pending = Arc::new(MockContext::new());
        pending.pending.store(true, Ordering::SeqCst);
        let pending_factory = Arc::clone(&pending);
        let pool = AdvancedSessionPool::new(1, move || Ok(pending_factory.clone()));
        let session = pool.get().unwrap();
        pool.put(&session);
        assert!(session.is_internal_closed());
        assert_eq!(pool.size(), 0);

        let valid = Arc::new(MockContext::new());
        valid.valid.store(true, Ordering::SeqCst);
        let valid_factory = Arc::clone(&valid);
        let pool = AdvancedSessionPool::new(1, move || Ok(valid_factory.clone()));
        let session = pool.get().unwrap();
        pool.put(&session);
        assert!(session.is_internal_closed());
        assert_eq!(pool.size(), 0);
    }

    #[test]
    fn reset_context_requires_current_owner() {
        let first = Arc::new(MockContext::new());
        let second = Arc::new(MockContext::new());
        let session = Session::new_for_test(first.clone()).unwrap();
        let replacement = second.clone();
        session
            .reset_context_for_test(move |_| replacement)
            .unwrap();
        assert!(Arc::ptr_eq(&session.internal_context_for_test(), &second));

        let pool_context = Arc::new(MockContext::new());
        let factory_context = pool_context.clone();
        let pool = AdvancedSessionPool::new(1, move || Ok(factory_context.clone()));
        let checked_out = pool.get().unwrap();
        pool.put(&checked_out);
        assert!(checked_out
            .reset_context_for_test(|context| context)
            .is_err());
    }

    #[test]
    fn session_proxies_both_executor_interfaces_through_one_operation_path() {
        let context = Arc::new(MockContext::new());
        let session = Session::new_for_test(context.clone()).unwrap();
        let request = BackgroundContext;
        session.execute(&request, "select 1").unwrap();
        session
            .execute_internal(&request, "select %?", &[SqlArg::Signed(1)])
            .unwrap();
        session
            .parse_with_params(&request, "select %?", &[SqlArg::Signed(1)])
            .unwrap();
        session
            .exec_restricted_sql(&request, &[], "select 1", &[])
            .unwrap();
        assert_eq!(
            *context.executor.calls.lock().unwrap(),
            [
                "execute:select 1",
                "internal:select %?:1",
                "parse:select %?:1",
                "restricted-sql",
            ]
        );
        assert_eq!(session.internal.state.lock().unwrap().in_use, 0);
    }

    struct FailingOwner;

    impl OwnerHook<MockContext> for FailingOwner {
        fn on_became_owner(&self, _context: &MockContext) -> Result<()> {
            Err(SysSessionError::new("mockOnBecameOwner"))
        }

        fn on_resign_owner(&self, _context: &MockContext) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn transfer_owner_failure_closes_internal_session() {
        let context = Arc::new(MockContext::new());
        let original = owner::<MockContext>(Arc::new(NoopOwner));
        let internal = InternalSession::new(context.clone(), original.clone()).unwrap();
        let replacement = owner::<MockContext>(Arc::new(FailingOwner));
        assert_eq!(
            internal
                .transfer_owner(&original, replacement)
                .unwrap_err()
                .to_string(),
            "mockOnBecameOwner"
        );
        assert!(internal.is_closed());
        assert_eq!(context.closed.load(Ordering::SeqCst), 1);
    }
}
