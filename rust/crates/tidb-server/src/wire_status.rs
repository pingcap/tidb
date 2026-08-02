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

//! The server status word every OK/EOF packet carries.
//!
//! Go threads the LIVE session status into every packet it writes: `status :=
//! cc.ctx.Status()` in `clientConn.handleStmt` (`pkg/server/conn.go`) reads
//! `session.Status()` -> `SessionVars.Status()` -> the atomic bitfield in
//! `pkg/sessionctx/variable/session.go`, and passes that word to
//! `writeResultSet`/`writeOkWith`/`writeEOF`. A connection-lifetime constant is
//! not an approximation of that -- it is a different value, and Connector/J
//! with `useLocalTransactionState=true` acts on the difference by skipping the
//! `COMMIT` for a transaction it was told is not open.
//!
//! So this type exists to make the constant unrepresentable at the packet
//! writers. Every OK/EOF site in `mysql_connection` takes a [`WireStatus`],
//! which can be produced only from a session's own state (or from the two
//! documented session-less defaults), never from a `u16` literal.
//!
//! The bits, from `pkg/parser/mysql/const.go`:
//!
//! ```text
//! ServerStatusInTrans      uint16 = 0x0001
//! ServerStatusAutocommit   uint16 = 0x0002
//! ServerMoreResultsExists  uint16 = 0x0008
//! ServerStatusCursorExists uint16 = 0x0040
//! ServerStatusLastRowSend  uint16 = 0x0080
//! ```

use tidb_session::Session;

/// `ServerStatusInTrans` (`pkg/parser/mysql/const.go:120`).
///
/// Go sets it through `SessionVars.SetInTxn(true)`
/// (`pkg/sessionctx/variable/session.go:2796`) at two moments: the explicit
/// `BEGIN`/`START TRANSACTION` (`pkg/sessiontxn/isolation/base.go:114`) and the
/// LAZY start a non-autocommit session performs before an ordinary statement
/// (`pkg/sessiontxn/isolation/base.go:323`, guarded by `!sessVars.IsAutocommit()`).
/// It is cleared by `COMMIT` (`pkg/executor/simple.go:792`), by `ROLLBACK`
/// (`pkg/executor/simple.go:814`, but NOT by `ROLLBACK TO <savepoint>`, which
/// returns before that line), and by the implicit commit `SET autocommit=1`
/// performs while a transaction is open (`pkg/sessionctx/variable/sysvar.go:2121`).
pub const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
/// `ServerStatusAutocommit` (`pkg/parser/mysql/const.go:121`).
///
/// It is the initial value of the whole word --
/// `vars.status.Store(uint32(mysql.ServerStatusAutocommit))`
/// (`pkg/sessionctx/variable/session.go:2505`) -- and it TRACKS the `autocommit`
/// variable: `SetSession` for `AutoCommit` runs
/// `s.SetStatusFlag(mysql.ServerStatusAutocommit, isAutocommit)`
/// (`pkg/sessionctx/variable/sysvar.go:2123`), so `SET autocommit=0` clears it.
pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
/// `ServerMoreResultsExists` (`pkg/parser/mysql/const.go:122`).
///
/// Go ORs it onto the live word for every statement of a multi-statement
/// COM_QUERY except the last: `if lastStmt { ... } else { status |=
/// mysql.ServerMoreResultsExists }` (`pkg/server/conn.go`, right after
/// `status := cc.ctx.Status()`).
pub const SERVER_MORE_RESULTS_EXISTS: u16 = 0x0008;
/// `ServerStatusCursorExists` (`pkg/parser/mysql/const.go:125`): a read-only
/// cursor opened by `COM_STMT_EXECUTE` with `CURSOR_TYPE_READ_ONLY` is still
/// open, so the client fetches with `COM_STMT_FETCH` instead of reading rows.
pub const SERVER_STATUS_CURSOR_EXISTS: u16 = 0x0040;
/// `ServerStatusLastRowSend` (`pkg/parser/mysql/const.go:126`): the fetch that
/// carried this EOF drained the cursor, which is retired in the same breath --
/// so this bit arrives with `SERVER_STATUS_CURSOR_EXISTS` cleared.
pub const SERVER_STATUS_LAST_ROW_SEND: u16 = 0x0080;

/// One server status word, on its way to one OK or EOF packet.
///
/// There is deliberately no `From<u16>`: a status can be read off a session
/// ([`WireStatus::of_session`]), inherited from the two session-less defaults
/// below, or derived from another status by setting the per-packet bits
/// ([`WireStatus::with`] / [`WireStatus::without`]). Nothing else produces one,
/// which is what keeps a fourth hardcoded `SERVER_STATUS_AUTOCOMMIT` from
/// growing back at a packet writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WireStatus(u16);

impl WireStatus {
    /// The word Go's `SessionVars` is born with
    /// (`pkg/sessionctx/variable/session.go:2505`): autocommit on, no
    /// transaction. It is the honest answer for a session that has no
    /// transaction concept at all -- such a session is always in autocommit and
    /// never in a transaction -- and it is what Go's `writeInitialHandshake`
    /// hardcodes too (`pkg/server/conn.go:496`,
    /// `data = dump.Uint16(data, mysql.ServerStatusAutocommit)`), since the
    /// handshake precedes any session.
    pub const AUTOCOMMIT: Self = Self(SERVER_STATUS_AUTOCOMMIT);

    /// Reads the live status off a session, the way `cc.ctx.Status()` does.
    ///
    /// The session owns both facts already, so this derives rather than
    /// duplicates: there is no second copy of "am I in a transaction" that can
    /// drift from the one the session acts on.
    #[must_use]
    pub fn of_session(session: &Session) -> Self {
        Self::of_facts(session.is_autocommit(), session.in_transaction())
    }

    /// Assembles the word from the two session facts Go keeps in it.
    ///
    /// This takes the FACTS, never a bit pattern, which is what keeps the
    /// assembly in one place: a caller that owns its transaction state some
    /// other way than a [`Session`] still cannot express a word Go would not.
    #[must_use]
    pub const fn of_facts(autocommit: bool, in_transaction: bool) -> Self {
        let mut status = 0;
        if autocommit {
            status |= SERVER_STATUS_AUTOCOMMIT;
        }
        if in_transaction {
            status |= SERVER_STATUS_IN_TRANS;
        }
        Self(status)
    }

    /// The status of a session that owns a transaction but no `autocommit`
    /// variable of its own.
    ///
    /// Such a session is permanently in autocommit -- there is no `SET
    /// autocommit` for it to have run -- so the autocommit bit is a constant of
    /// its nature rather than a guess, and only the transaction bit varies.
    /// This takes the FACT, not the word: a caller supplies "am I in a
    /// transaction", never a bit pattern.
    #[must_use]
    pub const fn autocommit_session(in_transaction: bool) -> Self {
        Self::of_facts(true, in_transaction)
    }

    /// Sets the per-packet bits a session does not own -- more results, cursor
    /// exists, last row sent.
    #[must_use]
    pub const fn with(self, flag: u16) -> Self {
        Self(self.0 | flag)
    }

    /// Clears a per-packet bit; the drained cursor's EOF clears
    /// [`SERVER_STATUS_CURSOR_EXISTS`] as it sets
    /// [`SERVER_STATUS_LAST_ROW_SEND`].
    #[must_use]
    pub const fn without(self, flag: u16) -> Self {
        Self(self.0 & !flag)
    }

    /// The word as it goes on the wire.
    #[must_use]
    pub const fn bits(self) -> u16 {
        self.0
    }
}

impl Default for WireStatus {
    fn default() -> Self {
        Self::AUTOCOMMIT
    }
}
