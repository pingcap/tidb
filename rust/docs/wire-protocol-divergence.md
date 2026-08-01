# MySQL wire-protocol divergence inventory

File-by-file semantic comparison of TiDB's Go wire implementation against the
Rust one.

| side | files |
| --- | --- |
| Go | `pkg/server/conn.go`, `pkg/server/conn_stmt.go`, `pkg/server/driver_tidb.go`, `pkg/server/internal/packetio.go`, `pkg/server/internal/column/{column,convert}.go`, `pkg/format/textrow/result_encoder.go`, `pkg/parser/mysql/{const,charset,state}.go` |
| Rust | `rust/crates/tidb-protocol/src/*`, `rust/crates/tidb-mysql/src/charset.rs`, `rust/crates/tidb-error/src/mysql/state.rs`, `rust/crates/tidb-exec/src/result_metadata.rs`, `rust/crates/tidb-server/src/{handshake,mysql_connection,resultset_writer}.rs` |

**Nothing here was executed.** This machine cannot run a freshly built binary
(`syspolicyd` is wedged; every new executable hangs at `_dyld_start`), so no
client was ever pointed at a Rust node. Every claim below is source-derived,
with the Go file:line, the Rust file:line, and the client action that produces
different bytes. Line numbers on the Rust side are as of commit `b05550e670`.

Counts: **12 divergences** (2 rank-1, 3 rank-2, 5 rank-3, 2 rank-4) and
**11 verified-equal areas**.

---

## Rank 1 — the client breaks or desynchronises (the 2014 class)

### D1. `COM_STMT_SEND_LONG_DATA` is answered with an ERR packet; Go answers with nothing

- Go: `pkg/server/conn.go:1578-1579` dispatches `mysql.ComStmtSendLongData` to
  `handleStmtSendLongData` (`pkg/server/conn_stmt.go:610-625`), which returns
  `nil` after `stmt.AppendParam`. A `nil` return from `dispatch` writes **no
  packet at all** — MySQL's `COM_STMT_SEND_LONG_DATA` has no response.
- Rust: `rust/crates/tidb-protocol/src/command.rs:24-46` has no constant for
  `0x18`, so `decode_command` returns `Command::Unknown { code: 0x18, .. }`,
  and `rust/crates/tidb-server/src/mysql_connection.rs:1453-1463` answers every
  `Unknown` with an ERR packet at sequence 1.

Distinguishing case: any client that binds a `TEXT`/`BLOB`/large parameter —
JDBC `PreparedStatement.setBinaryStream`, `setBlob`, or any parameter over
`blobSendChunkSize`; Python `mysqlclient` with a large `bytes` argument; the C
API `mysql_stmt_send_long_data`.

```
C: COM_STMT_PREPARE "INSERT INTO t VALUES (?)"    S: PREPARE_OK
C: COM_STMT_SEND_LONG_DATA(stmt, param 0, bytes)  S: (Go: nothing) (Rust: ERR 1047)
C: COM_STMT_EXECUTE(stmt)                         S: OK / result set
```

The client does not read after `SEND_LONG_DATA`. Our extra ERR sits in the
socket, is consumed as the answer to `COM_STMT_EXECUTE` (its sequence id 1 is
valid there, so nothing rejects it), and the real execute response is left
behind. From that point every response is off by one — the exact shape of the
error 2014 "Commands out of sync" bug this project already hit on a binary-
protocol range select. Every subsequent command on the connection is wrong.

Not a small fix: correct behavior needs a per-statement long-data parameter
buffer (Go's `TiDBStatement.AppendParam` / `BoundParams`), which
`rust/crates/tidb-protocol/src/binary_params.rs:170-200` already accepts as
`bound_params` but no server code ever fills. Writing nothing instead of the
ERR would fix the framing but silently drop the parameter, which is worse.

### D2. Every OK and EOF packet claims `SERVER_STATUS_AUTOCOMMIT` and never `SERVER_STATUS_IN_TRANS`

- Go: `pkg/server/conn.go:2265` takes `status := cc.ctx.Status()` — the live
  session status — and threads it through `writeResultSet`/`writeChunks` into
  every metadata and terminal `writeEOF` (`conn.go:2589,2630`), and
  `writeOkWith` (`conn.go:1688-1721`) writes `cc.ctx.Status()` likewise. So
  `SERVER_STATUS_IN_TRANS` (0x0001) is set on every packet while a transaction
  is open, and `SERVER_STATUS_AUTOCOMMIT` (0x0002) is cleared after
  `SET autocommit=0`.
- Rust: `rust/crates/tidb-server/src/mysql_connection.rs:696` builds the
  connection-lifetime `ResultSetOptions { status_flags: SERVER_STATUS_AUTOCOMMIT, .. }`
  once, and every result set inherits it (`:830, :1080, :1154`).
  `write_affected_rows_ok` hardcodes it again at `:1480`, and `write_ok` at `:1657`. Only `write_transaction_control_ok` (`:1490-1509`) ever ORs in
  `SERVER_STATUS_IN_TRANS`, and only for the `BEGIN`/`COMMIT`/`ROLLBACK`
  statement itself.

Distinguishing case:

```
C: BEGIN                  S: OK status=0x0003 (both)
C: INSERT INTO t VALUES(1) S: Go OK status=0x0003; Rust OK status=0x0002
C: SELECT * FROM t         S: Go EOF status=0x0003; Rust EOF status=0x0002
C: COMMIT                  S: OK status=0x0002 (both)
```

After the first statement inside the transaction we tell the client the
transaction is gone. Connector/J with `useLocalTransactionState=true` (a common
production setting) then **skips sending `COMMIT` entirely** because
`inTransactionOnServer()` is false — the writes are silently lost when the
connection returns to the pool and is reset. The same flag drives whether
`Connection.close()` issues a rollback. Separately, `SET autocommit=0` never
clears bit 0x0002, so a client reading autocommit from the status flags
believes autocommit is still on.

Not a small fix: the status word has to come from the session
(`QuerySession`) per statement, not from a connection-lifetime constant.

---

## Rank 2 — the client silently misinterprets data

### D3. The column-definition charset id is the column's collation id, not its charset's default collation id

- Go: `pkg/server/internal/column/convert.go:31` sets
  `Charset: uint16(mysql.CharsetNameToID(fld.Column.GetCharset()))` — the id is
  derived from the **charset name**, so it is always that charset's default
  collation (`pkg/parser/mysql/charset.go:19-34`).
- Rust: `rust/crates/tidb-exec/src/result_metadata.rs:214` sets
  `charset: collation_id(field.field_type.collation)` — the column's **actual**
  collation id.

Distinguishing case: `CREATE TABLE t (c VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci); SELECT c FROM t;`
Go puts `46` (`utf8mb4_bin`) in the column definition's two charset bytes;
Rust puts `224`. With `COLLATE utf8mb4_general_ci` Go still puts `46`, Rust
puts `45`. Any client that surfaces `charsetnr` — `mysql_fetch_fields()`,
JDBC `ResultSetMetaData`, ORM schema reflection — reads a different value for
the identical table.

### D4. `@@character_set_results` is ignored on the wire — both the advertised charset id and the row bytes

- Go: `pkg/server/internal/column/column.go:76` writes
  `d.ColumnCharsetID(column.dumpCharset(), textrow.IsStringColumnType(column.Type))`,
  which (`pkg/format/textrow/result_encoder.go:104-112`) replaces a non-binary
  string column's id with `@@character_set_results`' default collation. Row
  values go through `d.EncodeData(...)` (`column.go:162-179` text,
  `:181-232` binary), which transcodes them (`result_encoder.go:137-148`).
  `cc.initResultEncoder` builds the encoder per statement (`conn.go:1103`).
- Rust: `ColumnInfo::dump_charset` (`rust/crates/tidb-protocol/src/column.rs:165-171`)
  returns `self.charset` verbatim. `ResultEncoder` exists and is faithful
  (`rust/crates/tidb-protocol/src/result_encoder.rs`) but **nothing in
  `tidb-server` ever constructs one** — grep for `ResultEncoder` across
  `rust/crates/tidb-server/src` returns no hits. `character_set_results` is a
  settable sysvar (`rust/crates/tidb-session/src/sysvar/catalog/types_and_expressions.rs:64`)
  with no wire effect.

Distinguishing case: connect with `charset=latin1` (go-sql-driver `?charset=latin1`,
JDBC `characterEncoding=latin1`, or plain `SET character_set_results = latin1`),
then `SELECT 'é'` from a utf8mb4 column. Go advertises collation `47` and sends
the single byte `0xE9`. Rust advertises the column's own utf8mb4 id and sends
the two bytes `0xC3 0xA9`; the client, told to expect latin1 by its own
session setting, renders `Ã©`. Silent mojibake with no error anywhere.

### D5. `CLIENT_TRANSACTIONS` and `CLIENT_FOUND_ROWS` are not advertised

- Go: `pkg/server/server.go:116-121` `defaultCapability` includes
  `ClientTransactions` (1<<13) and `ClientFoundRows` (1<<1).
- Rust: `rust/crates/tidb-server/src/mysql_connection.rs:107-112`
  `SERVER_CAPABILITIES` is exactly
  `CLIENT_PROTOCOL_41 | CLIENT_CONNECT_WITH_DB | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_CONNECT_ATTRS | CLIENT_DEPRECATE_EOF`.

Distinguishing case: a client connecting with `CLIENT_FOUND_ROWS` set (JDBC
`useAffectedRows=false`, which is the **default**) has the bit masked away by
`negotiate_capabilities` (`handshake.rs:485-490`, `client & server`). An
`UPDATE t SET c=c WHERE id=1` that matches one row but changes nothing then
reports `affected_rows=0` where MySQL and Go report 1 — JDBC's
`executeUpdate()` returns a different number, and code that branches on
"did the update find the row" takes the wrong branch. `CLIENT_TRANSACTIONS`
compounds D2: a client is told the server has no transaction support at all.

---

## Rank 3 — a missing command or capability that clients actually use

### D6. `COM_FIELD_LIST`, `COM_SET_OPTION`, `COM_RESET_CONNECTION` and `COM_CHANGE_USER` are refused

- Go: `pkg/server/conn.go:1548-1549` (`handleFieldList`, `conn.go:2443`),
  `:1584-1585` (`handleSetOption`, `conn_stmt.go:656-677`, replies with an
  **EOF packet**), `:1589-1590` (`handleResetConnection`, `conn.go:2781`),
  `:1566-1567` (`handleChangeUser`, `conn.go:2720`).
- Rust: `mysql_connection.rs:1453-1463` — `FieldList`, `SetOption`,
  `ResetConnection` and every `Unknown` code (which includes `COM_CHANGE_USER`
  0x11, `COM_REFRESH` 0x07, `COM_SHUTDOWN` 0x08, `COM_STATISTICS` 0x09) get one
  ERR packet.

Distinguishing cases the user never types: the `mysql` CLI with `--auto-rehash`
(the default when not `--batch`) sends `COM_FIELD_LIST` per table to build tab
completion; JDBC with `allowMultiQueries=true` sends `COM_SET_OPTION` during
connection setup and treats the ERR as a connection failure; HikariCP/JDBC
connection pools call `COM_RESET_CONNECTION` on return-to-pool for MySQL >= 5.7.3;
PHP `mysqlnd` persistent connections and `mysql_change_user()` send
`COM_CHANGE_USER`. Because each does get *a* packet back, these are refusals,
not desyncs.

### D7. The errno for a genuinely unknown command differs

- Go: `pkg/server/conn.go:1592-1593` default arm returns
  `mysql.NewErrf(mysql.ErrUnknown /* 1105 */, "command %d not supported now", nil, cmd)`,
  written with SQLSTATE `HY000`.
- Rust: `mysql_connection.rs:1453-1463` uses `ER_UNKNOWN_COM_ERROR` (1047).

Distinguishing case: send command byte `0xfa`. Go replies
`ERR 1105 HY000 "command 250 not supported now"`; Rust replies
`ERR 1047 08S01 "command is not supported by the read-only Rust SQL node"`.
(1047's SQLSTATE was `HY000` before commit `b05550e670`; the errno itself is
still deliberately different and is left alone here.)

### D8. `CLIENT_COMPRESS` / `CLIENT_ZSTD_COMPRESSION_ALGORITHM` are implemented but never advertised

- Go: `pkg/server/server.go:121` advertises both; `packetio.go:316-470` and
  `:472-552` implement the compressed envelope.
- Rust: `rust/crates/tidb-protocol/src/compression.rs` and
  `rust/crates/tidb-server/src/compressed_command_io.rs` implement the same
  envelope, and `PacketIoReader`/`PacketIoWriter`
  (`rust/crates/tidb-protocol/src/packet.rs:251-544`) carry the dual
  inner/outer sequence rule faithfully — but `SERVER_CAPABILITIES`
  (`mysql_connection.rs:107-112`) omits both bits and
  `serve_connection_inner` constructs the plain `PacketReader`
  (`:474`). The compression path is dead on the live connection.

Distinguishing case: `mysql --compress` or go-sql-driver
`?compress=true` connects uncompressed against Rust. No breakage — the clients
degrade — but the entire ported implementation is unreachable.

### D9. `CLIENT_MULTI_STATEMENTS` / `CLIENT_MULTI_RESULTS` / `CLIENT_LOCAL_FILES` are not advertised

Go `server.go:119-120` advertises all three; Rust does not
(`mysql_connection.rs:107-112`). Consequences: `SELECT 1; SELECT 2` in one
`COM_QUERY` reaches the parser as one string and fails; `SERVER_MORE_RESULTS_EXISTS`
(0x0008), which Go sets at `conn.go:2269`, is never set by Rust (consistent,
since the capability is absent); `LOAD DATA LOCAL INFILE` cannot work.

### D10. The advertised server version is `5.7.25`, not `8.0.11`

- Go: `pkg/parser/mysql/const.go:30,59` — `ServerVersion` is
  `"8.0.11" + "-TiDB-" + TiDBReleaseVersion`.
- Rust: `mysql_connection.rs:466` — `"5.7.25-TiDB-Rust"`.

Distinguishing case: any client that version-gates. Connector/J's
`versionMeetsMinimum(8, 0, x)` switches off the 8.0-only paths (query
attributes, `utf8mb4_0900_ai_ci` handling, `RESET CONNECTION` availability);
ORMs pick 5.7 SQL dialects. Nothing breaks loudly, but the negotiated feature
set changes wholesale from a single string.

---

## Rank 4 — cosmetic field differences

### D11. The `CLIENT_DEPRECATE_EOF` terminal packet zeroes `affected_rows`/`last_insert_id` and carries no info string

- Go: `pkg/server/conn.go:1770-1772` — `writeEOF` under `ClientDeprecateEOF`
  delegates to `writeOkWith(mysql.EOFHeader, ...)`, which fills
  `cc.ctx.AffectedRows()`, `cc.ctx.LastInsertID()` and the length-encoded
  `cc.ctx.LastMessage()` (`conn.go:1688-1721`).
- Rust: `rust/crates/tidb-protocol/src/resultset.rs:145-157` hardcodes
  `affected_rows: 0, last_insert_id: 0`, and every server call site passes
  `info: Vec::new()` (`mysql_connection.rs:1576-1583`).

Distinguishing case: after a statement that set `LastMessage` (e.g. an
`UPDATE`'s `"Rows matched: 1  Changed: 0  Warnings: 0"`), the next result set's
terminal EOF-shaped OK differs in its trailing bytes. No mainstream client
reads those fields off the terminal EOF, which is why this is rank 4.

### D12. `Command::Unknown`'s message text and empty-payload handling

`decode_command` rejects a zero-length command packet with
`CommandError::EmptyPayload` → `ERR 1047` (`mysql_connection.rs:736-747`); Go
indexes `data[0]` in `dispatch` on a packet the read loop guarantees non-empty.
Cosmetic; no client sends an empty command packet.

---

## Verified equal

These were compared field by field and match. Do not re-audit them.

1. **Packet framing arithmetic.** `PacketHeader::encode`/`decode`
   (`rust/crates/tidb-protocol/src/packet.rs:50-68`) produce the same
   three-byte little-endian length plus sequence byte as
   `packetio.go:159-161,273-276`. `MAX_PAYLOAD_LEN` is `(1<<24)-1` on both
   sides.
2. **The >16 MiB split-and-continue rule, including the exact-multiple case.**
   `PacketWriter::write_packet` (`packet.rs:104-120`) loops while
   `frame_len == MAX_PAYLOAD_LEN`, so a payload that is an exact multiple gets
   its terminating zero-length frame — the same outcome as
   `packetio.go:250-272`'s `for length >= maxPayloadLen` followed by the
   unconditional final write. The read side (`packet.rs:185-213` vs
   `packetio.go:208-241`) joins continuation frames on the same
   `< MaxPayloadLen` predicate and applies the same accumulated
   `max_allowed_packet` check, reset per logical packet.
3. **Sequence-id reset points.** Rust resets the read sequence to 0 at the top
   of the command loop (`mysql_connection.rs:710`) and starts each response at
   1; Go resets both read and compressed sequence after each dispatch
   (`conn.go:1342-1343`). Handshake sequencing matches too: initial handshake
   at 0, response read at 1 (`mysql_connection.rs:475`), reply at 2, shifted to
   3 after a TLS upgrade (`:483-493`), auth-switch reply at `reply_sequence+1`
   (`:560`). Within a response, `FramedResultSetSink` carries
   `writer.sequence()` forward across payloads (`resultset_writer.rs:75-84`),
   so a 300-packet result set numbers identically.
4. **Compressed-envelope framing and the inner-sequence exemption.**
   `packet.rs:311-319` reproduces `packetio.go:162-172`'s MariaDB Connector/J
   rule exactly: an inner sequence mismatch is ignored only under compression,
   an outer mismatch is always an error; and `flush` reassigns the inner
   sequence from the compressed sequence (`packet.rs:499-509` vs
   `packetio.go:299-314`). Unreachable in production today — see D8.
5. **Initial handshake packet layout.** `InitialHandshake::encode_payload`
   (`handshake.rs:83-108`) emits byte-for-byte what
   `conn.go:writeInitialHandshake` (`:474-531`) does: protocol 10, NUL server
   version, 4-byte connection id, `salt[0..8]`, filler `0`, low capability
   word, collation, status, high capability word, `len(salt)+1`, ten reserved
   zeros, `salt[8..]`, `0`, plugin name, `0`. Field *values* diverge (D5, D10);
   the layout does not.
6. **HandshakeResponse41 parsing**, including the `CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA`
   one-byte-marker special case, the `CLIENT_SECURE_CONNECTION` fallback, the
   NUL-terminated legacy form, optional database, plugin name, the 1 MiB
   connection-attributes hard limit, and the trailing zstd level
   (`handshake.rs:387-479`).
7. **Auth-plugin switch trigger.** Rust switches to `mysql_native_password`
   when the client advertised `CLIENT_PLUGIN_AUTH` and named a different plugin
   (`mysql_connection.rs:553-564`); Go does the same for an account with no
   stored plugin (`conn.go:checkAuthPlugin`, the
   `resp.AuthPlugin != mysql.AuthNativePassword && resp.Capability&mysql.ClientPluginAuth > 0`
   arm). A `caching_sha2_password` client (MySQL 8 default) is switched
   identically.
8. **Binary-row null bitmap.** `encode_binary_result_row`
   (`rust/crates/tidb-protocol/src/prepared_statement.rs:765-807`) uses
   `(n + 7 + 2) / 8` bytes, the `mysql.OKHeader` prefix, and bit `i + 2` in
   byte `(i + 2) / 8` — identical to `column.go:181-232`'s
   `numBytes4Null := (len(columns) + 7 + 2) / 8` and `bitPos := byte((i + 2) % 8)`.
   No off-by-one.
9. **Binary temporal encodings.** `encode_binary_time`
   (`prepared_statement.rs:591-627`) and `encode_binary_datetime`
   (`:662-712`) reproduce `dump.BinaryTime` / `dump.BinaryDateTime`'s
   length-prefixed 0/8/12 and 0/4/7/11 byte shapes, sign flag, day/hour/minute/
   second layout and little-endian microsecond word. (They are encoders only;
   no result cell reaches them yet — see the gap note below.)
10. **OK / EOF / ERR packet field order.** `encode_ok_like_packet`
    (`resultset.rs:219-233`) and `encode_error_packet`
    (`error_packet.rs:67-82`) match `writeOkWith` (`conn.go:1688-1721`) and
    `writeError` (`conn.go:1725-1767`) field for field, including the
    protocol-4.1 gating of status/warnings and of `'#'`+SQLSTATE, and the
    legacy compact EOF (`resultset.rs:160-165` vs `conn.go:1770-1782`).
11. **Static tables.** Machine-diffed and identical:
    - 223 collation id → name pairs, `pkg/parser/mysql/charset.go` `Collations`
      vs `rust/crates/tidb-mysql/src/charset.rs` `COLLATIONS` — 0 mismatches.
    - 41 charset name → default collation id pairs, `CharsetIDs` vs
      `CHARSET_IDS` — 0 mismatches.
    - 244 explicit SQLSTATE overrides, `pkg/parser/mysql/state.go` `MySQLState`
      vs `rust/crates/tidb-error/src/mysql/state.rs` `MYSQL_STATES` — 0
      mismatches, same `HY000` default.
    - Length-encoded integer/string encoding (`result.rs:24-53` vs
      `dump.LengthEncodedInt`/`LengthEncodedString`), including the `0xfb` NULL
      marker never being emitted for an integer.
    - Every `COM_*` byte value in `command.rs:24-46` against
      `pkg/parser/mysql/const.go:171-203`.
    - `dumpType`/`DumpFlag`/`dumpCharset`/`dumpLength` remapping — SET/ENUM to
      `TypeString` with the SET/ENUM flag, tiny/medium/long blob to `TypeBlob`,
      vector-float32 to `TypeLongBlob` with `MaxLongBlobWidth` and the binary
      flag cleared (`column.rs:193-210` vs `column.go:97-146`).
    - `ConvertColumnInfo` arithmetic other than the charset id: the
      `NewDecimal` +1/+2 width, the string `Maxlen` multiplier, the
      `NotFixedDec` / `DefaultFsp` decimal rule, the default-flen table, the
      `TypeVarchar` → `TypeVarString` old-client remap, and the 256-byte
      alias/name truncation (`result_metadata.rs:193-300`, `column.rs:153-163`
      vs `convert.go:25-108`, `column.go:60-67`).

---

## Fixed in this pass

Both were single wrong constants, `cargo check` and `cargo clippy` clean, and
`cargo fmt --all --check` clean (commit `b05550e670`):

- `COM_STMT_PREPARE` now drops one trailing NUL like `COM_QUERY`. Go trims it
  for both (`conn.go:1543-1546` and `:1571-1574`, issue 39132); Rust trimmed it
  only for `COM_QUERY`, so a client that NUL-terminates its prepare text got a
  parse error on a statement that Go prepares fine.
- `ER_UNKNOWN_COM_ERROR` (1047) now carries SQLSTATE `08S01`. Go resolves every
  ERR packet's state through `mysql.MySQLState`, which maps `ErrUnknownCom` to
  `08S01`; both call sites hardcoded the `HY000` default.

## Known gaps that are not divergences in the compared files

- Binary result cells exist only for the integer/float/decimal/string group
  (`prepared_statement.rs:726-747`); temporal, enum, set, JSON and vector
  columns fail `BinaryResultSetStream::new` closed, so a prepared
  `SELECT` over a `DATE` column gets an ERR packet rather than wrong bytes.
  Graceful, but the encoders in verified-equal item 9 are unreachable.
- `parse_binary_params`' `bound_params` argument (`binary_params.rs:169-200`)
  is the long-data seam D1 needs; no caller supplies it.

## Not verified because nothing can execute here

Everything. Specifically: no packet capture was taken, no client
(`mysql`, `sysbench`, go-sql-driver, Connector/J) was connected, and none of
the Rust unit tests in `tidb-protocol` were run — including the two assertions
added to `command.rs`'s `dispatch_command_vectors_preserve_source_payloads`.
The evidence is `cargo check` + `cargo clippy` (both exit 0) plus source
reading and machine diffs of the static tables. Each finding above names the
client action that would confirm it against a running node in one round trip.
