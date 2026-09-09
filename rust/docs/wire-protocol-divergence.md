# MySQL wire-protocol divergence inventory

File-by-file semantic comparison of TiDB's Go wire implementation against the
Rust one.

| side | files |
| --- | --- |
| Go | `pkg/server/conn.go`, `pkg/server/conn_stmt.go`, `pkg/server/driver_tidb.go`, `pkg/server/internal/packetio.go`, `pkg/server/internal/column/{column,convert}.go`, `pkg/format/textrow/result_encoder.go`, `pkg/parser/mysql/{const,charset,state}.go` |
| Rust | `rust/crates/tidb-protocol/src/*`, `rust/crates/tidb-mysql/src/charset.rs`, `rust/crates/tidb-error/src/mysql/state.rs`, `rust/crates/tidb-exec/src/result_metadata.rs`, `rust/crates/tidb-server/src/{handshake,mysql_connection,resultset_writer}.rs` |

Every claim below is source-derived, with the Go file:line, the Rust file:line,
and the client action that produces different bytes. Focused Rust regressions
have now executed for the fixed command/error/capability/metadata seams,
including loopback TCP packet tests for command framing. No external MySQL
client or production-cluster packet capture has been run. Line numbers on the
Rust side are refreshed only where a batch touched the entry.

Counts: **12 audited divergence entries**: 8 fixed, 4 remaining (1 rank-2,
1 rank-3, 2 rank-4), plus **11 verified-equal areas**. D11's packet encoder,
statement-value plumbing, and the live UPDATE producer are now aligned; the
remaining insert/replace/load and configured real-TiKV message producers stay
explicitly partial instead of being counted as fixed.

---

## Rank 1 — the client breaks or desynchronises (the 2014 class)

### D1. `COM_STMT_SEND_LONG_DATA` buffers data and answers with nothing (FIXED 2026-09-05)

- Go: `pkg/server/conn.go:1578-1579` dispatches `mysql.ComStmtSendLongData` to
  `handleStmtSendLongData` (`pkg/server/conn_stmt.go:610-625`), which returns
  `nil` after `stmt.AppendParam`. A `nil` return from `dispatch` writes **no
  packet at all** — MySQL's `COM_STMT_SEND_LONG_DATA` has no response.
- Rust: `tidb-protocol` decodes `0x18` as `Command::StmtSendLongData`, and the
  connection-owned prepared-statement registry appends each chunk to the
  matching parameter buffer. The command arm intentionally writes no packet;
  `COM_STMT_EXECUTE` consumes and clears the retained chunks on both success
  and decode failure, matching Go's `BoundParams`/`Reset` lifetime.

Distinguishing case: any client that binds a `TEXT`/`BLOB`/large parameter —
JDBC `PreparedStatement.setBinaryStream`, `setBlob`, or any parameter over
`blobSendChunkSize`; Python `mysqlclient` with a large `bytes` argument; the C
API `mysql_stmt_send_long_data`.

```
C: COM_STMT_PREPARE "INSERT INTO t VALUES (?)"    S: PREPARE_OK
C: COM_STMT_SEND_LONG_DATA(stmt, param 0, bytes)  S: nothing (both)
C: COM_STMT_EXECUTE(stmt)                         S: OK / result set
```

The client does not read after `SEND_LONG_DATA`; the Rust server now leaves no
packet queued to desynchronize the following execute response.

### D2. OK and EOF packets carry the live transaction status (FIXED 2026-09-05)

- Go: `pkg/server/conn.go:2265` takes `status := cc.ctx.Status()` — the live
  session status — and threads it through `writeResultSet`/`writeChunks` into
  every metadata and terminal `writeEOF` (`conn.go:2589,2630`), and
  `writeOkWith` (`conn.go:1688-1721`) writes `cc.ctx.Status()` likewise. So
  `SERVER_STATUS_IN_TRANS` (0x0001) is set on every packet while a transaction
  is open, and `SERVER_STATUS_AUTOCOMMIT` (0x0002) is cleared after
  `SET autocommit=0`.
- Rust: `QuerySession::wire_status` now snapshots the session's live
  transaction/autocommit state for every OK packet and result set. `WireStatus`
  is passed through `WireFraming::result_set`, `write_ok`, and
  `write_affected_rows_ok`; no connection-lifetime autocommit literal remains
  on those response paths.

Distinguishing case:

```
C: BEGIN                  S: OK status=0x0003 (both)
C: INSERT INTO t VALUES(1) S: OK status=0x0003 (both)
C: SELECT * FROM t         S: EOF status=0x0003 (both)
C: COMMIT                  S: OK status=0x0002 (both)
```

Connector/J and other clients that use the server status for local transaction
state now observe an open transaction until the session actually commits or
rolls it back; `SET autocommit=0` also clears the advertised autocommit bit.

---

## Rank 2 — the client silently misinterprets data

### D3. The column-definition charset id uses the charset's default collation id (FIXED 2026-09-05)

- Go: `pkg/server/internal/column/convert.go:31` sets
  `Charset: uint16(mysql.CharsetNameToID(fld.Column.GetCharset()))` — the id is
  derived from the **charset name**, so it is always that charset's default
  collation (`pkg/parser/mysql/charset.go:19-34`).
- Rust now derives the protocol field from the owning `Charset` using the
  source `CharsetNameToID` defaults (`utf8mb4` → 46, `gbk` → 28, and so on),
  independent of the new-collation compatibility switch. The selected column
  collation remains available to execution and length calculations; it no
  longer leaks into the wire charset number.

Distinguishing case: `CREATE TABLE t (c VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci); SELECT c FROM t;`
now puts `46` (`utf8mb4_bin`) in the column definition's two charset bytes on
both implementations. With `COLLATE utf8mb4_general_ci`, the wire id remains
`46`; clients that surface `charsetnr` therefore see the same metadata.

### D4. `@@character_set_results` controls the advertised charset id and row bytes (FIXED 2026-09-05)

- Go: `pkg/server/internal/column/column.go:76` writes
  `d.ColumnCharsetID(column.dumpCharset(), textrow.IsStringColumnType(column.Type))`,
  which (`pkg/format/textrow/result_encoder.go:104-112`) replaces a non-binary
  string column's id with `@@character_set_results`' default collation. Row
  values go through `d.EncodeData(...)` (`column.go:162-179` text,
  `:181-232` binary), which transcodes them (`result_encoder.go:137-148`).
  `cc.initResultEncoder` builds the encoder per statement (`conn.go:1103`).
- Rust: `serve_connection_inner` refreshes `ResultEncoder` from
  `QuerySession::result_charset` once per command (`rust/crates/tidb-server/src/mysql_connection.rs:1274-1281`)
  and passes it through `ResultSetOptions` to both metadata and row writers.
  `ColumnInfo::dump_charset` and `ResultEncoder::encode_data` therefore use the
  same per-command result charset as Go, including the unset/binary fallback.
  Unknown session-charset spellings also follow Go's
  `FindEncodingTakeUTF8AsNoop`: the encoder stays byte-preserving and
  `ColumnCharsetID` emits charset number zero instead of refusing construction.

Distinguishing case: with `SET character_set_results = latin1`, both servers
advertise collation `47` and send `0xE9` for `SELECT 'é'`; the client no longer
sees a UTF-8 column id paired with UTF-8 row bytes.

### D5. `CLIENT_FOUND_ROWS` and `CLIENT_TRANSACTIONS` are advertised (FIXED 2026-09-05)

- Go: `pkg/server/server.go:116-121` `defaultCapability` includes
  `ClientTransactions` (1<<13) and `ClientFoundRows` (1<<1).
- Rust: `rust/crates/tidb-server/src/mysql_connection.rs` advertises both bits
  and stores negotiated `CLIENT_FOUND_ROWS` state in `SessionContext`.
  Pipeline and cluster sessions copy it into each statement context, while
  configured real-TiKV nodes apply the same value to their completed write
  reports. Single-table UPDATE, multi-table UPDATE, unchanged duplicate-key
  UPDATE, and configured point-update reports then count successfully touched
  rows when the bit is enabled while retaining the changed-row default when it
  is absent.

Distinguishing case: a client connecting with `CLIENT_FOUND_ROWS` set (JDBC
`useAffectedRows=false`, which is the **default**) now retains the bit through
`negotiate_capabilities` (`handshake.rs`, `client & server`). Over one changed
row and one unchanged matched row, `UPDATE t SET c=10` reports
`affected_rows=2`; an unchanged `ON DUPLICATE KEY UPDATE` reports one. Without
the bit, the same execution paths preserve their changed-row counts.

---

## Rank 3 — a missing command or capability that clients actually use

### D6. `COM_FIELD_LIST`, `COM_RESET_CONNECTION` and `COM_CHANGE_USER` remain unavailable (`COM_SET_OPTION`, `COM_STATISTICS`, and `COM_REFRESH` fixed)

- Go: `pkg/server/conn.go:1548-1554` (`handleFieldList`, `handleRefresh`),
  `:1584-1585` (`handleSetOption`, `conn_stmt.go:656-677`, replies with an
  **EOF packet**), `:1589-1590` (`handleResetConnection`, `conn.go:2781`),
  `:1566-1567` (`handleChangeUser`, `conn.go:2720`).
- Rust: `COM_SET_OPTION` now toggles the negotiated multi-statement bit and
  returns Go's EOF/OK form; `COM_STATISTICS` writes Go's raw status line; and
  `COM_REFRESH` decodes its subcommand, runs `FLUSH PRIVILEGES` for `0x01`, and
  preserves Go's two-OK response shape. `COM_FIELD_LIST` and
  `COM_RESET_CONNECTION` remain explicit 1047/`08S01` refusals. `COM_CHANGE_USER`
  and `COM_SHUTDOWN` are still unowned command bytes and therefore use the
  generic 1105/`HY000` unknown-command response.

Distinguishing cases the user never types: the `mysql` CLI with `--auto-rehash`
(the default when not `--batch`) sends `COM_FIELD_LIST` per table to build tab
completion; JDBC with `allowMultiQueries=true` sends `COM_SET_OPTION` during
connection setup and treats the ERR as a connection failure; HikariCP/JDBC
connection pools call `COM_RESET_CONNECTION` on return-to-pool for MySQL >= 5.7.3;
PHP `mysqlnd` persistent connections and `mysql_change_user()` send
`COM_CHANGE_USER`. Because each does get *a* packet back, these are refusals,
not desyncs.

### D7. The errno for a genuinely unknown command differs (FIXED)

- Go: `pkg/server/conn.go:1592-1593` default arm returns
  `mysql.NewErrf(mysql.ErrUnknown /* 1105 */, "command %d not supported now", nil, cmd)`,
  written with SQLSTATE `HY000`.
- Rust: `mysql_connection.rs` now routes `Command::Unknown` through the Go
  generic unknown error (1105/HY000); 1047/08S01 remains reserved for a known
  command that this node explicitly refuses.

Distinguishing case: send command byte `0xfa`. Both implementations now reply
`ERR 1105 HY000 "command 250 not supported now"`; sending a known but
unsupported command such as `COM_FIELD_LIST` still takes the separate Rust
1047/08S01 refusal path.

### D8. `CLIENT_COMPRESS` / `CLIENT_ZSTD_COMPRESSION_ALGORITHM` are implemented and advertised (FIXED 2026-09-05)

- Go: `pkg/server/server.go:121` advertises both; `packetio.go:316-470` and
  `:472-552` implement the compressed envelope.
- Rust: `SERVER_CAPABILITIES` includes both bits and the negotiated command
  loop selects `PacketIoReader`/`PacketIoWriter`
  (`rust/crates/tidb-server/src/mysql_connection.rs:1190-1199`) over the same
  envelope covered by the compressed packet suite.

Distinguishing case: `mysql --compress` or go-sql-driver `?compress=true` now
negotiates zlib against both servers; clients that request the zstd capability
likewise reach the Rust zstd envelope.

### D9. `CLIENT_MULTI_STATEMENTS` / `CLIENT_MULTI_RESULTS` / `CLIENT_LOCAL_FILES` are advertised (FIXED 2026-09-05)

Go `server.go:119-120` advertises all three, and Rust now does the same in
`SERVER_CAPABILITIES`. The command loop splits admitted multi-statements,
stamps `SERVER_MORE_RESULTS_EXISTS` on every non-final response, and negotiates
the `CLIENT_LOCAL_FILES` request/transfer path.

### D10. The advertised server version is runtime-configured like Go (FIXED 2026-09-05)

- Go: `pkg/parser/mysql/const.go:30,59` — `ServerVersion` is
  `"8.0.11" + "-TiDB-" + TiDBReleaseVersion`.
- Rust: `mysql_connection.rs:817` reads
  `tidb_mysql::runtime_versions().server_version`, whose default is the same
  `8.0.11-TiDB-<release>` shape and which the configured node can override.

Distinguishing case: version-gating clients now observe the same runtime
server-version contract as Go; `RESET CONNECTION` remains unavailable for the
separate command-owner reason recorded in D6.

---

## Rank 4 — cosmetic field differences

### D11. The `CLIENT_DEPRECATE_EOF` terminal packet zeroes `affected_rows`/`last_insert_id` and carries no info string (PARTIALLY FIXED 2026-09-05)

- Go: `pkg/server/conn.go:1770-1772` — `writeEOF` under `ClientDeprecateEOF`
  delegates to `writeOkWith(mysql.EOFHeader, ...)`, which fills
  `cc.ctx.AffectedRows()`, `cc.ctx.LastInsertID()` and the length-encoded
  `cc.ctx.LastMessage()` (`conn.go:1688-1721`).
- Rust before this batch: `rust/crates/tidb-protocol/src/resultset.rs:145-157`
  hardcoded `affected_rows: 0, last_insert_id: 0`, and every server call site
  passed `info: Vec::new()` (`mysql_connection.rs:1576-1583`).
- Rust after this batch: `ResultSetOptions`, `EofPacket`, and
  `QueryResult` preserve the statement's affected rows, last-insert id, and
  info bytes through text, binary, prepared, and cursor result writers
  (`tidb-protocol/src/resultset.rs`, `tidb-server/src/sql_node.rs`,
  `connection_writers.rs`, `mysql_connection.rs`). Pipeline and cluster
  sessions now publish their live `last_insert_id` into that snapshot. A
  focused protocol regression proves non-zero values and info bytes survive
  the deprecated-EOF encoding.
- Remaining boundary: Rust's pipeline and cluster UPDATE paths now publish
  Go's `Rows matched: …  Changed: …  Warnings: …` message through the session
  and OK/EOF writers. INSERT/REPLACE/LOAD DATA and configured real-TiKV write
  paths still have no equivalent producer, so their info field remains empty;
  the transport no longer discards a value if a producer supplies one.

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
   `packetio.go:299-314`). The negotiated zlib/zstd path now exercises this
   logic in production too — see D8.
5. **Initial handshake packet layout.** `InitialHandshake::encode_payload`
   (`handshake.rs:83-108`) emits byte-for-byte what
   `conn.go:writeInitialHandshake` (`:474-531`) does: protocol 10, NUL server
   version, 4-byte connection id, `salt[0..8]`, filler `0`, low capability
   word, collation, status, high capability word, `len(salt)+1`, ten reserved
   zeros, `salt[8..]`, `0`, plugin name, `0`. The capability value, layout,
   and runtime server-version value (D10) now match for the implemented bits.
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

## Fixed in current Rust batches

- `COM_STMT_PREPARE` now drops one trailing NUL like `COM_QUERY`. Go trims it
  for both (`conn.go:1543-1546` and `:1571-1574`, issue 39132); Rust trimmed it
  only for `COM_QUERY`, so a client that NUL-terminates its prepare text got a
  parse error on a statement that Go prepares fine.
- `ER_UNKNOWN_COM_ERROR` (1047) now carries SQLSTATE `08S01`. Go resolves every
  ERR packet's state through `mysql.MySQLState`, which maps `ErrUnknownCom` to
  `08S01`; both call sites hardcoded the `HY000` default.
- Genuinely unknown command bytes now keep Go's generic `ErrUnknown` identity
  (1105/HY000 and `command %d not supported now`) instead of being flattened
  into the known-command `ErrUnknownCom` (1047/08S01) refusal.
- Result metadata now emits the charset's default collation ID, as Go's
  `CharsetNameToID` does, even when the column uses a non-default collation;
  the focused `tidb-exec` regression covers `utf8mb4_general_ci` → 46.
- `COM_REFRESH` now follows Go's command routing: non-privilege targets are
  successful no-ops, while `0x01` runs `FLUSH PRIVILEGES` and emits the same
  two consecutive OK packets as Go. The focused TCP regression consumes both
  packets and verifies the connection remains synchronized.

## Known gaps that are not divergences in the compared files

- Binary result cells exist only for the integer/float/decimal/string group
  (`prepared_statement.rs:726-747`); temporal, enum, set, JSON and vector
  columns fail `BinaryResultSetStream::new` closed, so a prepared
  `SELECT` over a `DATE` column gets an ERR packet rather than wrong bytes.
  Graceful, but those temporal encoders remain outside the currently served
  binary result-cell set.

## Validation boundary

The fixed command seams have loopback TCP packet evidence, but no external
client (`mysql`, `sysbench`, go-sql-driver, or Connector/J) was connected and
no production TiKV-backed cluster was used. Remaining divergence claims are
source-derived and still need their own implementation and client round trip.
