#!/usr/bin/env python3

"""Raw MySQL prepared-statement proof client for Campaign 27."""

from __future__ import annotations

import argparse
import hashlib
import json
import select
import socket
import struct
import sys
from dataclasses import dataclass
from typing import Any


CLIENT_PROTOCOL_41 = 1 << 9
CLIENT_SECURE_CONNECTION = 1 << 15
CLIENT_PLUGIN_AUTH = 1 << 19
CLIENT_DEPRECATE_EOF = 1 << 24

COM_QUIT = 0x01
COM_STMT_PREPARE = 0x16
COM_STMT_EXECUTE = 0x17
COM_STMT_CLOSE = 0x19
MYSQL_TYPE_LONG = 0x03
MYSQL_TYPE_LONGLONG = 0x08
UNSIGNED_FLAG = 0x80


class ProtocolError(RuntimeError):
    """The peer violated the bounded Campaign 27 protocol contract."""


@dataclass(frozen=True)
class MysqlError:
    code: int
    state: str
    message: str


@dataclass(frozen=True)
class Column:
    schema: str
    table: str
    org_table: str
    name: str
    org_name: str
    charset: int
    column_length: int
    type_code: int
    flags: int
    decimals: int


@dataclass(frozen=True)
class Prepared:
    statement_id: int
    parameters: tuple[Column, ...]
    columns: tuple[Column, ...]


def emit(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, separators=(",", ":")), flush=True)


def read_exact(stream: socket.socket, length: int) -> bytes:
    chunks: list[bytes] = []
    remaining = length
    while remaining:
        chunk = stream.recv(remaining)
        if not chunk:
            raise ProtocolError(f"peer closed with {remaining} packet bytes outstanding")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_lenenc(payload: bytes, offset: int) -> tuple[int | None, int]:
    if offset >= len(payload):
        raise ProtocolError("truncated length-encoded integer")
    first = payload[offset]
    if first < 0xFB:
        return first, offset + 1
    if first == 0xFB:
        return None, offset + 1
    widths = {0xFC: 2, 0xFD: 3, 0xFE: 8}
    width = widths.get(first)
    if width is None or offset + 1 + width > len(payload):
        raise ProtocolError("invalid length-encoded integer")
    value = int.from_bytes(payload[offset + 1 : offset + 1 + width], "little")
    return value, offset + 1 + width


def read_lenenc_bytes(payload: bytes, offset: int) -> tuple[bytes | None, int]:
    length, offset = read_lenenc(payload, offset)
    if length is None:
        return None, offset
    end = offset + length
    if end > len(payload):
        raise ProtocolError("truncated length-encoded byte string")
    return payload[offset:end], end


def parse_error(payload: bytes) -> MysqlError:
    if len(payload) < 3 or payload[0] != 0xFF:
        raise ProtocolError(f"expected ERR packet, received {payload.hex()}")
    code = int.from_bytes(payload[1:3], "little")
    if len(payload) >= 9 and payload[3] == ord("#"):
        state = payload[4:9].decode("ascii", "replace")
        message = payload[9:].decode("utf-8", "replace")
    else:
        state = ""
        message = payload[3:].decode("utf-8", "replace")
    return MysqlError(code, state, message)


def parse_column(payload: bytes) -> Column:
    values: list[str] = []
    offset = 0
    for _ in range(6):
        value, offset = read_lenenc_bytes(payload, offset)
        if value is None:
            raise ProtocolError("column definition contains NULL identity field")
        values.append(value.decode("utf-8", "strict"))
    fixed_length, offset = read_lenenc(payload, offset)
    if fixed_length != 0x0C or offset + 12 > len(payload):
        raise ProtocolError("column definition has invalid fixed field block")
    charset = int.from_bytes(payload[offset : offset + 2], "little")
    column_length = int.from_bytes(payload[offset + 2 : offset + 6], "little")
    type_code = payload[offset + 6]
    flags = int.from_bytes(payload[offset + 7 : offset + 9], "little")
    decimals = payload[offset + 9]
    if offset + 12 != len(payload):
        raise ProtocolError("column definition has trailing bytes")
    return Column(
        schema=values[1],
        table=values[2],
        org_table=values[3],
        name=values[4],
        org_name=values[5],
        charset=charset,
        column_length=column_length,
        type_code=type_code,
        flags=flags,
        decimals=decimals,
    )


class MysqlConnection:
    def __init__(self, host: str, port: int, user: str, password: str) -> None:
        self.stream = socket.create_connection((host, port), timeout=5)
        self.stream.settimeout(5)
        self.user = user
        self.password = password
        self.server_capabilities = 0
        self.capabilities = 0
        self.connection_id = 0
        self._authenticate()

    def close(self) -> None:
        if self.stream.fileno() < 0:
            return
        try:
            self.write_packet(bytes([COM_QUIT]), 0)
        except OSError:
            pass
        self.stream.close()

    def __enter__(self) -> "MysqlConnection":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def read_packet(self, expected_sequence: int) -> bytes:
        header = read_exact(self.stream, 4)
        length = int.from_bytes(header[:3], "little")
        sequence = header[3]
        if sequence != expected_sequence:
            raise ProtocolError(
                f"packet sequence mismatch: expected {expected_sequence}, got {sequence}"
            )
        return read_exact(self.stream, length)

    def write_packet(self, payload: bytes, sequence: int) -> None:
        if len(payload) >= 1 << 24:
            raise ProtocolError("proof client does not emit multi-packet payloads")
        self.stream.sendall(len(payload).to_bytes(3, "little") + bytes([sequence]) + payload)

    def _authenticate(self) -> None:
        handshake = self.read_packet(0)
        if not handshake or handshake[0] != 10:
            raise ProtocolError("server did not send protocol-v10 handshake")
        version_end = handshake.find(b"\0", 1)
        if version_end < 0 or version_end + 32 > len(handshake):
            raise ProtocolError("server handshake is truncated")
        offset = version_end + 1
        self.connection_id = int.from_bytes(handshake[offset : offset + 4], "little")
        offset += 4
        salt_one = handshake[offset : offset + 8]
        offset += 9
        capability_low = int.from_bytes(handshake[offset : offset + 2], "little")
        offset += 2
        charset = handshake[offset]
        offset += 3
        capability_high = int.from_bytes(handshake[offset : offset + 2], "little")
        offset += 2
        self.server_capabilities = capability_low | capability_high << 16
        auth_length = handshake[offset]
        offset += 11
        salt_tail_length = max(13, auth_length - 8)
        if offset + salt_tail_length > len(handshake):
            raise ProtocolError("server handshake authentication salt is truncated")
        salt_two = handshake[offset : offset + salt_tail_length].rstrip(b"\0")
        salt = (salt_one + salt_two)[: max(0, auth_length - 1)]
        requested = (
            CLIENT_PROTOCOL_41
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH
            | CLIENT_DEPRECATE_EOF
        )
        self.capabilities = requested & self.server_capabilities
        required = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
        if self.capabilities & required != required:
            raise ProtocolError(
                f"server capabilities 0x{self.server_capabilities:08x} omit required flags"
            )
        token = native_password_token(self.password.encode(), salt)
        response = bytearray()
        response.extend(self.capabilities.to_bytes(4, "little"))
        response.extend((16 << 20).to_bytes(4, "little"))
        response.append(charset)
        response.extend(bytes(23))
        response.extend(self.user.encode())
        response.append(0)
        response.append(len(token))
        response.extend(token)
        response.extend(b"mysql_native_password\0")
        self.write_packet(bytes(response), 1)
        auth_result = self.read_packet(2)
        if auth_result and auth_result[0] == 0xFF:
            error = parse_error(auth_result)
            raise ProtocolError(f"authentication failed: {error.code} {error.message}")
        if not auth_result or auth_result[0] != 0x00:
            raise ProtocolError(f"unexpected authentication response {auth_result.hex()}")

    @property
    def deprecate_eof(self) -> bool:
        return bool(self.capabilities & CLIENT_DEPRECATE_EOF)

    def prepare(self, sql: str) -> Prepared | MysqlError:
        self.write_packet(bytes([COM_STMT_PREPARE]) + sql.encode(), 0)
        payload = self.read_packet(1)
        if payload and payload[0] == 0xFF:
            return parse_error(payload)
        if len(payload) != 12 or payload[0] != 0:
            raise ProtocolError(f"invalid COM_STMT_PREPARE response {payload.hex()}")
        statement_id = int.from_bytes(payload[1:5], "little")
        column_count = int.from_bytes(payload[5:7], "little")
        parameter_count = int.from_bytes(payload[7:9], "little")
        if statement_id == 0 or payload[9] != 0:
            raise ProtocolError("prepare response has invalid statement identity or filler")
        sequence = 2
        parameters: list[Column] = []
        for _ in range(parameter_count):
            parameters.append(parse_column(self.read_packet(sequence)))
            sequence += 1
        if parameter_count and not self.deprecate_eof:
            assert_eof(self.read_packet(sequence), False)
            sequence += 1
        columns: list[Column] = []
        for _ in range(column_count):
            columns.append(parse_column(self.read_packet(sequence)))
            sequence += 1
        if column_count and not self.deprecate_eof:
            assert_eof(self.read_packet(sequence), False)
        return Prepared(statement_id, tuple(parameters), tuple(columns))

    def execute(self, payload: bytes) -> tuple[list[int], MysqlError | None]:
        self.write_packet(bytes([COM_STMT_EXECUTE]) + payload, 0)
        first = self.read_packet(1)
        if first and first[0] == 0xFF:
            return [], parse_error(first)
        column_count, offset = read_lenenc(first, 0)
        if column_count is None or offset != len(first):
            raise ProtocolError("execute response omitted a concrete column count")
        sequence = 2
        columns = [parse_column(self.read_packet(sequence + index)) for index in range(column_count)]
        sequence += column_count
        if column_count and not self.deprecate_eof:
            assert_eof(self.read_packet(sequence), False)
            sequence += 1
        rows: list[int] = []
        while True:
            packet = self.read_packet(sequence)
            sequence += 1
            if packet and packet[0] == 0xFE and len(packet) < 9:
                assert_eof(packet, self.deprecate_eof)
                break
            if len(columns) != 1 or columns[0].type_code != MYSQL_TYPE_LONGLONG:
                raise ProtocolError("execute result is not one signed BIGINT column")
            if len(packet) != 10 or packet[:2] != b"\0\0":
                raise ProtocolError(f"invalid one-column binary row {packet.hex()}")
            rows.append(int.from_bytes(packet[2:], "little", signed=True))
        return rows, None

    def execute_write(self, payload: bytes) -> tuple[int, MysqlError | None]:
        self.write_packet(bytes([COM_STMT_EXECUTE]) + payload, 0)
        first = self.read_packet(1)
        if first and first[0] == 0xFF:
            return 0, parse_error(first)
        if not first or first[0] != 0x00:
            raise ProtocolError(f"prepared write expected an OK packet, got {first.hex()}")
        affected, _ = read_lenenc(first, 1)
        if affected is None:
            raise ProtocolError(f"prepared write OK packet omitted affected rows: {first.hex()}")
        return affected, None

    def close_statement(self, statement_id: int) -> None:
        self.write_packet(bytes([COM_STMT_CLOSE]) + statement_id.to_bytes(4, "little"), 0)

    def assert_no_response(self) -> None:
        readable, _, _ = select.select([self.stream], [], [], 0.1)
        if readable:
            raise ProtocolError("silent COM_STMT_CLOSE unexpectedly produced bytes or EOF")


def native_password_token(password: bytes, salt: bytes) -> bytes:
    if not password:
        return b""
    stage_one = hashlib.sha1(password).digest()
    stage_two = hashlib.sha1(stage_one).digest()
    challenge = hashlib.sha1(salt + stage_two).digest()
    return bytes(left ^ right for left, right in zip(stage_one, challenge, strict=True))


def assert_eof(payload: bytes, deprecate_eof: bool) -> None:
    expected_length = 7 if deprecate_eof else 5
    if len(payload) != expected_length or payload[0] != 0xFE:
        raise ProtocolError(f"invalid terminal EOF packet {payload.hex()}")


def execute_payload(
    statement_id: int,
    value: int,
    *,
    new_types: bool = True,
    cursor: int = 0,
    iteration_count: int = 1,
    null_bitmap: int = 0,
    type_code: int = MYSQL_TYPE_LONGLONG,
    type_flags: int = 0,
) -> bytes:
    payload = bytearray(statement_id.to_bytes(4, "little"))
    payload.append(cursor)
    payload.extend(iteration_count.to_bytes(4, "little"))
    payload.append(null_bitmap)
    payload.append(int(new_types))
    if new_types:
        payload.extend((type_code, type_flags))
    payload.extend(value.to_bytes(8, "little", signed=True))
    return bytes(payload)


def execute_write_payload(
    statement_id: int, values: list[int], *, new_types: bool = True
) -> bytes:
    """A COM_STMT_EXECUTE payload binding N signed-BIGINT parameters."""
    payload = bytearray(statement_id.to_bytes(4, "little"))
    payload.append(0)  # no cursor
    payload.extend((1).to_bytes(4, "little"))  # iteration count
    payload.extend(bytes((len(values) + 7) // 8))  # null bitmap, no NULLs
    payload.append(int(new_types))
    if new_types:
        for _ in values:
            payload.extend((MYSQL_TYPE_LONGLONG, 0))
    for value in values:
        payload.extend(value.to_bytes(8, "little", signed=True))
    return bytes(payload)


def require_error(
    connection: MysqlConnection, name: str, payload: bytes, expected_code: int
) -> dict[str, Any]:
    rows, error = connection.execute(payload)
    if rows or error is None or error.code != expected_code or error.state != "HY000":
        raise ProtocolError(
            f"{name}: expected {expected_code}/HY000, received rows={rows} error={error}"
        )
    return {"case": name, "code": error.code, "state": error.state, "message": error.message}


def positive(args: argparse.Namespace) -> None:
    sql = f"SELECT balance FROM {args.database}.rows WHERE id = ?"
    with MysqlConnection(args.host, args.port, args.user, args.password) as connection:
        prepared = connection.prepare(sql)
        if isinstance(prepared, MysqlError):
            raise ProtocolError(f"positive prepare failed: {prepared.code} {prepared.message}")
        if len(prepared.parameters) != 1 or len(prepared.columns) != 1:
            raise ProtocolError("positive prepare did not advertise one parameter and one result")
        parameter = prepared.parameters[0]
        result = prepared.columns[0]
        if (
            parameter.name != "?"
            or parameter.type_code != MYSQL_TYPE_LONGLONG
            or parameter.flags & UNSIGNED_FLAG
            or result.schema != args.database
            or result.table != "rows"
            or result.name != "balance"
            or result.org_name != "balance"
            or result.type_code != MYSQL_TYPE_LONGLONG
            or result.flags & UNSIGNED_FLAG
        ):
            raise ProtocolError(f"prepare metadata mismatch: parameter={parameter} result={result}")
        first_rows, first_error = connection.execute(
            execute_payload(prepared.statement_id, args.first_id)
        )
        if first_error or first_rows != [args.first_balance]:
            raise ProtocolError(f"first execute mismatch: rows={first_rows} error={first_error}")
        second_rows, second_error = connection.execute(
            execute_payload(prepared.statement_id, args.second_id, new_types=False)
        )
        if second_error or second_rows != [args.second_balance]:
            raise ProtocolError(f"type-reuse execute mismatch: rows={second_rows} error={second_error}")
        connection.close_statement(prepared.statement_id)
        connection.assert_no_response()
        closed = require_error(
            connection,
            "execute_after_close",
            execute_payload(prepared.statement_id, args.first_id),
            1243,
        )
        emit(
            "prepared_positive",
            connection_id=connection.connection_id,
            statement_id=prepared.statement_id,
            parameter_type=parameter.type_code,
            result_type=result.type_code,
            first={"id": args.first_id, "balance": first_rows[0]},
            second={"id": args.second_id, "balance": second_rows[0], "type_reuse": True},
            close="silent",
            after_close=closed,
        )


def negative(args: argparse.Namespace) -> None:
    sql = f"SELECT balance FROM {args.database}.rows WHERE id = ?"
    cases: list[dict[str, Any]] = []
    connection_ids: list[int] = []
    with MysqlConnection(args.host, args.port, args.user, args.password) as owner:
        connection_ids.append(owner.connection_id)
        prepared = owner.prepare(sql)
        if isinstance(prepared, MysqlError):
            raise ProtocolError(f"negative matrix setup prepare failed: {prepared}")
        statement_id = prepared.statement_id
        cases.extend(
            [
                require_error(owner, "truncated_statement_id", b"\x01\0\0", 1210),
                require_error(owner, "zero_statement_id", execute_payload(0, 1), 1210),
                require_error(owner, "unknown_statement_id", execute_payload(0x7FFFFFFF, 1), 1243),
                require_error(owner, "cursor", execute_payload(statement_id, 1, cursor=1), 1210),
                require_error(
                    owner,
                    "iteration_count",
                    execute_payload(statement_id, 1, iteration_count=2),
                    1210,
                ),
                require_error(
                    owner, "null_parameter", execute_payload(statement_id, 1, null_bitmap=1), 1210
                ),
                require_error(
                    owner, "null_bitmap_padding", execute_payload(statement_id, 1, null_bitmap=2), 1210
                ),
                require_error(
                    owner,
                    "type_reuse_before_type",
                    execute_payload(statement_id, 1, new_types=False),
                    1210,
                ),
                require_error(
                    owner,
                    "unsigned_bigint",
                    execute_payload(statement_id, 1, type_flags=UNSIGNED_FLAG),
                    1210,
                ),
                require_error(
                    owner,
                    "non_bigint",
                    execute_payload(statement_id, 1, type_code=MYSQL_TYPE_LONG),
                    1210,
                ),
                require_error(
                    owner,
                    "truncated_value",
                    execute_payload(statement_id, 1)[:-1],
                    1210,
                ),
                require_error(
                    owner,
                    "trailing_bytes",
                    execute_payload(statement_id, 1) + b"\0",
                    1210,
                ),
            ]
        )
        with MysqlConnection(args.host, args.port, args.user, args.password) as other:
            connection_ids.append(other.connection_id)
            cases.append(
                require_error(
                    other,
                    "cross_connection_statement_id",
                    execute_payload(statement_id, 1),
                    1243,
                )
            )
        owner.close_statement(statement_id)
        owner.assert_no_response()
        cases.append(
            require_error(
                owner, "execute_after_close", execute_payload(statement_id, 1), 1243
            )
        )
        unsupported = owner.prepare(
            f"SELECT balance FROM {args.database}.rows WHERE balance = ?"
        )
        if not isinstance(unsupported, MysqlError) or unsupported.code != 1105:
            raise ProtocolError(f"unsupported SQL prepare unexpectedly succeeded: {unsupported}")
        cases.append(
            {
                "case": "unsupported_sql",
                "code": unsupported.code,
                "state": unsupported.state,
                "message": unsupported.message,
            }
        )
        wrong_count = owner.prepare(
            f"SELECT balance FROM {args.database}.rows WHERE id = ? AND balance = ?"
        )
        if not isinstance(wrong_count, MysqlError) or wrong_count.code != 1105:
            raise ProtocolError(f"wrong-count SQL prepare unexpectedly succeeded: {wrong_count}")
        cases.append(
            {
                "case": "wrong_parameter_count",
                "code": wrong_count.code,
                "state": wrong_count.state,
                "message": wrong_count.message,
            }
        )
    emit(
        "prepared_negative_matrix",
        connection_ids=connection_ids,
        case_count=len(cases),
        cases=cases,
    )


def write(args: argparse.Namespace) -> None:
    """Drives `count` prepared point read + prepared arithmetic UPDATE pairs
    through the Rust endpoint, exactly the C28 read+write mix."""
    read_sql = f"SELECT balance FROM {args.database}.accounts WHERE id = ?"
    write_sql = f"UPDATE {args.database}.accounts SET balance = balance + ? WHERE id = ?"
    with MysqlConnection(args.host, args.port, args.user, args.password) as connection:
        read_stmt = connection.prepare(read_sql)
        if isinstance(read_stmt, MysqlError):
            raise ProtocolError(f"read prepare failed: {read_stmt.code} {read_stmt.message}")
        if len(read_stmt.parameters) != 1 or len(read_stmt.columns) != 1:
            raise ProtocolError("read prepare did not advertise one parameter and one column")
        write_stmt = connection.prepare(write_sql)
        if isinstance(write_stmt, MysqlError):
            raise ProtocolError(f"write prepare failed: {write_stmt.code} {write_stmt.message}")
        if len(write_stmt.parameters) != 2 or len(write_stmt.columns) != 0:
            raise ProtocolError(
                f"write prepare advertised {len(write_stmt.parameters)} params /"
                f" {len(write_stmt.columns)} columns; expected 2/0"
            )
        reads = 0
        affected_rows = 0
        for index in range(args.count):
            handle = (index % args.table_size) + 1
            first = index == 0
            rows, read_error = connection.execute(
                execute_payload(read_stmt.statement_id, handle, new_types=first)
            )
            if read_error is not None or len(rows) != 1:
                raise ProtocolError(f"read {index} failed: rows={rows} error={read_error}")
            reads += 1
            affected, write_error = connection.execute_write(
                execute_write_payload(write_stmt.statement_id, [1, handle], new_types=first)
            )
            if write_error is not None or affected != 1:
                raise ProtocolError(
                    f"write {index} failed: affected={affected} error={write_error}"
                )
            affected_rows += affected
        connection.close_statement(read_stmt.statement_id)
        connection.close_statement(write_stmt.statement_id)
        emit(
            "prepared_read_write",
            count=args.count,
            reads=reads,
            affected_rows=affected_rows,
            connection_id=connection.connection_id,
        )


def matrix(args: argparse.Namespace) -> None:
    """Drives the full C28 prepared write matrix through the Rust endpoint once:
    one-row INSERT, two-row INSERT, direct SET update, arithmetic update, and a
    point read. Uses ids outside the seeded range so it never collides."""
    table = f"{args.database}.accounts"
    with MysqlConnection(args.host, args.port, args.user, args.password) as connection:

        def prepare_write_stmt(sql: str, params: int) -> Any:
            prepared = connection.prepare(sql)
            if isinstance(prepared, MysqlError):
                raise ProtocolError(f"prepare failed: {prepared.code} {prepared.message} ({sql})")
            if len(prepared.parameters) != params or len(prepared.columns) != 0:
                raise ProtocolError(
                    f"write prepare advertised {len(prepared.parameters)} params /"
                    f" {len(prepared.columns)} columns; expected {params}/0 ({sql})"
                )
            return prepared

        def commit(prepared: Any, values: list[int], expected: int) -> None:
            affected, error = connection.execute_write(
                execute_write_payload(prepared.statement_id, values)
            )
            if error is not None or affected != expected:
                raise ProtocolError(f"execute affected={affected} expected={expected} error={error}")

        one_row = prepare_write_stmt(f"INSERT INTO {table} (id, balance) VALUES (?, ?)", 2)
        commit(one_row, [101, 1000], 1)
        two_row = prepare_write_stmt(
            f"INSERT INTO {table} (id, balance) VALUES (?, ?), (?, ?)", 4
        )
        commit(two_row, [102, 2000, 103, 3000], 2)
        set_update = prepare_write_stmt(f"UPDATE {table} SET balance = ? WHERE id = ?", 2)
        commit(set_update, [1500, 101], 1)
        arith_update = prepare_write_stmt(
            f"UPDATE {table} SET balance = balance + ? WHERE id = ?", 2
        )
        commit(arith_update, [5, 102], 1)

        read = connection.prepare(f"SELECT balance FROM {table} WHERE id = ?")
        if isinstance(read, MysqlError):
            raise ProtocolError(f"read prepare failed: {read.code} {read.message}")
        rows, read_error = connection.execute(execute_payload(read.statement_id, 103))
        if read_error is not None or rows != [3000]:
            raise ProtocolError(f"point read mismatch: rows={rows} error={read_error}")

        # A duplicate-key INSERT of an existing handle must be rejected and must
        # NOT overwrite the stored row (id=101 stays 1500 from the SET above; the
        # harness re-checks it through Go TiDB).
        dup_affected, dup_error = connection.execute_write(
            execute_write_payload(one_row.statement_id, [101, 9999])
        )
        if dup_error is None:
            raise ProtocolError(
                f"duplicate INSERT of id=101 unexpectedly succeeded (affected={dup_affected})"
            )

        # Value overflow: a stored balance at i64::MAX plus a positive addend must
        # be rejected with a range error and must NOT change the stored value
        # (id=104 stays i64::MAX; the harness re-checks it through Go TiDB).
        i64_max = (1 << 63) - 1
        commit(one_row, [104, i64_max], 1)
        ov_affected, ov_error = connection.execute_write(
            execute_write_payload(arith_update.statement_id, [1, 104])
        )
        if ov_error is None:
            raise ProtocolError(
                f"overflow UPDATE of id=104 unexpectedly succeeded (affected={ov_affected})"
            )

        for prepared in (one_row, two_row, set_update, arith_update, read):
            connection.close_statement(prepared.statement_id)
        emit(
            "prepared_matrix",
            connection_id=connection.connection_id,
            one_row_insert=1,
            two_row_insert=2,
            set_update=1,
            arithmetic_update=1,
            point_read=rows[0],
            duplicate_rejected_code=dup_error.code,
            overflow_rejected_code=ov_error.code,
        )


def self_test() -> None:
    payload = execute_payload(7, -42)
    if payload != b"\x07\0\0\0\0\x01\0\0\0\0\x01\x08\0\xd6\xff\xff\xff\xff\xff\xff\xff":
        raise ProtocolError(f"execute packet self-test mismatch: {payload.hex()}")
    reused = execute_payload(7, 42, new_types=False)
    if len(reused) != 19 or reused[10] != 0 or reused[11:] != (42).to_bytes(8, "little", signed=True):
        raise ProtocolError("type-reuse packet self-test mismatch")
    expected = bytes.fromhex("21b3ff405f32cbe4aafff291396046ea29fa3a4d")
    actual = native_password_token(b"secret", bytes(range(20)))
    if actual != expected:
        raise ProtocolError(f"native password self-test mismatch: {actual.hex()}")
    emit("prepared_client_self_test", outcome="success")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    subcommands = result.add_subparsers(dest="command", required=True)
    subcommands.add_parser("self-test")
    for name in ("positive", "negative", "write", "matrix"):
        command = subcommands.add_parser(name)
        command.add_argument("--host", default="127.0.0.1")
        command.add_argument("--port", type=int, required=True)
        command.add_argument("--user", required=True)
        command.add_argument("--password", required=True)
        command.add_argument("--database", required=True)
        if name == "positive":
            command.add_argument("--first-id", type=int, required=True)
            command.add_argument("--first-balance", type=int, required=True)
            command.add_argument("--second-id", type=int, required=True)
            command.add_argument("--second-balance", type=int, required=True)
        if name == "write":
            command.add_argument("--count", type=int, required=True)
            command.add_argument("--table-size", type=int, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "self-test":
            self_test()
        elif args.command == "positive":
            positive(args)
        elif args.command == "write":
            write(args)
        elif args.command == "matrix":
            matrix(args)
        else:
            negative(args)
    except (OSError, ProtocolError, UnicodeError, ValueError) as error:
        emit("prepared_client_error", error=str(error))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
