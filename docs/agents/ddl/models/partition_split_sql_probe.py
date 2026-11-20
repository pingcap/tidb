# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Probe transaction prerequisites in an owned local TiDB/unistore process.

Requires PyMySQL and a prebuilt TiDB binary. Never connects to an external server.
The fixture tables represent record namespaces; this is NOT a DXF, SST, actual
hidden-rowid allocator, or production reorg implementation test. Artifacts and
binary identity are retained; the owned process is always terminated.
"""
import argparse
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time

import pymysql


def execute(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def begin(conn):
    execute(conn, 'BEGIN OPTIMISTIC')


def choose_handle(conn, parent, child, preferred, locking=False):
    # Deterministic candidate generator, not TiDB's hidden-rowid allocator.
    candidate = preferred
    suffix = ' FOR UPDATE' if locking else ''
    while (execute(conn, 'SELECT h FROM old_rows WHERE p=%s AND h=%s' + suffix, (parent, candidate))
           or execute(conn, 'SELECT h FROM new_rows WHERE c=%s AND h=%s' + suffix, (child, candidate))):
        candidate += 1
    return candidate


def insert_pair(conn, parent, child, handle, value):
    execute(conn, 'INSERT INTO new_rows VALUES (%s,%s,%s)', (child, handle, value))
    execute(conn, 'INSERT INTO old_rows VALUES (%s,%s,%s,%s)', (parent, handle, child, value))


def layout_rows(conn):
    old = execute(conn, 'SELECT p,h,c,v FROM old_rows ORDER BY p,h')
    new = execute(conn, 'SELECT c,h,v FROM new_rows ORDER BY c,h')
    expected = sorted((c, h, v) for p, h, c, v in old)
    assert all(p == c // 2 for p, h, c, v in old)
    assert expected == list(new), (old, new)
    return {'old': old, 'new': new}


def expect_conflict(conn):
    try:
        conn.commit()
    except pymysql.MySQLError as error:
        conn.rollback()
        # Either a write conflict or a duplicate-key constraint proves the loser
        # did not silently overwrite the parent record. Record the actual error.
        assert error.args[0] in (9007, 1062), error.args
        return {'code': error.args[0], 'message': str(error)}
    raise AssertionError('a competing stale writer unexpectedly committed')


def transaction_probes(connect):
    a, b, check = connect(), connect(), connect()
    execute(check, 'CREATE TABLE old_rows (p INT,h BIGINT,c INT,v VARCHAR(20),PRIMARY KEY(p,h) CLUSTERED)')
    execute(check, 'CREATE TABLE new_rows (c INT,h BIGINT,v VARCHAR(20),PRIMARY KEY(c,h) CLUSTERED)')
    execute(check, "INSERT INTO old_rows VALUES (0,7,0,'A'),(1,7,2,'C')")
    execute(check, "INSERT INTO new_rows VALUES (0,7,'A'),(2,7,'C')")

    begin(a)
    begin(b)
    ah = choose_handle(a, 1, 2, 7)
    bh = choose_handle(b, 1, 3, 7)
    assert ah == bh == 8
    insert_pair(a, 1, 2, ah, 'X')
    insert_pair(b, 1, 3, bh, 'Y')
    a.commit()
    conflict = expect_conflict(b)
    layout_rows(check)
    begin(b)
    retry = choose_handle(b, 1, 3, 7)
    assert retry == 9
    insert_pair(b, 1, 3, retry, 'Y')
    b.commit()
    after_retry = layout_rows(check)

    # Cross-parent move, preserving the business value but selecting a different
    # hidden-handle candidate because the destination parent has 7,8,9 occupied.
    begin(a)
    assert execute(a, 'SELECT v FROM new_rows WHERE c=0 AND h=7') == (('A',),)
    execute(a, 'DELETE FROM new_rows WHERE c=0 AND h=7')
    execute(a, 'DELETE FROM old_rows WHERE p=0 AND h=7')
    moved = choose_handle(a, 1, 3, 7)
    assert moved == 10
    insert_pair(a, 1, 3, moved, 'A')
    a.commit()
    after_move = layout_rows(check)
    assert sorted(row[2] for row in after_move['new']) == ['A', 'C', 'X', 'Y']

    before = layout_rows(check)
    begin(a)
    execute(a, 'DELETE FROM new_rows WHERE c=3 AND h=10')
    execute(a, 'DELETE FROM old_rows WHERE p=1 AND h=10')
    a.rollback()
    assert before == layout_rows(check)

    execute(check, 'CREATE TABLE source_record (k INT PRIMARY KEY,v VARCHAR(20))')
    execute(check, 'CREATE TABLE target_record LIKE source_record')
    execute(check, 'CREATE TABLE temp_record LIKE source_record')
    for table in ('source_record', 'target_record', 'temp_record'):
        execute(check, f"INSERT INTO {table} VALUES (1,'v1')")
    begin(a)
    assert execute(a, 'SELECT v FROM temp_record WHERE k=1') == (('v1',),)
    execute(a, "UPDATE target_record SET v='v1' WHERE k=1")
    execute(a, 'DELETE FROM temp_record WHERE k=1')
    begin(b)
    for table in ('source_record', 'target_record', 'temp_record'):
        execute(b, f"UPDATE {table} SET v='v2' WHERE k=1")
    b.commit()
    merge_conflict = expect_conflict(a)
    begin(a)
    latest = execute(a, 'SELECT v FROM temp_record WHERE k=1')[0][0]
    assert latest == 'v2'
    execute(a, 'UPDATE target_record SET v=%s WHERE k=1', (latest,))
    execute(a, 'DELETE FROM temp_record WHERE k=1')
    a.commit()
    assert execute(check, 'SELECT v FROM target_record') == (('v2',),)
    assert execute(check, 'SELECT v FROM source_record') == (('v2',),)
    assert not execute(check, 'SELECT * FROM temp_record')
    return {'parent_claim_conflict': conflict, 'retry_handle': retry,
            'cross_parent_handle': moved, 'after_retry': after_retry,
            'after_cross_parent_move': after_move, 'user_rollback': 'PASS',
            'temp_merge_conflict': merge_conflict, 'latest_temp_replay': 'PASS'}


def marker_probes(connect):
    merge, dml, check = connect(), connect(), connect()
    # Startup: a captured v1 can precede an old-schema uncaptured v2 commit.
    execute(check, "UPDATE source_record SET v='v1' WHERE k=1")
    execute(check, "INSERT INTO temp_record VALUES (1,'v1')")
    execute(check, "UPDATE source_record SET v='v2' WHERE k=1")
    execute(check, "UPDATE target_record SET v='v2' WHERE k=1")
    old_value = execute(check, 'SELECT v FROM temp_record WHERE k=1')[0][0]
    execute(check, 'UPDATE target_record SET v=%s WHERE k=1', (old_value,))
    counterexample = {'source': execute(check, 'SELECT v FROM source_record')[0][0],
                      'target': execute(check, 'SELECT v FROM target_record')[0][0]}
    assert counterexample == {'source': 'v2', 'target': 'v1'}
    execute(check, 'CREATE TABLE dirty_record (k INT PRIMARY KEY,nonce BIGINT)')
    execute(check, 'INSERT INTO dirty_record VALUES (1,1)')
    begin(merge)
    assert execute(merge, 'SELECT nonce FROM dirty_record WHERE k=1')
    current = execute(merge, 'SELECT v FROM source_record WHERE k=1')[0][0]
    execute(merge, 'UPDATE target_record SET v=%s WHERE k=1', (current,))
    execute(merge, 'DELETE FROM dirty_record WHERE k=1')
    merge.commit()
    assert execute(check, 'SELECT v FROM target_record') == (('v2',),)

    # Force an actual marker write in every DML transaction: SQL no-op updates
    # are not a substitute for a raw KV Set/transaction nonce.
    for table in ('source_record', 'target_record'):
        execute(check, f"UPDATE {table} SET v='v1' WHERE k=1")
    execute(check, 'INSERT INTO dirty_record VALUES (1,1)')
    begin(merge)
    execute(merge, 'SELECT nonce FROM dirty_record WHERE k=1')
    stale_source = execute(merge, 'SELECT v FROM source_record WHERE k=1')[0][0]
    begin(dml)
    for table in ('source_record', 'target_record'):
        execute(dml, f"UPDATE {table} SET v='v2' WHERE k=1")
    execute(dml, 'UPDATE dirty_record SET nonce=2 WHERE k=1')
    dml.commit()
    execute(merge, 'UPDATE target_record SET v=%s WHERE k=1', (stale_source,))
    execute(merge, 'DELETE FROM dirty_record WHERE k=1')
    conflict = expect_conflict(merge)
    begin(merge)
    execute(merge, 'SELECT nonce FROM dirty_record WHERE k=1')
    fresh = execute(merge, 'SELECT v FROM source_record WHERE k=1')[0][0]
    execute(merge, 'UPDATE target_record SET v=%s WHERE k=1', (fresh,))
    execute(merge, 'DELETE FROM dirty_record WHERE k=1')
    merge.commit()
    assert fresh == 'v2'

    # Dirty old destinations must delete, not copy a source row now routed away.
    execute(check, 'CREATE TABLE dirty_slots (c INT,h BIGINT,nonce BIGINT,PRIMARY KEY(c,h) CLUSTERED)')
    for child, handle, value in ((2, 10, 'stale-A'), (0, 99, 'ghost')):
        execute(check, 'INSERT INTO new_rows VALUES (%s,%s,%s)', (child, handle, value))
        execute(check, 'INSERT INTO dirty_slots VALUES (%s,%s,1)', (child, handle))
        begin(merge)
        execute(merge, 'SELECT nonce FROM dirty_slots WHERE c=%s AND h=%s', (child, handle))
        source = execute(merge, 'SELECT c,v FROM old_rows WHERE p=%s AND h=%s', (child // 2, handle))
        assert not source or source[0][0] != child
        execute(merge, 'DELETE FROM new_rows WHERE c=%s AND h=%s', (child, handle))
        execute(merge, 'DELETE FROM dirty_slots WHERE c=%s AND h=%s', (child, handle))
        merge.commit()
    layout_rows(check)

    # A transaction fence also protects repair workers after the source layout
    # ceases to be authoritative. Closing the gate must race the same write key.
    execute(check, 'CREATE TABLE repair_gate (g INT PRIMARY KEY,state VARCHAR(10),nonce BIGINT)')
    execute(check, "INSERT INTO repair_gate VALUES (1,'open',0)")
    execute(check, 'INSERT INTO dirty_record VALUES (1,3)')
    begin(merge)
    assert execute(merge, 'SELECT state FROM repair_gate WHERE g=1') == (('open',),)
    execute(merge, 'SELECT nonce FROM dirty_record WHERE k=1')
    source = execute(merge, 'SELECT v FROM source_record WHERE k=1')[0][0]
    execute(merge, 'UPDATE target_record SET v=%s WHERE k=1', (source,))
    execute(merge, 'DELETE FROM dirty_record WHERE k=1')
    execute(merge, 'UPDATE repair_gate SET nonce=nonce+1 WHERE g=1')
    execute(check, "UPDATE repair_gate SET state='closed',nonce=nonce+1 WHERE g=1")
    fence_conflict = expect_conflict(merge)
    assert execute(check, 'SELECT state FROM repair_gate WHERE g=1') == (('closed',),)
    return {'full_value_startup_counterexample': counterexample,
            'dirty_source_repair': 'PASS', 'marker_conflict': conflict,
            'routed_away_and_absent_source_delete': 'PASS',
            'repair_gate_conflict': fence_conflict, 'closed_gate_observed': True}


def pessimistic_probe(connect):
    a, b, check = connect(), connect(), connect()
    for conn in (a, b):
        execute(conn, 'SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    execute(b, 'SET SESSION innodb_lock_wait_timeout=1')
    execute(a, 'BEGIN PESSIMISTIC')
    assert not execute(a, 'SELECT h FROM old_rows WHERE p=1 AND h=20 FOR UPDATE')
    execute(b, 'BEGIN PESSIMISTIC')
    try:
        execute(b, 'SELECT h FROM old_rows WHERE p=1 AND h=20 FOR UPDATE')
    except pymysql.MySQLError as error:
        assert error.args[0] == 1205, error.args
        blocked = {'code': error.args[0], 'message': str(error)}
        b.rollback()
    else:
        raise AssertionError('RR absent parent point key was not locked')
    insert_pair(a, 1, 2, 20, 'P')
    a.commit()
    execute(b, 'BEGIN PESSIMISTIC')
    candidate = choose_handle(b, 1, 3, 20, locking=True)
    assert candidate == 21
    insert_pair(b, 1, 3, candidate, 'Q')
    b.commit()
    layout_rows(check)
    return {'rr_absent_parent_key_lock': blocked, 'retry_handle': candidate,
            'scope': 'SQL point locking in RR; no direct KV helper or RC verification'}


def mdl_probe(connect):
    a, check = connect(), connect()
    enabled = execute(check, 'SELECT @@global.tidb_enable_metadata_lock')[0][0]
    assert str(enabled).lower() in ('1', 'on'), enabled
    execute(check, '''CREATE TABLE sql_shape (id INT,k INT,KEY idx_id(id))
        PARTITION BY RANGE(k) (PARTITION p0 VALUES LESS THAN (100),
                              PARTITION p1 VALUES LESS THAN (200))''')
    execute(check, 'INSERT INTO sql_shape VALUES (1,25),(2,75),(3,125),(4,175)')
    execute(check, 'CREATE TABLE exchange_seed (id INT,k INT,KEY idx_id(id))')
    execute(check, 'INSERT INTO exchange_seed VALUES (3,125),(4,175)')
    execute(check, 'ALTER TABLE sql_shape EXCHANGE PARTITION p1 WITH TABLE exchange_seed')
    before_handles = execute(check, 'SELECT id,_tidb_rowid FROM sql_shape ORDER BY id')
    left_handles = execute(check, 'SELECT _tidb_rowid FROM sql_shape PARTITION(p0)')
    right_handles = execute(check, 'SELECT _tidb_rowid FROM sql_shape PARTITION(p1)')
    assert set(left_handles) & set(right_handles), (left_handles, right_handles)
    begin(a)
    assert len(execute(a, 'SELECT * FROM sql_shape')) == 4
    done = threading.Event()
    errors = []

    def ddl():
        conn = connect()
        try:
            execute(conn, '''ALTER TABLE sql_shape REORGANIZE PARTITION p0,p1 INTO (
                PARTITION q00 VALUES LESS THAN (50),PARTITION q01 VALUES LESS THAN (100),
                PARTITION q10 VALUES LESS THAN (150),PARTITION q11 VALUES LESS THAN (200))''')
        except Exception as error:
            errors.append(str(error))
        finally:
            conn.close()
            done.set()

    thread = threading.Thread(target=ddl, daemon=True)
    thread.start()
    blocked = None
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and not done.is_set():
        with check.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute('ADMIN SHOW DDL JOBS 10')
            for row in cur.fetchall():
                names = {str(k).lower(): v for k, v in row.items()}
                if (names.get('table_name') == 'sql_shape'
                        and 'reorganize' in str(names.get('job_type', '')).lower()
                        and str(names.get('schema_state', '')).lower() == 'delete only'
                        and str(names.get('state', '')).lower() not in ('synced', 'done', 'cancelled')):
                    blocked = {k: str(v) for k, v in names.items()}
        if blocked:
            break
        time.sleep(0.1)
    assert blocked and not done.is_set(), (blocked, errors)
    # Observe a stable block beyond two configured 1s leases, rather than just
    # catching the DDL at an intermediate state on its ordinary successful path.
    held_until = time.monotonic() + 2.5
    while time.monotonic() < held_until:
        assert not done.is_set(), errors
        with check.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute('ADMIN SHOW DDL JOBS 10')
            matches = [{str(k).lower(): v for k, v in row.items()} for row in cur.fetchall()]
        assert any(str(row.get('job_id')) == blocked['job_id']
                   and str(row.get('schema_state')).lower() == 'delete only' for row in matches)
        time.sleep(0.1)
    a.commit()
    thread.join(timeout=30)
    assert done.is_set() and not errors, errors
    assert execute(check, 'SELECT COUNT(*),SUM(id) FROM sql_shape') == ((4, 10),)
    parts = execute(check, '''SELECT partition_name FROM information_schema.partitions
        WHERE table_schema=DATABASE() AND table_name='sql_shape' ORDER BY partition_ordinal_position''')
    assert parts == (('q00',), ('q01',), ('q10',), ('q11',)), parts
    after_handles = execute(check, 'SELECT id,_tidb_rowid FROM sql_shape ORDER BY id')
    assert before_handles == after_handles, (before_handles, after_handles)
    execute(check, 'ADMIN CHECK TABLE sql_shape')
    return {'duplicate_old_parent_handles': sorted(set(left_handles) & set(right_handles)),
            'handles_before': before_handles, 'handles_after': after_handles,
            'implicit_handles_preserved': True, 'observed_blocked_job': blocked, 'observed_block_seconds': 2.5, 'released_after_old_txn_commit': True,
            'multi_partition_sql_split': 'PASS', 'admin_check_table': 'PASS'}


def datetime_probe(connect):
    conn = connect()
    execute(conn, """CREATE TABLE date_shape (id INT,create_time DATETIME,
        KEY local_id(id),KEY global_id(id) GLOBAL)
        PARTITION BY RANGE(TO_DAYS(create_time)) (
        PARTITION p0 VALUES LESS THAN (TO_DAYS('2026-11-01')),
        PARTITION pmax VALUES LESS THAN (MAXVALUE))""")
    execute(conn, """INSERT INTO date_shape VALUES
        (1,'2026-10-01'),(2,'2026-10-20'),(3,'2026-11-05'),(4,'2026-11-20'),(5,NULL)""")
    handles = execute(conn, 'SELECT id,_tidb_rowid FROM date_shape ORDER BY id')
    execute(conn, """ALTER TABLE date_shape REORGANIZE PARTITION p0,pmax INTO (
        PARTITION oct_a VALUES LESS THAN (TO_DAYS('2026-10-15')),
        PARTITION oct_b VALUES LESS THAN (TO_DAYS('2026-11-01')),
        PARTITION nov_a VALUES LESS THAN (TO_DAYS('2026-11-15')),
        PARTITION pmax_new VALUES LESS THAN (MAXVALUE))""")
    assert handles == execute(conn, 'SELECT id,_tidb_rowid FROM date_shape ORDER BY id')
    expected = {'oct_a': ((1,), (5,)), 'oct_b': ((2,),), 'nov_a': ((3,),), 'pmax_new': ((4,),)}
    for zone in ('+00:00', '+08:00'):
        execute(conn, 'SET SESSION time_zone=%s', (zone,))
        for partition, rows in expected.items():
            assert execute(conn, f'SELECT id FROM date_shape PARTITION({partition}) ORDER BY id') == rows
        assert execute(conn, 'SELECT id FROM date_shape FORCE INDEX(global_id) WHERE id IN (1,3,5) ORDER BY id') == ((1,), (3,), (5,))
    execute(conn, 'ADMIN CHECK TABLE date_shape')
    return {'to_days_datetime_and_null': 'PASS', 'time_zones': ['+00:00', '+08:00'],
            'implicit_handles_preserved': True, 'global_index_forced_reads': 'PASS',
            'scope': 'existing txn-mode REORGANIZE, not the proposed global-sort record path'}


def late_snapshot_probe(connect, bind_read_layout=False):
    reader, ddl = connect(), connect()
    execute(ddl, 'CREATE TABLE snapshot_clock (id INT PRIMARY KEY)')
    execute(ddl, 'INSERT INTO snapshot_clock VALUES (1)')
    execute(ddl, """CREATE TABLE late_shape (id INT,k INT,KEY g_id(id) GLOBAL)
        PARTITION BY RANGE(k) (PARTITION p0 VALUES LESS THAN (100),
                              PARTITION p1 VALUES LESS THAN (200))""")
    execute(ddl, 'INSERT INTO late_shape VALUES (1,25),(2,75),(3,125),(4,175)')
    execute(reader, 'SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    begin(reader)
    execute(reader, 'SELECT * FROM snapshot_clock')
    start_ts = str(execute(reader, 'SELECT @@tidb_current_ts')[0][0])
    execute(ddl, """ALTER TABLE late_shape REORGANIZE PARTITION p0,p1 INTO (
        PARTITION q00 VALUES LESS THAN (50),PARTITION q01 VALUES LESS THAN (100),
        PARTITION q10 VALUES LESS THAN (150),PARTITION q11 VALUES LESS THAN (200))""")
    fresh = execute(ddl, 'SELECT id,k FROM late_shape ORDER BY id')
    report = {'old_start_ts': start_ts, 'fresh_rows': fresh,
              'scope': 'old dirty binary; snapshot binding primitive, not a production RR hook'}
    try:
        observed = execute(reader, 'SELECT id,k FROM late_shape ORDER BY id')
        report['old_snapshot_rows'] = observed
        report['status'] = 'MATCH' if observed == fresh else 'SILENT_MISMATCH'
    except pymysql.MySQLError as error:
        report['old_reader_error'] = {'code': error.args[0], 'message': str(error)}
        report['status'] = 'EXPLICIT_ERROR_REQUIRES_CLASSIFICATION'
    reader.rollback()
    if bind_read_layout:
        historical = connect()
        # Same fixture-only safe point as testkit.MockGCSavePoint. Unistore
        # has no real GC worker; this only enables the snapshot-schema API.
        execute(historical, "INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point','20160102-15:04:05 -0700','probe fixture') ON DUPLICATE KEY UPDATE variable_value=VALUES(variable_value)")
        execute(historical, 'SET SESSION tidb_snapshot=%s', (start_ts,))
        bound = execute(historical, 'SELECT id,k FROM late_shape ORDER BY id')
        report['snapshot_schema_bound_rows'] = bound
        index_rows = execute(historical, 'SELECT id,k FROM late_shape FORCE INDEX(g_id) WHERE id>=0 ORDER BY id')
        report['snapshot_schema_global_index_rows'] = index_rows
        assert bound == fresh and index_rows == fresh, report
        report['status'] = 'BINDING_PRIMITIVE_MATCH'
        report['binding_mechanism'] = 'existing tidb_snapshot uses metadata and data at the same readTS'
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, default=Path('bin/tidb-server'))
    parser.add_argument('--late-snapshot', action='store_true')
    parser.add_argument('--bind-read-layout', action='store_true')
    args = parser.parse_args()
    binary = args.binary.resolve()
    artifact_root = Path('.codex-tmp').resolve()
    artifact_root.mkdir(parents=True, exist_ok=True)
    artifacts = Path(tempfile.mkdtemp(prefix='ps-', dir=artifact_root))
    version = subprocess.check_output([str(binary), '-V'], text=True)
    (artifacts / 'version.txt').write_text(version)
    result = {'binary': str(binary), 'version': version, 'store': 'unistore',
              'artifacts': str(artifacts), 'scope': 'SQL prerequisite probe; no DXF/SST/hidden allocator'}
    with socket.socket() as free:
        free.bind(('127.0.0.1', 0))
        port = free.getsockname()[1]
    socket_path = artifacts / 'sock'
    connections = []
    process = None
    output = (artifacts / 'stdout.log').open('w')

    def connect():
        conn = pymysql.connect(unix_socket=str(socket_path), user='root', password='',
                               autocommit=True, connect_timeout=2, read_timeout=40, write_timeout=10)
        execute(conn, 'USE split_proto_probe')
        execute(conn, 'SET SESSION tidb_retry_limit=0')
        connections.append(conn)
        return conn

    try:
        process = subprocess.Popen([str(binary), '-store=unistore', '-path', str(artifacts / 'store'),
                                    '-host=127.0.0.1', '-P', str(port), '-socket', str(socket_path),
                                    '-report-status=false', '-lease=1s', '-L=warn',
                                    '-log-file', str(artifacts / 'tidb.log')], stdout=output, stderr=output)
        result['pid'] = process.pid
        deadline = time.monotonic() + 50
        while True:
            if process.poll() is not None:
                raise RuntimeError(f'owned server exited; see {artifacts}')
            try:
                ready = pymysql.connect(unix_socket=str(socket_path), user='root', password='',
                                        autocommit=True, connect_timeout=1, read_timeout=5)
                execute(ready, 'CREATE DATABASE split_proto_probe')
                ready.close()
                break
            except pymysql.MySQLError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.2)
        print(f'Owned unistore ready; artifacts: {artifacts}', flush=True)
        if args.late_snapshot:
            result['late_snapshot'] = late_snapshot_probe(connect, args.bind_read_layout)
            result['status'] = result['late_snapshot']['status']
            print(json.dumps(result['late_snapshot'], default=str), flush=True)
            return 0 if result['status'] in ('MATCH', 'BINDING_PRIMITIVE_MATCH') else 1
        result['transactions'] = transaction_probes(connect)
        print('PASS: parent record conflict/retry, cross-parent move, user rollback, temp replay', flush=True)
        result['markers'] = marker_probes(connect)
        print('PASS: startup replay counterexample, current-source repair and transactional repair fence', flush=True)
        result['pessimistic'] = pessimistic_probe(connect)
        print('PASS: pessimistic RR absent-parent lock and retry', flush=True)
        result['mdl'] = mdl_probe(connect)
        print('PASS: MDL blocked multi-partition REORGANIZE until old transaction ended', flush=True)
        result['datetime'] = datetime_probe(connect)
        print('PASS: TO_DAYS(datetime), NULL, unchanged hidden handles and global-index reads', flush=True)
        result['status'] = 'PASS'
        return 0
    except Exception as error:
        result['status'] = 'FAIL'
        result['error'] = repr(error)
        raise
    finally:
        for conn in connections:
            try:
                conn.close()
            except Exception:
                pass
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        result['owned_process_stopped'] = process is None or process.poll() is not None
        output.close()
        (artifacts / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n')
        print(f'Result: {artifacts / "result.json"}', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
