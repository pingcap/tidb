#!/usr/bin/env bash
#
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

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
USER_NAME=lightning_credential_test
PASSWORD=LightningCredentialPassword42
WRONG_PASSWORD=WrongLightningCredentialPassword43
KEY_HEX=00112233445566778899aabbccddeeff
KEY_FILE="$TEST_DIR/lightning-password.key"
PASSWORD_FILE="$TEST_DIR/lightning-password.enc"
WRONG_PASSWORD_FILE="$TEST_DIR/lightning-wrong-password.enc"
AUTH_OUTPUT="$TEST_DIR/lightning-auth-failure.out"
AUTH_LOG="$TEST_DIR/lightning-auth-failure.log"
AUTH_SERVER_LOG="$TEST_DIR/tidb-auth-failure.log"

mysql_root() {
    mysql -uroot -h127.0.0.1 -P4000 \
        --ssl-ca="$TEST_DIR/certs/ca.pem" \
        --ssl-cert="$TEST_DIR/certs/curl.pem" \
        --ssl-key="$TEST_DIR/certs/curl.key" \
        --default-character-set=utf8 "$@"
}

cleanup() {
    mysql_root --execute="DROP USER IF EXISTS '$USER_NAME'@'%';" >/dev/null 2>&1 || true
}
trap cleanup EXIT

mysql_root >/dev/null <<SQL
DROP USER IF EXISTS '$USER_NAME'@'%';
CREATE USER '$USER_NAME'@'%' IDENTIFIED BY '$PASSWORD';
GRANT ALL PRIVILEGES ON *.* TO '$USER_NAME'@'%';
SQL

printf '%s' "$KEY_HEX" > "$KEY_FILE"
chmod 0600 "$KEY_FILE"
printf '%s' "$PASSWORD" | tidb-credential encrypt --key-file "$KEY_FILE" --output "$PASSWORD_FILE" >/dev/null
printf '%s' "$WRONG_PASSWORD" | tidb-credential encrypt --key-file "$KEY_FILE" --output "$WRONG_PASSWORD_FILE" >/dev/null

run_sql 'DROP DATABASE IF EXISTS lightning_encrypted_password;'
run_sql 'DROP DATABASE IF EXISTS tidb_lightning_checkpoint;'
run_lightning \
    --tidb-user "$USER_NAME" \
    --tidb-password-encrypted-file "$PASSWORD_FILE" \
    --tidb-password-key-file "$KEY_FILE" \
    --enable-checkpoint=1

run_sql 'SELECT id, value FROM lightning_encrypted_password.t;'
check_contains 'id: 1'
check_contains 'value: encrypted-password'

CHECKPOINT_DUMP="$TEST_DIR/checkpoint-dump"
run_lightning_ctl \
    --config "$CUR/config.toml" \
    --tidb-user "$USER_NAME" \
    --tidb-password-encrypted-file "$PASSWORD_FILE" \
    --tidb-password-key-file "$KEY_FILE" \
    --enable-checkpoint=1 \
    --checkpoint-dump "$CHECKPOINT_DUMP"
test -f "$CHECKPOINT_DUMP/tables.csv"
test -f "$CHECKPOINT_DUMP/engines.csv"
test -f "$CHECKPOINT_DUMP/chunks.csv"

run_lightning_ctl \
    --config "$CUR/config.toml" \
    --tidb-password-encrypted-file "$TEST_DIR/missing-password.enc" \
    --tidb-password-key-file "$TEST_DIR/missing-password.key" \
    --fetch-mode >"$TEST_DIR/fetch-mode.out" 2>&1

: > "$AUTH_OUTPUT"
: > "$AUTH_LOG"
TIDB_LOG_SIZE=$(wc -c < "$TEST_DIR/tidb.log")
set +e
run_lightning \
    --tidb-user "$USER_NAME" \
    --tidb-password-encrypted-file "$WRONG_PASSWORD_FILE" \
    --tidb-password-key-file "$KEY_FILE" \
    --enable-checkpoint=1 \
    --log-file "$AUTH_LOG" >"$AUTH_OUTPUT" 2>&1
AUTH_STATUS=$?
set -e
test "$AUTH_STATUS" -ne 0
tail -c "+$((TIDB_LOG_SIZE + 1))" "$TEST_DIR/tidb.log" > "$AUTH_SERVER_LOG"

CIPHERTEXT_HEX=$(od -An -tx1 -v "$WRONG_PASSWORD_FILE" | tr -d ' \n')
PASSWORD_DIGEST=$(printf '%s' "$WRONG_PASSWORD" | openssl dgst -sha256 | awk '{print $2}')
for secret in "$PASSWORD" "$WRONG_PASSWORD" "$KEY_HEX" "$CIPHERTEXT_HEX" "$PASSWORD_DIGEST"; do
    for output in "$AUTH_OUTPUT" "$AUTH_LOG" "$AUTH_SERVER_LOG"; do
        if grep -aF -- "$secret" "$output" >/dev/null; then
            echo "sensitive value leaked into $output" >&2
            exit 1
        fi
    done
done
