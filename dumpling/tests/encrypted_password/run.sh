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

DB_NAME=dumpling_encrypted_password
TABLE_NAME=t
USER_NAME=dumpling_credential_test
PASSWORD=DumplingCredentialPassword42
WRONG_PASSWORD=WrongDumplingCredentialPassword43
KEY_HEX=00112233445566778899aabbccddeeff
KEY_FILE="$DUMPLING_TEST_DIR/dumpling-password.key"
PASSWORD_FILE="$DUMPLING_TEST_DIR/dumpling-password.enc"
WRONG_PASSWORD_FILE="$DUMPLING_TEST_DIR/dumpling-wrong-password.enc"
SUCCESS_LOG="$DUMPLING_OUTPUT_DIR/dumpling.log"
AUTH_OUTPUT="$DUMPLING_TEST_DIR/dumpling-auth-failure.out"
AUTH_LOG="$DUMPLING_TEST_DIR/dumpling-auth-failure.log"
AUTH_SERVER_LOG="$DUMPLING_TEST_DIR/tidb-auth-failure.log"

mysql_root() {
    mysql -uroot -h127.0.0.1 -P4000 --default-character-set=utf8mb4 "$@"
}

cleanup() {
    mysql_root --execute="DROP USER IF EXISTS '$USER_NAME'@'%'; DROP DATABASE IF EXISTS $DB_NAME;" >/dev/null 2>&1 || true
}
trap cleanup EXIT

mysql_root >/dev/null <<SQL
DROP USER IF EXISTS '$USER_NAME'@'%';
DROP DATABASE IF EXISTS $DB_NAME;
CREATE DATABASE $DB_NAME;
CREATE TABLE $DB_NAME.$TABLE_NAME (id INT PRIMARY KEY, value VARCHAR(64));
INSERT INTO $DB_NAME.$TABLE_NAME VALUES (1, 'encrypted-password');
CREATE USER '$USER_NAME'@'%' IDENTIFIED BY '$PASSWORD';
GRANT ALL PRIVILEGES ON *.* TO '$USER_NAME'@'%';
SQL

export DUMPLING_TEST_DATABASE="$DB_NAME"
export DUMPLING_TEST_PORT=4000
export DUMPLING_TEST_USER="$USER_NAME"

printf '%s' "$KEY_HEX" > "$KEY_FILE"
chmod 0600 "$KEY_FILE"
printf '%s' "$PASSWORD" | bin/tidb-credential encrypt --key-file "$KEY_FILE" --output "$PASSWORD_FILE" >/dev/null
printf '%s' "$WRONG_PASSWORD" | bin/tidb-credential encrypt --key-file "$KEY_FILE" --output "$WRONG_PASSWORD_FILE" >/dev/null

run_dumpling \
    --password-encrypted-file "$PASSWORD_FILE" \
    --password-key-file "$KEY_FILE" \
    --logfile "$SUCCESS_LOG"

DUMP_FILE="$DUMPLING_OUTPUT_DIR/$DB_NAME.$TABLE_NAME.000000000.sql"
file_should_exist "$DUMP_FILE"
grep -F -- "encrypted-password" "$DUMP_FILE" >/dev/null

AUTH_OUTPUT_DIR="$DUMPLING_TEST_DIR/dumpling-auth-failure-output"
rm -rf "$AUTH_OUTPUT_DIR"
: > "$AUTH_OUTPUT"
: > "$AUTH_LOG"
TIDB_LOG_SIZE=$(wc -c < "$DUMPLING_TEST_DIR/tidb.log")
set +e
run_dumpling \
    --password-encrypted-file "$WRONG_PASSWORD_FILE" \
    --password-key-file "$KEY_FILE" \
    --output "$AUTH_OUTPUT_DIR" \
    --logfile "$AUTH_LOG" >"$AUTH_OUTPUT" 2>&1
AUTH_STATUS=$?
set -e
test "$AUTH_STATUS" -ne 0
tail -c "+$((TIDB_LOG_SIZE + 1))" "$DUMPLING_TEST_DIR/tidb.log" > "$AUTH_SERVER_LOG"

CIPHERTEXT_HEX=$(od -An -tx1 -v "$WRONG_PASSWORD_FILE" | tr -d ' \n')
PASSWORD_DIGEST=$(printf '%s' "$PASSWORD" | openssl dgst -sha256 | awk '{print $2}')
WRONG_PASSWORD_DIGEST=$(printf '%s' "$WRONG_PASSWORD" | openssl dgst -sha256 | awk '{print $2}')
for secret in "$PASSWORD" "$WRONG_PASSWORD" "$KEY_HEX" "$CIPHERTEXT_HEX" "$PASSWORD_DIGEST" "$WRONG_PASSWORD_DIGEST"; do
    for output in "$SUCCESS_LOG" "$AUTH_OUTPUT" "$AUTH_LOG" "$AUTH_SERVER_LOG"; do
        if grep -aF -- "$secret" "$output" >/dev/null; then
            echo "sensitive value leaked into $output" >&2
            exit 1
        fi
    done
done

CONFLICT_OUTPUT_DIR="$DUMPLING_TEST_DIR/dumpling-credential-conflict-output"
CONFLICT_LOG="$DUMPLING_TEST_DIR/dumpling-credential-conflict.log"
CONFLICT_OUTPUT="$DUMPLING_TEST_DIR/dumpling-credential-conflict.out"
rm -rf "$CONFLICT_OUTPUT_DIR" "$CONFLICT_LOG"
set +e
run_dumpling \
    --password "$PASSWORD" \
    --password-encrypted-file "$PASSWORD_FILE" \
    --password-key-file "$KEY_FILE" \
    --output "$CONFLICT_OUTPUT_DIR" \
    --logfile "$CONFLICT_LOG" >"$CONFLICT_OUTPUT" 2>&1
CONFLICT_STATUS=$?
set -e
test "$CONFLICT_STATUS" -ne 0
grep -F -- "Credential:Config:ErrPasswordSourceConflict" "$CONFLICT_OUTPUT" >/dev/null
test ! -e "$CONFLICT_OUTPUT_DIR"
test ! -e "$CONFLICT_LOG"
if grep -aF -- "$PASSWORD" "$CONFLICT_OUTPUT" >/dev/null; then
    echo "plaintext password leaked into $CONFLICT_OUTPUT" >&2
    exit 1
fi

INCOMPLETE_OUTPUT_DIR="$DUMPLING_TEST_DIR/dumpling-credential-incomplete-output"
INCOMPLETE_LOG="$DUMPLING_TEST_DIR/dumpling-credential-incomplete.log"
INCOMPLETE_OUTPUT="$DUMPLING_TEST_DIR/dumpling-credential-incomplete.out"
rm -rf "$INCOMPLETE_OUTPUT_DIR" "$INCOMPLETE_LOG"
set +e
run_dumpling \
    --password-encrypted-file "$PASSWORD_FILE" \
    --output "$INCOMPLETE_OUTPUT_DIR" \
    --logfile "$INCOMPLETE_LOG" >"$INCOMPLETE_OUTPUT" 2>&1
INCOMPLETE_STATUS=$?
set -e
test "$INCOMPLETE_STATUS" -ne 0
grep -F -- "Credential:Config:ErrIncompleteEncryptedSource" "$INCOMPLETE_OUTPUT" >/dev/null
test ! -e "$INCOMPLETE_OUTPUT_DIR"
test ! -e "$INCOMPLETE_LOG"
