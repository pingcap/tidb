#!/bin/bash
#
# Copyright 2020 PingCAP, Inc.
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

set -eux
DB="$TEST_NAME"
TABLE="usertable"
DB_COUNT=3

GCS_HOST="localhost"
GCS_PORT=21808
BUCKET="test"

# we need set public-host for download file, or it will return 404 when using client to read.
bin/fake-gcs-server -scheme http -host $GCS_HOST -port $GCS_PORT -backend memory -public-host $GCS_HOST:$GCS_PORT &
i=0
while ! curl -o /dev/null -v -s "http://$GCS_HOST:$GCS_PORT/"; do
    i=$(($i+1))
    if [ $i -gt 7 ]; then
        echo 'Failed to start gcs-server'
        exit 1
    fi
    sleep 2
done

# start oauth server
bin/oauth &

stop_gcs() {
    killall -9 fake-gcs-server || true
    killall -9 oauth || true
}
trap stop_gcs EXIT

rm -rf "$TEST_DIR/$DB"
mkdir -p "$TEST_DIR/$DB"

# start gcs-server
# Fill in the database
for i in $(seq $DB_COUNT); do
    run_sql "CREATE DATABASE $DB${i};"
    go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i}
done

# we need start a oauth server or gcs client will failed to handle request.
KEY=$(cat <<- EOF
{
  "type": "service_account",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCT524vzG7uEVtX\nojcHbyQzVwlcaGkg1DWWLT+SufD08UYF0bsfcD0Etrtzo4ggwdxJQy5ygl3TNlcD\nKdelWbVyGfg9/sNB1RDlZYbQb0LVLHKjkVs7JyJsxrLk2e6NqD9ajwTEJUcLAQkj\nxlCcIi51beqrIRlvHjbtGwet/dNnRLSZf+i9SHvB2j64+RVYdnyf/IiLBvYyu7hF\nT6VjlljdbwC4TZ2jpfDL8nHRTiDiV+CX3/iH8MlMEOSM30AO5MPNVCZLlTA9W24a\nKi4NPBBlJLvG2mQELYdbhdM64iMvbPkDRtajJD6ogPB7wUoWbtSke5oOJNyV1HNt\nn91JH/dlAgMBAAECggEAQBwve2GSbfgxD0Xds4e9+dEO2jLZ6uSBS9TWOywFIa9Z\nqlkUUtbMZDgu/buTXJubeFg6EGGo+M4TnmfrNR2zFD/khj7hdS49kinVa5Dmt895\n66Osl3HprpvcXG2IxXd56q+Woc0Ew+TRiOPD+kGowLcB4ubIhw1iQpmWVRlyos6Q\nyvHssolrqOkRK9+1asixgow2Y15HtpXFN3XDIVj3gfdN1Zg80S66bTap1DS+dkJH\nSMgEZRilAjUGzbroqvZCiymlIJP5Jj5L5Wy8Qp/k1ixK10oaPgwvdmwXHX/DZ0vC\nT6XwpIaCYd3/XUWBHvrmQHFucWVPISZRi5WidggzuwKBgQDNHrxKaDrxcrV5Ncgu\npQrtQvTsIUCJGMo5m30X0Ac5CsIssOoQHdtEQW1ehJ8DtJRRb9rdWc4aelXsDUr+\no2m1zyZzM6S7IO2YhGDAo7Uu3fy1r33qYAt6uS/nHaJBpsKcyqqK+0wPDikdPLLx\nBBWZHF6WoswDEUVLQa/hHgpjPwKBgQC4l2/6xShNoobivzk8AE/Acq7PazA8gu4K\nY0UghTBlAst4RvBTURYZ2V3uw0S2FbfwL0/snHhNWZl5XjBX/H9oQmLri5qGOOpf\n9A11p5kd0x1mHDgTm/k7EgoskdXGB5NqXIB7l/3UI8Sk2N1PzHwyJJYfaB+EWTs8\n+LVy99VQWwKBgQCilRwVtiwSOSPSYWi8YCEbEpljmK+4eye/JZmviDpRYk+qcMf1\n4lRr85gm9OO9YiK1sf0+ufH9Vr5IDflFgG1HqFwHsAWANYdd/n9Z8eior1ehAurB\nHUO8EJEBlaGIfA+Bi7pF0w3kWQsJm5USKHSeGbh3ma4vOD8+eWBZBSCirQKBgQCe\n1uEq/sChnXtIXpgXg4Uc6xJ1tZy6VUgUdDulsjZklTUU+KYQa7QC5kKoFCtqK+It\nseiqiDIVDUa9Y0liTQotYwLQAT8kxJEZpF54oZFmUqX3mcy/QvYB2JIcrBkx4I7/\ndT2yHKX1CBpMZ7h41FMCquzrdaO5NTd+Td2FYrGSBQKBgEBnAerHh/NafYlVumlS\nVgouR9IketTegyEyntVyEvENx8OA5ZLMywCIKbPMFZgPR0RgDpyDxKauCU2E09e/\nboN76UOuOg11fknJh7vFbUbzM6BXvXVOTyX9ZtZBQcd5Y3tV+tYD1tHUgurGYWb+\nyHLBMOlXdpn0gZ4rwoIQgzD9\n-----END PRIVATE KEY-----\n",
  "client_email": "test@email.com",
  "token_uri": "http://localhost:5000/oauth/token"
}
EOF
)

# save CREDENTIALS to file
echo $KEY > "tests/$TEST_NAME/config.json"

# export test CREDENTIALS for gcs oauth
export GOOGLE_APPLICATION_CREDENTIALS="tests/$TEST_NAME/config.json"

# create gcs bucket
curl -XPOST http://$GCS_HOST:$GCS_PORT/storage/v1/b -d '{"name":"test"}'

for i in $(seq $DB_COUNT); do
  row_count_ori[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done

# new version backup full
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "gcs://$BUCKET/$DB?endpoint=http://$GCS_HOST:$GCS_PORT/storage/v1/"

# old version backup full v4.0.8 and disable check-requirements
echo "v4.0.8 backup start..."
bin/brv4.0.8 backup full \
    -L "debug" \
    --ca "$TEST_DIR/certs/ca.pem" \
    --cert "$TEST_DIR/certs/br.pem" \
    --key "$TEST_DIR/certs/br.key" \
    --pd $PD_ADDR -s "gcs://$BUCKET/${DB}_old?endpoint=http://$GCS_HOST:$GCS_PORT/storage/v1/" --check-requirements=false

# clean up
for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done

# new version restore full
echo "restore start..."
run_br restore full -s "gcs://$BUCKET/$DB?" --pd $PD_ADDR --gcs.endpoint="http://$GCS_HOST:$GCS_PORT/storage/v1/" --check-requirements=false

for i in $(seq $DB_COUNT); do
    row_count_new[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done

fail=false
for i in $(seq $DB_COUNT); do
    if [ "${row_count_ori[i]}" != "${row_count_new[i]}" ];then
        fail=true
        echo "TEST: [$TEST_NAME] fail on database $DB${i}"
    fi
    echo "database $DB${i} [original] row count: ${row_count_ori[i]}, [after br] row count: ${row_count_new[i]}"
done

if $fail; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
else
    echo "TEST: [$TEST_NAME] new version successd!"
fi

# clean up
for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done

echo "v4.0.8 version restore start..."
run_br restore full -s "gcs://$BUCKET/${DB}_old" --pd $PD_ADDR --gcs.endpoint="http://$GCS_HOST:$GCS_PORT/storage/v1/" --check-requirements=false

for i in $(seq $DB_COUNT); do
    row_count_new[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done

fail=false
for i in $(seq $DB_COUNT); do
    if [ "${row_count_ori[i]}" != "${row_count_new[i]}" ];then
        fail=true
        echo "TEST: [$TEST_NAME] fail on database $DB${i}"
    fi
    echo "database $DB${i} [original] row count: ${row_count_ori[i]}, [after br] row count: ${row_count_new[i]}"
done

if $fail; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi
