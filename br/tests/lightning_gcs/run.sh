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
DB="gcs_test"
TABLE="tbl"

check_cluster_version 4 0 0 'local backend' || exit 0

GCS_HOST="localhost"
GCS_PORT=21808
GCS_STORAGE=$TEST_DIR/storage
BUCKET="lightning-gcs"
rm -rf "$TEST_DIR/$DB"
# NOTE: if the bucket alredy exists, 
#       the opration of the bucket in fake-gcs-server will fail.
rm -rf $GCS_STORAGE
mkdir -p "$TEST_DIR/$DB"

# we need set public-host for download file, or it will return 404 when using client to read.
bin/fake-gcs-server -scheme http -host $GCS_HOST -port $GCS_PORT -filesystem-root $GCS_STORAGE -public-host $GCS_HOST:$GCS_PORT &
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
curl -XPOST http://$GCS_HOST:$GCS_PORT/storage/v1/b -d "{\"name\":\"${BUCKET}\"}"

DATA_PATH="$TEST_DIR/$DB"
mkdir -p $DATA_PATH/$DB
echo "CREATE DATABASE $DB;" > "$DATA_PATH/$DB-schema-create.sql"
echo "CREATE TABLE $TABLE(i INT, s varchar(32));" > "$DATA_PATH/$DB.$TABLE-schema.sql"
echo "INSERT INTO $TABLE (i, s) VALUES (1, \"1\"),(2, \"test2\"), (3, \"qqqtest\");" > "$DATA_PATH/$DB.$TABLE.sql"
cat > "$DATA_PATH/$DB.$TABLE.0.csv" << _EOF_
i,s
100,"test100"
101,"\""
102,"😄😄😄😄😄"
104,""
_EOF_

# upload files to gcs
curl -XPOST --data-binary @$DATA_PATH/$DB-schema-create.sql "http://$GCS_HOST:$GCS_PORT/upload/storage/v1/b/$BUCKET/o?uploadType=media&name=$DB/$DB-schema-create.sql"
curl -XPOST --data-binary @$DATA_PATH/$DB.$TABLE-schema.sql "http://$GCS_HOST:$GCS_PORT/upload/storage/v1/b/$BUCKET/o?uploadType=media&name=$DB/$DB.$TABLE-schema.sql"
curl -XPOST --data-binary @$DATA_PATH/$DB.$TABLE.sql "http://$GCS_HOST:$GCS_PORT/upload/storage/v1/b/$BUCKET/o?uploadType=media&name=$DB/$DB.$TABLE.sql"
curl -XPOST --data-binary @$DATA_PATH/$DB.$TABLE.0.csv "http://$GCS_HOST:$GCS_PORT/upload/storage/v1/b/$BUCKET/o?uploadType=media&name=$DB/$DB.$TABLE.0.csv"

# Fill in the database
# Start importing the tables.
run_sql "DROP DATABASE IF EXISTS $DB;"
run_sql "DROP TABLE IF EXISTS $DB.$TABLE;"

SOURCE_DIR="gcs://$BUCKET/$DB?endpoint=http://$GCS_HOST:$GCS_PORT/storage/v1/"
run_lightning -d $SOURCE_DIR --backend local 2> /dev/null
run_sql "SELECT count(*), sum(i) FROM \`$DB\`.$TABLE"
check_contains "count(*): 7"
check_contains "sum(i): 413"
