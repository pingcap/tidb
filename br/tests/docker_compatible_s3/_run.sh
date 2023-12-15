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

# This test is used to test compatible for BR restore.
set -eux

BUCKET="test"
MINIO_ACCESS_KEY='brs3accesskey'
MINIO_SECRET_KEY='brs3secretkey'
S3_ENDPOINT=minio:24927
S3_KEY="&access-key=$MINIO_ACCESS_KEY&secret-access-key=$MINIO_SECRET_KEY"

# restore backup data one by one
for TAG in ${TAGS}; do
    echo "restore ${TAG} data starts..."
    # after BR merged into TiDB we need skip version check because the build from tidb is not a release version.
    bin/br restore db --db test -s "s3://$BUCKET/bk${TAG}?endpoint=http://$S3_ENDPOINT$S3_KEY" --pd $PD_ADDR --check-requirements=false
    row_count=$(run_sql_in_container  "SELECT COUNT(*) FROM test.usertable;" | awk '/COUNT/{print $2}')
    if [ $row_count != $EXPECTED_KVS ]; then
       echo "restore kv count is not as expected(1000), obtain $row_count"
       exit 1
    fi
    # clean up data for next restoration
    run_sql_in_container "drop database test;"
done
