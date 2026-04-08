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

# This test is used to generate backup for later compatible test.
set -eux

BUCKET="test"
# start the s3 server
MINIO_ACCESS_KEY='brs3accesskey'
MINIO_SECRET_KEY='brs3secretkey'
S3_ENDPOINT=minio:24927
S3_KEY="&access-key=$MINIO_ACCESS_KEY&secret-access-key=$MINIO_SECRET_KEY"

# create bucket
/usr/bin/mc config host add minio http://$S3_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY
/usr/bin/mc mb minio/test --ignore-existing

# backup cluster data
run_sql_in_container "backup database test to 's3://$BUCKET/bk${TAG}?endpoint=http://$S3_ENDPOINT$S3_KEY&force-path-style=true';"
