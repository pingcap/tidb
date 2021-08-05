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
# See the License for the specific language governing permissions and
# limitations under the License.

# This test is used to generate backup for later compatible test.
set -eux

BUCKET="test"

# create gcs bucket
curl -XPOST http://$GCS_HOST:$GCS_PORT/storage/v1/b -d '{"name":"test"}'

# backup cluster data
run_sql_in_container "backup database test to 'gcs://$BUCKET/bk${TAG}?endpoint=http://$GCS_HOST:$GCS_PORT/storage/v1/';"
