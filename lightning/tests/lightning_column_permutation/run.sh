#
# Copyright 2022 PingCAP, Inc.
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

set -eu

for BACKEND in local tidb; do
    if [ "$BACKEND" = 'local' ]; then
        check_cluster_version 4 0 0 'local backend' || continue
    fi
    run_sql 'DROP DATABASE IF EXISTS perm'

    run_lightning --backend $BACKEND

    run_sql 'select count(*) from perm.test_perm;'
    check_contains "count(*): 5"

    run_sql "SELECT fund_seq_no, region_code, credit_code FROM perm.test_perm WHERE contract_no = '2020061000019011020164030597';"
    check_contains "fund_seq_no: 202006100001901102016403059520200627"
    check_contains "region_code: 000002"
    check_contains "credit_code: 33"
done
