#!/bin/sh
#
# Copyright 2019 PingCAP, Inc.
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

# This test verifies if TOOL-1405 is fixed.
# Lightning should fail to start when finds unused config keys.

set +e
run_lightning --log-file "$TEST_DIR/lightning-unused-config-keys.log"
ERRORCODE=$?
set -e

[ "$ERRORCODE" -ne 0 ]

grep -q 'typo-1' "$TEST_DIR/lightning-unused-config-keys.log" &&
grep -q 'typo-2' "$TEST_DIR/lightning-unused-config-keys.log" &&
grep -q 'typo-3' "$TEST_DIR/lightning-unused-config-keys.log" &&
! grep -q 'typo-4' "$TEST_DIR/lightning-unused-config-keys.log"
