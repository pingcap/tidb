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

. br/compatibility/get_last_tags.sh

TAGS="v5.0.0"
getLatestTags
echo "recent version of cluster is $TAGS"

i=0

runBackup() {
  # generate backup data in /tmp/br/docker/backup_data/$TAG/, we can do restore after all data backuped later.
  echo "build $1 cluster"
  TAG=$1 PORT_SUFFIX=$2 docker-compose -p $1 -f br/compatibility/backup_cluster.yaml build
  TAG=$1 PORT_SUFFIX=$2 docker-compose -p $1 -f br/compatibility/backup_cluster.yaml up -d
  trap "TAG=$1 PORT_SUFFIX=$2 docker-compose -p $1 -f br/compatibility/backup_cluster.yaml down" EXIT
  # wait for cluster ready
  sleep 20
  # prepare SQL data
  TAG=$1 PORT_SUFFIX=$2 docker-compose -p $1 -f br/compatibility/backup_cluster.yaml exec -T control /go/bin/go-ycsb load mysql -P /prepare_data/workload -p mysql.host=tidb -p mysql.port=4000 -p mysql.user=root -p mysql.db=test
  TAG=$1 PORT_SUFFIX=$2 docker-compose -p $1 -f br/compatibility/backup_cluster.yaml exec -T control br/tests/run_compatible.sh prepare
}

for tag in $TAGS; do
   i=$(( i + 1 ))
   runBackup $tag $i &
done

wait

echo "prepare backup data successfully"
