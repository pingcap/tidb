# Copyright 2024 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import peewee
import tidb_vector
import tabulate
import h5py
import numpy
import time
import argparse

from tidb_vector.peewee import VectorField, VectorAdaptor
from tidb_vector.utils import encode_vector

dataset_path_1 = "./datasets/fashion-mnist-784-euclidean.hdf5"
dataset_path_2 = "./datasets/mnist-784-euclidean.hdf5"
table_name = "recall_test"

mysql_db = peewee.MySQLDatabase(
    "test",
    host="127.0.0.1",
    port=4000,
    user="root",
    passwd="",
)


class Sample(peewee.Model):
    class Meta:
        database = mysql_db
        db_table = table_name

    id = peewee.IntegerField(
        primary_key=True,
    )
    vec = VectorField(784)


def connect():
    print(
        f"+ Connecting to {mysql_db.connect_params['user']}@{mysql_db.connect_params['host']}...",
        flush=True,
    )
    mysql_db.connect()


def clean():
    mysql_db.drop_tables([Sample], safe=True)


def create_table():
    mysql_db.create_tables([Sample])
    VectorAdaptor(mysql_db).create_vector_index(
        Sample.vec, tidb_vector.DistanceMetric.L2
    )


def load(dataset_path: str, begin: int = 0, end: int = 60000):
    print()
    print("+ Loading data...", flush=True)

    with h5py.File(dataset_path, "r") as data_file:
        data: numpy.ndarray = data_file["train"][()]
        assert end <= len(data)

        data_with_id = [(idx, data[idx]) for idx in range(begin, end)]
        max_data_id = data_with_id[-1][0]

        for batch in peewee.chunked(data_with_id, 1000):
            print(
                f"  - Batch insert [{batch[0][0]}..{batch[-1][0]}] (max PK={max_data_id})...",
                flush=True,
            )
            Sample.insert_many(batch, fields=[Sample.id, Sample.vec]).execute()


def remove(begin: int, end: int):
    print()
    print(f"+ Removing data in range [{begin}..{end})...", flush=True)
    Sample.delete().where(Sample.id >= begin, Sample.id < end).execute()


def check(dataset_path: str, check_tiflash_used_index: bool):
    recall = 0.0

    print()
    print("+ Current index distribution:")
    cursor = mysql_db.execute_sql(
        f"SELECT ROWS_STABLE_INDEXED, ROWS_STABLE_NOT_INDEXED, ROWS_DELTA_INDEXED, ROWS_DELTA_NOT_INDEXED FROM INFORMATION_SCHEMA.TIFLASH_INDEXES WHERE TIDB_TABLE='{table_name}'"
    )
    print(
        tabulate.tabulate(
            cursor.fetchall(),
            headers=[
                "StableIndexed",
                "StableNotIndexed",
                "DeltaIndexed",
                "DeltaNotIndexed",
            ],
            tablefmt="psql",
        ),
        flush=True,
    )

    with h5py.File(dataset_path, "r") as data_file:
        query_rows = data_file["test"][()]
        query_rows_len = min(
            len(query_rows), 200
        )  # Just check with first 200 test rows

        print("+ Execution Plan:")

        with mysql_db.execute_sql(
            # EXPLAIN ANALYZE SELECT * FROM {table_name} ORDER BY VEC_L2_Distance(vec, %s) LIMIT 100
            # In the cluster started by tiup, the tiflash component does not yet include pr-10103, so '*' is used here as a substitute.
            # For details, see: https://github.com/pingcap/tiflash/pull/10103
            f"EXPLAIN ANALYZE SELECT * FROM {table_name} ORDER BY VEC_L2_Distance(vec, %s) LIMIT 100",
            (encode_vector(query_rows[0]),),
        ) as cursor:
            plan = tabulate.tabulate(cursor.fetchall(), tablefmt="psql")
            print(plan, flush=True)
            assert "mpp[tiflash]" in plan
            assert "annIndex:L2(vec.." in plan
            if check_tiflash_used_index:
                assert "vector_idx:{" in plan

        print()
        print(f"+ Checking recall (via {query_rows_len} groundtruths)...", flush=True)

        total_recall = 0.0
        total_tests = 0

        for test_rowid in range(query_rows_len):
            query_row: numpy.ndarray = query_rows[test_rowid]
            groundtruth_results_set = set(data_file["neighbors"][test_rowid])

            with mysql_db.execute_sql(
                # SELECT id FROM {table_name} ORDER BY VEC_L2_Distance(vec, %s) LIMIT 100
                # In the cluster started by tiup, the tiflash component does not yet include pr-10103, so '*' is used here as a substitute.
                # For details, see: https://github.com/pingcap/tiflash/pull/10103
                f"SELECT * FROM {table_name} ORDER BY VEC_L2_Distance(vec, %s) LIMIT 100",
                (encode_vector(query_row),),
            ) as cursor:
                actual_results = cursor.fetchall()
                actual_results_set = set([int(row[0]) for row in actual_results])
                recall = (
                    len(groundtruth_results_set & actual_results_set)
                    / len(groundtruth_results_set)
                    * 100
                )
                total_recall += recall
                total_tests += 1

                if recall < 80:
                    print(
                        f"  - WARNING: groundtruth #{test_rowid} recall {recall:.2f}%",
                        flush=True,
                    )

        avg_recall = total_recall / total_tests
        print(f"  - Average recall: {recall:.2f}%", flush=True)

        # For this dataset, our recall is very high, so we set a very high standard here
        assert avg_recall >= 95


def compact_and_wait_index_built():
    print()
    print("+ Wait data synchronize...", flush=True)
    cursor = mysql_db.execute_sql(f"SELECT COUNT(*) FROM {table_name}")
    print(f"  - Current row count: {cursor.fetchone()[0]}", flush=True)
    print("+ Compact table...", flush=True)
    mysql_db.execute_sql(f"ALTER TABLE {table_name} COMPACT")
    print("+ Waiting index build finish...", flush=True)

    start_time = time.time()
    while True:
        cursor = mysql_db.execute_sql(
            f"SELECT ROWS_STABLE_NOT_INDEXED, ROWS_STABLE_INDEXED FROM INFORMATION_SCHEMA.TIFLASH_INDEXES WHERE TIDB_TABLE='{table_name}'"
        )
        row = cursor.fetchone()
        if row is None:
            time.sleep(10)
            continue

        if row[0] == 0:
            break

        print(f"  - StableIndexed: {row[1]}, StableNotIndexed: {row[0]}", flush=True)
        time.sleep(10)

        if time.time() - start_time > 600:
            raise Exception("Index build not finished in 10 minutes")


def main():
    parser = argparse.ArgumentParser(prog="vector_recall")
    parser.add_argument(
        "--check-only",
        help="Only do the check without loading data",
        action="store_true",
    )
    args = parser.parse_args()

    connect()

    if args.check_only:
        print("+ Perform check over existing data...", flush=True)
        check(dataset_path_2, check_tiflash_used_index=True)
        return

    clean()
    create_table()

    print("+ Insert data and ensure there are both stable and delta...", flush=True)
    # First insert a part of data, and makes it become the stable layer
    load(dataset_path_1, 0, 30000)
    compact_and_wait_index_built()
    # Then insert the rest of data, becomes the delta layer
    load(dataset_path_1, 30000, 60000)
    # Now we check the recall when we hybrid some stable and data
    check(dataset_path_1, check_tiflash_used_index=True)
    print("+ Wait 10s so that some delta index may be built...", flush=True)
    time.sleep(10)
    check(dataset_path_1, check_tiflash_used_index=True)

    # Try to remove some data, and insert data again to check multi-version recall
    print("+ Reinsert multi-version data...", flush=True)
    remove(22400, 41234) # This covers both delta and stable
    load(dataset_path_1, 22400, 41234)
    check(dataset_path_1, check_tiflash_used_index=True)

    # Remove all data, insert dataset 2
    print("+ Reinsert multi-version data using dataset 2...", flush=True)
    remove(0, 60000)
    load(dataset_path_2, 0, 20000)
    compact_and_wait_index_built()
    load(dataset_path_2, 20000, 60000)
    check(dataset_path_2, check_tiflash_used_index=True)
    print("+ Wait 10s so that some delta index may be built...", flush=True)
    time.sleep(10)
    check(dataset_path_2, check_tiflash_used_index=True)

    # Compact all data, and check again, this checks the recall for stable only
    compact_and_wait_index_built()
    check(dataset_path_2, check_tiflash_used_index=True)


if __name__ == "__main__":
    main()