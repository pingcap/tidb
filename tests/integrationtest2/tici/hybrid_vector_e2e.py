#!/usr/bin/env python3
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

from __future__ import annotations

import argparse
import random
import subprocess
import time
from typing import Iterable, List, Sequence, Tuple


VECTOR_DIM = 32
BASE_ANCHOR_ID = 1
DELTA_ANCHOR_ID = 10001
BASE_ANCHOR_TITLE = "anchor_title_keep"
DELTA_ANCHOR_TITLE = "post_index_anchor"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["prepare", "verify", "recall"])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4000)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="test")
    parser.add_argument("--table", default="hybrid_vector_docs")
    parser.add_argument("--index-name", default="idx_hybrid_vector")
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--interval", type=int, default=5)
    parser.add_argument("--recall-queries", type=int, default=20, help="number of random query vectors for recall test")
    parser.add_argument(
        "--recall-k",
        type=str,
        default="1,10,50",
        help="comma-separated K values for recall@K",
    )
    return parser.parse_args()


def mysql_base_command(args: argparse.Namespace) -> List[str]:
    command = [
        "mysql",
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--user",
        args.user,
        "--batch",
        "--raw",
        "--skip-column-names",
    ]
    if args.password:
        command.append(f"--password={args.password}")
    return command


def run_sql(args: argparse.Namespace, sql: str) -> str:
    process = subprocess.run(
        mysql_base_command(args),
        input=sql,
        text=True,
        capture_output=True,
        check=False,
    )
    if process.returncode != 0:
        raise RuntimeError(
            f"mysql failed with exit code {process.returncode}: {process.stderr.strip()}"
        )
    return process.stdout


def query_rows(args: argparse.Namespace, sql: str) -> List[List[str]]:
    output = run_sql(args, sql)
    if not output.strip():
        return []
    return [line.split("\t") for line in output.strip().splitlines()]


def sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def vector_literal(values: Sequence[float]) -> str:
    return "[" + ", ".join(f"{value:.6f}" for value in values) + "]"


def one_hot(index: int, dim: int = VECTOR_DIM) -> List[float]:
    values = [0.0] * dim
    values[index] = 1.0
    return values


def random_vector(rng: random.Random, dim: int = VECTOR_DIM) -> List[float]:
    return [round(rng.uniform(0.2, 0.8), 6) for _ in range(dim)]


def batched(items: Sequence[Tuple[object, ...]], batch_size: int) -> Iterable[Sequence[Tuple[object, ...]]]:
    for start in range(0, len(items), batch_size):
        yield items[start : start + batch_size]


def prepare_schema(args: argparse.Namespace) -> None:
    sql = f"""
    CREATE DATABASE IF NOT EXISTS `{args.database}`;
    DROP TABLE IF EXISTS `{args.database}`.`{args.table}`;
    CREATE TABLE `{args.database}`.`{args.table}` (
      id BIGINT PRIMARY KEY,
      title_kw VARCHAR(64),
      payload TEXT,
      embedding VECTOR({VECTOR_DIM})
    );
    ALTER TABLE `{args.database}`.`{args.table}` SET TIFLASH REPLICA 1;
    """
    run_sql(args, sql)


def wait_for_tiflash_replica(args: argparse.Namespace) -> None:
    sql = f"""
    SELECT AVAILABLE, PROGRESS
    FROM information_schema.tiflash_replica
    WHERE TABLE_SCHEMA = {sql_literal(args.database)}
      AND TABLE_NAME = {sql_literal(args.table)};
    """
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        try:
            rows = query_rows(args, sql)
        except RuntimeError:
            rows = []
        if rows and rows[0][0] == "1":
            return
        time.sleep(args.interval)
    raise RuntimeError("TiFlash replica did not become available in time")


def build_base_rows(row_count: int, rng: random.Random) -> List[Tuple[object, ...]]:
    if row_count < 4:
        raise ValueError("rows must be >= 4")

    rows: List[Tuple[object, ...]] = [
        (
            BASE_ANCHOR_ID,
            BASE_ANCHOR_TITLE,
            "anchor_payload_hot",
            vector_literal(one_hot(0)),
        ),
        (
            2,
            "anchor_title_other",
            "anchor_payload_other",
            vector_literal(one_hot(2)),
        ),
        (
            3,
            "anchor_title_extra",
            "anchor_payload_extra",
            vector_literal(one_hot(4)),
        ),
        (
            4,
            "anchor_title_filter",
            "anchor_payload_filter",
            vector_literal(one_hot(6)),
        ),
    ]

    for row_id in range(5, row_count + 1):
        rows.append(
            (
                row_id,
                f"rand_title_{row_id:05d}",
                f"rand_payload_{row_id:05d}",
                vector_literal(random_vector(rng)),
            )
        )
    return rows


def insert_rows(
    args: argparse.Namespace, rows: Sequence[Tuple[object, ...]], batch_size: int = 500
) -> None:
    for batch in batched(rows, batch_size):
        values = ",\n".join(
            f"({row_id}, {sql_literal(title_kw)}, {sql_literal(payload)}, {sql_literal(embedding)})"
            for row_id, title_kw, payload, embedding in batch
        )
        sql = f"""
        INSERT INTO `{args.database}`.`{args.table}`
          (id, title_kw, payload, embedding)
        VALUES
        {values};
        """
        run_sql(args, sql)


def create_hybrid_index(args: argparse.Namespace) -> None:
    sql = f"""
    CREATE HYBRID INDEX `{args.index_name}`
    ON `{args.database}`.`{args.table}`(title_kw, embedding)
    PARAMETER '{{
      "inverted": {{
        "columns": ["title_kw"]
      }},
      "vector": [{{
        "columns": ["embedding"],
        "index_info": {{
          "distance_metric": "L2"
        }}
      }}],
      "sharding_key": {{
        "columns": ["title_kw"]
      }}
    }}';
    """
    run_sql(args, sql)


def insert_delta_row(args: argparse.Namespace) -> None:
    sql = f"""
    INSERT INTO `{args.database}`.`{args.table}`
      (id, title_kw, payload, embedding)
    VALUES
      (
        {DELTA_ANCHOR_ID},
        {sql_literal(DELTA_ANCHOR_TITLE)},
        {sql_literal("delta_payload_hot")},
        {sql_literal(vector_literal(one_hot(31)))}
      )
    ON DUPLICATE KEY UPDATE
      title_kw = VALUES(title_kw),
      payload = VALUES(payload),
      embedding = VALUES(embedding);
    """
    run_sql(args, sql)


def prepare(args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)
    prepare_schema(args)
    wait_for_tiflash_replica(args)
    create_hybrid_index(args)
    insert_rows(args, build_base_rows(args.rows, rng))
    insert_delta_row(args)


def vector_query_sql(args: argparse.Namespace, vector_index: int, limit: int = 3) -> str:
    return (
        f"SELECT id FROM `{args.database}`.`{args.table}` "
        f"ORDER BY vec_l2_distance(embedding, {sql_literal(vector_literal(one_hot(vector_index)))}) "
        f"LIMIT {limit};"
    )


def vector_explain_sql(args: argparse.Namespace, vector_index: int, limit: int = 3) -> str:
    return (
        f"EXPLAIN FORMAT='brief' SELECT id FROM `{args.database}`.`{args.table}` "
        f"ORDER BY vec_l2_distance(embedding, {sql_literal(vector_literal(one_hot(vector_index)))}) "
        f"LIMIT {limit};"
    )


def inverted_query_sql(args: argparse.Namespace, title: str) -> str:
    return (
        f"SELECT id FROM `{args.database}`.`{args.table}` USE INDEX (`{args.index_name}`) "
        f"WHERE title_kw = {sql_literal(title)} ORDER BY id;"
    )


def explain_contains(args: argparse.Namespace, sql: str, needle: str) -> bool:
    rows = query_rows(args, sql)
    joined = "\n".join("\t".join(row) for row in rows)
    return needle in joined


def fetch_ids(args: argparse.Namespace, sql: str) -> List[int]:
    rows = query_rows(args, sql)
    return [int(row[0]) for row in rows]


def wait_until(description: str, args: argparse.Namespace, fn) -> None:
    deadline = time.time() + args.timeout
    last_error = "no attempts made"
    while time.time() < deadline:
        try:
            if fn():
                return
            last_error = "condition returned false"
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
        time.sleep(args.interval)
    raise RuntimeError(f"timed out waiting for {description}: {last_error}")


def verify(args: argparse.Namespace) -> None:
    wait_until(
        "base inverted anchor",
        args,
        lambda: fetch_ids(args, inverted_query_sql(args, BASE_ANCHOR_TITLE))
        == [BASE_ANCHOR_ID],
    )
    wait_until(
        "delta inverted anchor",
        args,
        lambda: fetch_ids(args, inverted_query_sql(args, DELTA_ANCHOR_TITLE))
        == [DELTA_ANCHOR_ID],
    )
    wait_until(
        "base vector top1",
        args,
        lambda: fetch_ids(args, vector_query_sql(args, 0, 1)) == [BASE_ANCHOR_ID],
    )
    wait_until(
        "delta vector top1",
        args,
        lambda: fetch_ids(args, vector_query_sql(args, 31, 1)) == [DELTA_ANCHOR_ID],
    )
    wait_until(
        "vector explain path",
        args,
        lambda: explain_contains(args, vector_explain_sql(args, 0, 3), "vector search")
        and explain_contains(args, vector_explain_sql(args, 0, 3), args.index_name),
    )


def brute_force_topk_sql(
    args: argparse.Namespace, query_vec: List[float], k: int
) -> str:
    """Brute-force top-K via TiKV table scan (USE INDEX() bypasses all indexes)."""
    return (
        f"SELECT id FROM `{args.database}`.`{args.table}` USE INDEX() "
        f"ORDER BY vec_l2_distance(embedding, {sql_literal(vector_literal(query_vec))}) "
        f"LIMIT {k};"
    )


def tici_topk_sql(
    args: argparse.Namespace, query_vec: List[float], k: int
) -> str:
    """Top-K via TiCI hybrid vector index."""
    return (
        f"SELECT id FROM `{args.database}`.`{args.table}` "
        f"ORDER BY vec_l2_distance(embedding, {sql_literal(vector_literal(query_vec))}) "
        f"LIMIT {k};"
    )


def compute_recall(ground_truth: List[int], predicted: List[int], k: int) -> float:
    gt_set = set(ground_truth[:k])
    pred_set = set(predicted[:k])
    if not gt_set:
        return 1.0
    return len(gt_set & pred_set) / len(gt_set)


def recall(args: argparse.Namespace) -> None:
    k_values = [int(x.strip()) for x in args.recall_k.split(",")]
    max_k = max(k_values)
    num_queries = args.recall_queries
    rng = random.Random(args.seed + 1000)  # different seed from data generation

    print(f"recall test: {num_queries} queries, K={k_values}, dim={VECTOR_DIM}")

    query_vectors = [random_vector(rng) for _ in range(num_queries)]

    results: dict[int, List[float]] = {k: [] for k in k_values}

    for qi, qvec in enumerate(query_vectors):
        gt_ids = fetch_ids(args, brute_force_topk_sql(args, qvec, max_k))
        tici_ids = fetch_ids(args, tici_topk_sql(args, qvec, max_k))

        for k in k_values:
            r = compute_recall(gt_ids, tici_ids, k)
            results[k].append(r)

        if (qi + 1) % 5 == 0 or qi == 0:
            print(f"  query {qi + 1}/{num_queries} done")

    print()
    all_pass = True
    for k in k_values:
        scores = results[k]
        avg = sum(scores) / len(scores) if scores else 0.0
        min_r = min(scores) if scores else 0.0
        perfect = sum(1 for s in scores if s >= 1.0)
        threshold = 0.80 if k <= 1 else 0.90
        status = "PASS" if avg >= threshold else "FAIL"
        if status == "FAIL":
            all_pass = False
        print(
            f"  recall@{k:>3d}: avg={avg:.4f}  min={min_r:.4f}  "
            f"perfect={perfect}/{len(scores)}  [{status}] (threshold={threshold:.2f})"
        )

    print()
    if not all_pass:
        raise RuntimeError("recall test FAILED: one or more K values below threshold")
    print("recall test PASSED")


def main() -> None:
    args = parse_args()
    if args.command == "prepare":
        prepare(args)
        return
    if args.command == "verify":
        verify(args)
        return
    if args.command == "recall":
        recall(args)
        return
    raise ValueError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    main()
