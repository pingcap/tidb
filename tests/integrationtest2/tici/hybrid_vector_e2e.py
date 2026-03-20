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
    parser.add_argument("command", choices=["prepare", "verify", "recall", "bench"])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4000)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="test")
    parser.add_argument("--table", default="hybrid_vector_docs")
    parser.add_argument("--bf-table", default="hybrid_vector_docs_bf")
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
    parser.add_argument("--bench-queries", type=int, default=100, help="number of queries for bench")
    parser.add_argument("--bench-k", type=int, default=10, help="top-K for bench queries")
    parser.add_argument("--batch-size", type=int, default=0, help="INSERT batch size (0=auto)")
    parser.add_argument("--hnsw-m", type=int, default=16, help="HNSW M (connectivity)")
    parser.add_argument("--hnsw-ef-construction", type=int, default=200, help="HNSW ef_construction")
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


def random_vector(rng: random.Random, dim: int = VECTOR_DIM) -> List[float]:
    return [round(rng.uniform(0.2, 0.8), 6) for _ in range(dim)]


def anchor_vector(seed: int, index: int, dim: int = VECTOR_DIM) -> List[float]:
    """Deterministic in-distribution random vector for anchor rows."""
    return random_vector(random.Random(seed * 1000 + index), dim)


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
    DROP TABLE IF EXISTS `{args.database}`.`{args.bf_table}`;
    CREATE TABLE `{args.database}`.`{args.bf_table}` (
      id BIGINT PRIMARY KEY,
      embedding VECTOR({VECTOR_DIM})
    );
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


def build_base_rows(row_count: int, rng: random.Random, seed: int) -> List[Tuple[object, ...]]:
    if row_count < 4:
        raise ValueError("rows must be >= 4")

    rows: List[Tuple[object, ...]] = [
        (
            BASE_ANCHOR_ID,
            BASE_ANCHOR_TITLE,
            "anchor_payload_hot",
            vector_literal(anchor_vector(seed, 0)),
        ),
        (
            2,
            "anchor_title_other",
            "anchor_payload_other",
            vector_literal(anchor_vector(seed, 1)),
        ),
        (
            3,
            "anchor_title_extra",
            "anchor_payload_extra",
            vector_literal(anchor_vector(seed, 2)),
        ),
        (
            4,
            "anchor_title_filter",
            "anchor_payload_filter",
            vector_literal(anchor_vector(seed, 3)),
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


def insert_bf_rows(
    args: argparse.Namespace, rows: Sequence[Tuple[object, ...]], batch_size: int = 500
) -> None:
    """Insert id + embedding into the brute-force table (no index)."""
    for batch in batched(rows, batch_size):
        values = ",\n".join(
            f"({row_id}, {sql_literal(embedding)})"
            for row_id, _title_kw, _payload, embedding in batch
        )
        sql = f"""
        INSERT INTO `{args.database}`.`{args.bf_table}` (id, embedding)
        VALUES {values};
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
          "distance_metric": "L2",
          "hnsw_m": {args.hnsw_m},
          "hnsw_ef_construction": {args.hnsw_ef_construction}
        }}
      }}],
      "sharding_key": {{
        "columns": ["title_kw"]
      }}
    }}';
    """
    run_sql(args, sql)


def insert_delta_row(args: argparse.Namespace) -> None:
    delta_vec = anchor_vector(args.seed, 4)
    sql = f"""
    INSERT INTO `{args.database}`.`{args.table}`
      (id, title_kw, payload, embedding)
    VALUES
      (
        {DELTA_ANCHOR_ID},
        {sql_literal(DELTA_ANCHOR_TITLE)},
        {sql_literal("delta_payload_hot")},
        {sql_literal(vector_literal(delta_vec))}
      )
    ON DUPLICATE KEY UPDATE
      title_kw = VALUES(title_kw),
      payload = VALUES(payload),
      embedding = VALUES(embedding);
    """
    run_sql(args, sql)


def auto_batch_size(args: argparse.Namespace) -> int:
    if args.batch_size > 0:
        return args.batch_size
    if args.rows >= 100_000:
        return 5000
    return 500


def insert_delta_bf_row(args: argparse.Namespace) -> None:
    delta_vec = anchor_vector(args.seed, 4)
    sql = f"""
    INSERT INTO `{args.database}`.`{args.bf_table}` (id, embedding)
    VALUES ({DELTA_ANCHOR_ID}, {sql_literal(vector_literal(delta_vec))})
    ON DUPLICATE KEY UPDATE embedding = VALUES(embedding);
    """
    run_sql(args, sql)


def prepare(args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)
    batch_size = auto_batch_size(args)

    t0 = time.time()
    prepare_schema(args)
    wait_for_tiflash_replica(args)
    t_schema = time.time() - t0

    t0 = time.time()
    create_hybrid_index(args)
    t_index = time.time() - t0

    rows = build_base_rows(args.rows, rng, args.seed)
    t0 = time.time()
    insert_rows(args, rows, batch_size=batch_size)
    insert_bf_rows(args, rows, batch_size=batch_size)
    t_insert = time.time() - t0

    t0 = time.time()
    insert_delta_row(args)
    insert_delta_bf_row(args)
    t_delta = time.time() - t0

    print(f"\nprepare summary ({args.rows} rows, batch_size={batch_size}):")
    print(f"  schema + tiflash replica: {t_schema:.1f}s")
    print(f"  create hybrid index:      {t_index:.1f}s")
    print(f"  insert {args.rows} rows:       {t_insert:.1f}s ({args.rows / max(t_insert, 0.001):.0f} rows/s)")
    print(f"  insert delta row:         {t_delta:.1f}s")
    print(f"  total:                    {t_schema + t_index + t_insert + t_delta:.1f}s")


def vector_query_sql(args: argparse.Namespace, anchor_idx: int, limit: int = 3) -> str:
    vec = anchor_vector(args.seed, anchor_idx)
    return (
        f"SELECT id FROM `{args.database}`.`{args.table}` "
        f"ORDER BY vec_l2_distance(embedding, {sql_literal(vector_literal(vec))}) "
        f"LIMIT {limit};"
    )


def vector_explain_sql(args: argparse.Namespace, anchor_idx: int, limit: int = 3) -> str:
    vec = anchor_vector(args.seed, anchor_idx)
    return (
        f"EXPLAIN FORMAT='brief' SELECT id FROM `{args.database}`.`{args.table}` "
        f"ORDER BY vec_l2_distance(embedding, {sql_literal(vector_literal(vec))}) "
        f"LIMIT {limit};"
    )


def inverted_query_sql(args: argparse.Namespace, title: str) -> str:
    return (
        f"SELECT id FROM `{args.database}`.`{args.table}` USE INDEX (`{args.index_name}`) "
        f"WHERE title_kw = {sql_literal(title)} ORDER BY id;"
    )


def explain_contains_all(args: argparse.Namespace, sql: str, needles: Sequence[str]) -> bool:
    rows = query_rows(args, sql)
    joined = "\n".join("\t".join(row) for row in rows)
    return all(needle in joined for needle in needles)


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
        lambda: fetch_ids(args, vector_query_sql(args, 4, 1)) == [DELTA_ANCHOR_ID],
    )
    wait_until(
        "vector explain path",
        args,
        lambda: explain_contains_all(
            args, vector_explain_sql(args, 0, 3), ["vector search", args.index_name]
        ),
    )


def brute_force_topk_sql(
    args: argparse.Namespace, query_vec: List[float], k: int
) -> str:
    """Brute-force top-K via table scan on the no-index bf table."""
    return (
        f"SELECT id FROM `{args.database}`.`{args.bf_table}` "
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


def percentile(sorted_values: List[float], p: float) -> float:
    idx = int(len(sorted_values) * p)
    return sorted_values[min(idx, len(sorted_values) - 1)]


def bench(args: argparse.Namespace) -> None:
    k = args.bench_k
    num_queries = args.bench_queries
    rng = random.Random(args.seed + 2000)

    query_vectors = [random_vector(rng) for _ in range(num_queries)]

    print(f"bench: {num_queries} queries, top-{k}, {args.rows} rows, dim={VECTOR_DIM}")

    # --- vector search via TiCI index ---
    print("\nwarming up vector search (3 queries)...")
    for qvec in query_vectors[:3]:
        fetch_ids(args, tici_topk_sql(args, qvec, k))

    print(f"running {num_queries} vector search queries...")
    vec_latencies: List[float] = []
    for qi, qvec in enumerate(query_vectors):
        t0 = time.time()
        fetch_ids(args, tici_topk_sql(args, qvec, k))
        vec_latencies.append(time.time() - t0)
        if (qi + 1) % 20 == 0:
            print(f"  {qi + 1}/{num_queries}")

    vec_latencies.sort()
    vec_total = sum(vec_latencies)
    print(f"\nvector search top-{k} latency ({num_queries} queries):")
    print(f"  avg:  {vec_total / num_queries * 1000:.1f} ms")
    print(f"  p50:  {percentile(vec_latencies, 0.50) * 1000:.1f} ms")
    print(f"  p90:  {percentile(vec_latencies, 0.90) * 1000:.1f} ms")
    print(f"  p99:  {percentile(vec_latencies, 0.99) * 1000:.1f} ms")
    print(f"  min:  {vec_latencies[0] * 1000:.1f} ms")
    print(f"  max:  {vec_latencies[-1] * 1000:.1f} ms")
    print(f"  qps:  {num_queries / vec_total:.1f}")

    # --- brute-force (TiKV table scan) for comparison ---
    print(f"\nrunning {num_queries} brute-force queries (USE INDEX()) for comparison...")
    bf_latencies: List[float] = []
    for qi, qvec in enumerate(query_vectors):
        t0 = time.time()
        fetch_ids(args, brute_force_topk_sql(args, qvec, k))
        bf_latencies.append(time.time() - t0)
        if (qi + 1) % 20 == 0:
            print(f"  {qi + 1}/{num_queries}")

    bf_latencies.sort()
    bf_total = sum(bf_latencies)
    print(f"\nbrute-force top-{k} latency ({num_queries} queries):")
    print(f"  avg:  {bf_total / num_queries * 1000:.1f} ms")
    print(f"  p50:  {percentile(bf_latencies, 0.50) * 1000:.1f} ms")
    print(f"  p90:  {percentile(bf_latencies, 0.90) * 1000:.1f} ms")
    print(f"  p99:  {percentile(bf_latencies, 0.99) * 1000:.1f} ms")
    print(f"  min:  {bf_latencies[0] * 1000:.1f} ms")
    print(f"  max:  {bf_latencies[-1] * 1000:.1f} ms")
    print(f"  qps:  {num_queries / bf_total:.1f}")

    speedup = (bf_total / num_queries) / (vec_total / num_queries)
    print(f"\nspeedup: {speedup:.1f}x (vector index vs brute-force)")

    # --- inverted search ---
    print(f"\nrunning {num_queries} inverted index queries...")
    inv_titles = [f"rand_title_{rng.randint(5, args.rows):05d}" for _ in range(num_queries)]
    inv_latencies: List[float] = []
    for title in inv_titles:
        t0 = time.time()
        fetch_ids(args, inverted_query_sql(args, title))
        inv_latencies.append(time.time() - t0)

    inv_latencies.sort()
    inv_total = sum(inv_latencies)
    print(f"\ninverted search latency ({num_queries} queries):")
    print(f"  avg:  {inv_total / num_queries * 1000:.1f} ms")
    print(f"  p50:  {percentile(inv_latencies, 0.50) * 1000:.1f} ms")
    print(f"  p90:  {percentile(inv_latencies, 0.90) * 1000:.1f} ms")
    print(f"  p99:  {percentile(inv_latencies, 0.99) * 1000:.1f} ms")
    print(f"  qps:  {num_queries / inv_total:.1f}")


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
    if args.command == "bench":
        bench(args)
        return
    raise ValueError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    main()
