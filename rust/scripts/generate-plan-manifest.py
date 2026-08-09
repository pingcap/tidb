#!/usr/bin/env python3
"""Generate the pinned TPCC/Sysbench physical-plan case manifest."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = SCRIPT_DIR / "tpcc-sysbench-plan-manifest.json"


def parameter(type_name: str, value: Any) -> dict[str, Any]:
    return {"type": type_name, "value": value}


def plan_case(
    identity: str,
    suite: str,
    phase: str,
    source: str,
    sql: str,
    params: list[dict[str, Any]] | None = None,
    *,
    protocol: str = "prepared",
    workloads: list[str] | None = None,
) -> dict[str, Any]:
    case: dict[str, Any] = {
        "id": identity,
        "suite": suite,
        "phase": phase,
        "kind": "plan",
        "protocol": protocol,
        "source": source,
        "sql": sql,
    }
    if params is not None:
        case["params"] = params
    if workloads is not None:
        case["workloads"] = workloads
    return case


def non_plan_case(
    identity: str,
    suite: str,
    phase: str,
    source: str,
    sql: str,
    workloads: list[str] | None = None,
    *,
    protocol: str = "direct",
) -> dict[str, Any]:
    case: dict[str, Any] = {
        "id": identity,
        "suite": suite,
        "phase": phase,
        "kind": "non_plan",
        "protocol": protocol,
        "source": source,
        "sql": sql,
        "compatibility": "pending",
    }
    if workloads is not None:
        case["workloads"] = workloads
    return case


def multi_row_insert_family(
    identity: str,
    suite: str,
    source: str,
    sql_prefix: str,
    row_template: str,
    row_counts: list[int] | dict[str, int],
    *,
    phase: str = "prepare",
    separator: str = ", ",
    workloads: list[str] | None = None,
    proof: str,
) -> dict[str, Any]:
    family: dict[str, Any] = {
        "id": identity,
        "suite": suite,
        "phase": phase,
        "kind": "plan",
        "protocol": "direct",
        "source": source,
        "generator": "multi_row_insert",
        "sql_prefix": sql_prefix,
        "row_template": row_template,
        "row_counts": row_counts,
        "separator": separator,
        "coverage_proof": proof,
    }
    if workloads is not None:
        family["workloads"] = workloads
    return family


def ints(count: int, value: int = 1, type_name: str = "i64") -> list[dict[str, Any]]:
    return [parameter(type_name, value) for _ in range(count)]


def tpcc_run_cases() -> list[dict[str, Any]]:
    cases = [
        plan_case(
            "tpcc.run.new_order.select_customer",
            "tpcc",
            "run",
            "tpcc/new_order.go:12",
            "SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?",
            ints(3),
        ),
        plan_case(
            "tpcc.run.new_order.select_district",
            "tpcc",
            "run",
            "tpcc/new_order.go:13",
            "SELECT d_next_o_id, d_tax FROM district WHERE d_id = ? AND d_w_id = ? FOR UPDATE",
            ints(2),
        ),
        plan_case(
            "tpcc.run.new_order.update_district",
            "tpcc",
            "run",
            "tpcc/new_order.go:14",
            "UPDATE district SET d_next_o_id = ? + 1 WHERE d_id = ? AND d_w_id = ?",
            ints(3),
        ),
        plan_case(
            "tpcc.run.new_order.insert_order",
            "tpcc",
            "run",
            "tpcc/new_order.go:15",
            "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ints(4)
            + [parameter("string", "2026-08-09 00:00:00")]
            + ints(2),
        ),
        plan_case(
            "tpcc.run.new_order.insert_new_order",
            "tpcc",
            "run",
            "tpcc/new_order.go:16",
            "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)",
            ints(3),
        ),
        plan_case(
            "tpcc.run.new_order.update_stock",
            "tpcc",
            "run",
            "tpcc/new_order.go:17",
            "UPDATE stock SET s_quantity = ?, s_ytd = s_ytd + ?, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + ? WHERE s_i_id = ? AND s_w_id = ?",
            ints(5),
        ),
        plan_case(
            "tpcc.run.payment.update_district",
            "tpcc",
            "run",
            "tpcc/payment.go:10",
            "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?",
            [parameter("f64", 1.0)] + ints(2),
        ),
        plan_case(
            "tpcc.run.payment.select_district",
            "tpcc",
            "run",
            "tpcc/payment.go:11",
            "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?",
            ints(2),
        ),
        plan_case(
            "tpcc.run.payment.update_warehouse",
            "tpcc",
            "run",
            "tpcc/payment.go:12",
            "UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?",
            [parameter("f64", 1.0), parameter("i64", 1)],
        ),
        plan_case(
            "tpcc.run.payment.select_warehouse",
            "tpcc",
            "run",
            "tpcc/payment.go:13",
            "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?",
            ints(1),
        ),
        plan_case(
            "tpcc.run.payment.select_customer_list_by_last",
            "tpcc",
            "run",
            "tpcc/payment.go:14",
            "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
            ints(2) + [parameter("string", "ABLEABLEABLE")],
        ),
        plan_case(
            "tpcc.run.payment.select_customer_for_update",
            "tpcc",
            "run",
            "tpcc/payment.go:15",
            "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone,\nc_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ? \nAND c_id = ? FOR UPDATE",
            ints(3),
        ),
        plan_case(
            "tpcc.run.payment.update_customer",
            "tpcc",
            "run",
            "tpcc/payment.go:18",
            "UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, \nc_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
            [parameter("f64", 1.0), parameter("f64", 1.0)] + ints(3),
        ),
        plan_case(
            "tpcc.run.payment.select_customer_data",
            "tpcc",
            "run",
            "tpcc/payment.go:20",
            "SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
            ints(3),
        ),
        plan_case(
            "tpcc.run.payment.update_customer_with_data",
            "tpcc",
            "run",
            "tpcc/payment.go:21",
            "UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, \nc_payment_cnt = c_payment_cnt + 1, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
            [
                parameter("f64", 1.0),
                parameter("f64", 1.0),
                parameter("string", "customer-data"),
            ]
            + ints(3),
        ),
        plan_case(
            "tpcc.run.payment.insert_history",
            "tpcc",
            "run",
            "tpcc/payment.go:23",
            "INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)\nVALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ints(5)
            + [
                parameter("string", "2026-08-09 00:00:00"),
                parameter("f64", 1.0),
                parameter("string", "warehouse district"),
            ],
        ),
        plan_case(
            "tpcc.run.order_status.select_customer_count_by_last",
            "tpcc",
            "run",
            "tpcc/order_status.go:10",
            "SELECT count(c_id) namecnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?",
            ints(2) + [parameter("string", "ABLEABLEABLE")],
        ),
        plan_case(
            "tpcc.run.order_status.select_customer_by_last",
            "tpcc",
            "run",
            "tpcc/order_status.go:11",
            "SELECT c_balance, c_first, c_middle, c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
            ints(2) + [parameter("string", "ABLEABLEABLE")],
        ),
        plan_case(
            "tpcc.run.order_status.select_customer_by_id",
            "tpcc",
            "run",
            "tpcc/order_status.go:12",
            "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
            ints(3),
        ),
        plan_case(
            "tpcc.run.order_status.select_latest_order",
            "tpcc",
            "run",
            "tpcc/order_status.go:13",
            "SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1",
            ints(3),
        ),
        plan_case(
            "tpcc.run.order_status.select_order_line",
            "tpcc",
            "run",
            "tpcc/order_status.go:14",
            "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?",
            ints(3),
        ),
        plan_case(
            "tpcc.run.delivery.select_new_order",
            "tpcc",
            "run",
            "tpcc/delivery.go:17",
            "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE",
            ints(2),
        ),
        plan_case(
            "tpcc.run.delivery.update_customer",
            "tpcc",
            "run",
            "tpcc/delivery.go:33",
            "UPDATE customer SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
            [parameter("f64", 1.0)] + ints(3),
        ),
        plan_case(
            "tpcc.run.stock_level.select_district",
            "tpcc",
            "run",
            "tpcc/stock_level.go:9",
            "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?",
            ints(2),
        ),
        plan_case(
            "tpcc.run.stock_level.count",
            "tpcc",
            "run",
            "tpcc/stock_level.go:7",
            "SELECT /*+ TIDB_INLJ(order_line,stock) */ COUNT(DISTINCT (s_i_id)) stock_count FROM order_line, stock \nWHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= ? - 20 AND s_w_id = ? AND s_i_id = ol_i_id AND s_quantity < ?",
            ints(6),
        ),
    ]

    delivery_tuples: list[dict[str, Any]] = []
    for district in range(1, 11):
        delivery_tuples.extend(ints(1) + [parameter("i64", district)] + ints(1))
    delivery_specs = [
        (
            "delete_new_order",
            "tpcc/delivery.go:18",
            "DELETE FROM new_order WHERE (no_w_id, no_d_id, no_o_id) IN (\n\t%s\n)",
            delivery_tuples,
        ),
        (
            "update_order",
            "tpcc/delivery.go:21",
            "UPDATE orders SET o_carrier_id = ? WHERE (o_w_id, o_d_id, o_id) IN (\n\t%s\n)",
            ints(1) + delivery_tuples,
        ),
        (
            "select_orders",
            "tpcc/delivery.go:24",
            "SELECT o_d_id, o_c_id FROM orders WHERE (o_w_id, o_d_id, o_id) IN (\n\t%s\n)",
            delivery_tuples,
        ),
        (
            "update_order_line",
            "tpcc/delivery.go:27",
            "UPDATE order_line SET ol_delivery_d = ? WHERE (ol_w_id, ol_d_id, ol_o_id) IN (\n\t%s\n)",
            [parameter("string", "2026-08-09 00:00:00")] + delivery_tuples,
        ),
        (
            "select_sum_amount",
            "tpcc/delivery.go:30",
            "SELECT ol_d_id, SUM(ol_amount) FROM order_line WHERE (ol_w_id, ol_d_id, ol_o_id) IN (\n\t%s\n) GROUP BY ol_d_id",
            delivery_tuples,
        ),
    ]
    tuple_sql = ",".join("(?,?,?)" for _ in range(10))
    for name, source, template, params in delivery_specs:
        cases.append(
            plan_case(
                f"tpcc.run.delivery.{name}",
                "tpcc",
                "run",
                source,
                template % tuple_sql,
                params,
            )
        )

    for width in range(5, 16):
        item_sql = (
            "SELECT i_price, i_name, i_data, i_id FROM item WHERE i_id IN ("
            + ",".join("?" for _ in range(width))
            + ")"
        )
        stock_sql = (
            "SELECT s_i_id, s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, "
            "s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, "
            "s_dist_10 FROM stock WHERE (s_w_id, s_i_id) IN ("
            + ",".join("(?,?)" for _ in range(width))
            + ") FOR UPDATE"
        )
        order_line_sql = (
            "INSERT into order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, "
            "ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES "
            + ",".join("(?,?,?,?,?,?,?,?,?)" for _ in range(width))
        )
        cases.extend(
            [
                plan_case(
                    f"tpcc.run.new_order.select_items.width_{width}",
                    "tpcc",
                    "run",
                    "tpcc/new_order.go:34",
                    item_sql,
                    [parameter("i64", value) for value in range(1, width + 1)],
                ),
                plan_case(
                    f"tpcc.run.new_order.select_stock.width_{width}",
                    "tpcc",
                    "run",
                    "tpcc/new_order.go:46",
                    stock_sql,
                    [
                        parameter("i64", value)
                        for item in range(1, width + 1)
                        for value in (1, item)
                    ],
                ),
                plan_case(
                    f"tpcc.run.new_order.insert_order_line.width_{width}",
                    "tpcc",
                    "run",
                    "tpcc/new_order.go:58",
                    order_line_sql,
                    [
                        param
                        for item in range(1, width + 1)
                        for param in (
                            parameter("i64", 3001),
                            parameter("i64", 1),
                            parameter("i64", 1),
                            parameter("i64", item),
                            parameter("i64", item),
                            parameter("i64", 1),
                            parameter("i64", 5),
                            parameter("f64", 1.0),
                            parameter("string", "district-info"),
                        )
                    ],
                ),
            ]
        )

    cases.extend(
        [
            non_plan_case(
                "tpcc.run.transaction.begin",
                "tpcc",
                "run",
                "tpcc/workload.go:database/sql.DB.BeginTx",
                "BEGIN",
            ),
            non_plan_case(
                "tpcc.run.transaction.commit",
                "tpcc",
                "run",
                "tpcc/* transaction Commit",
                "COMMIT",
            ),
            non_plan_case(
                "tpcc.run.transaction.rollback",
                "tpcc",
                "run",
                "tpcc/* deferred transaction Rollback",
                "ROLLBACK",
            ),
        ]
    )
    return cases


def tpcc_check_cases() -> list[dict[str, Any]]:
    specs = [
        (
            "condition_01",
            71,
            "SELECT sum(d_ytd) - max(w_ytd) diff FROM district, warehouse WHERE d_w_id = w_id AND w_id = ? group by d_w_id",
            1,
        ),
        (
            "condition_02",
            106,
            "SELECT POWER((d_next_o_id -1 - mo), 2) + POWER((d_next_o_id -1 - mno), 2) diff FROM district dis, (SELECT o_d_id,max(o_id) mo FROM orders WHERE o_w_id= ? GROUP BY o_d_id) q, (select no_d_id,max(no_o_id) mno from new_order where no_w_id= ? group by no_d_id) no where d_w_id = ? and q.o_d_id=dis.d_id and no.no_d_id=dis.d_id",
            3,
        ),
        (
            "condition_03",
            136,
            "SELECT max(no_o_id)-min(no_o_id)+1 - count(*) diff from new_order where no_w_id = ? group by no_d_id",
            1,
        ),
        (
            "condition_04",
            166,
            "SELECT count(*) FROM (SELECT o_d_id, SUM(o_ol_cnt) sm1, MAX(cn) as cn FROM orders,(SELECT ol_d_id, COUNT(*) cn FROM order_line WHERE ol_w_id = ? GROUP BY ol_d_id) ol WHERE o_w_id = ? AND ol_d_id=o_d_id GROUP BY o_d_id) t1 WHERE sm1<>cn",
            2,
        ),
        (
            "condition_05",
            196,
            "SELECT count(*)  FROM orders LEFT JOIN new_order ON (no_w_id=o_w_id AND o_d_id=no_d_id AND o_id=no_o_id) where o_w_id = ? and ((o_carrier_id IS NULL and no_o_id IS  NULL) OR (o_carrier_id IS NOT NULL and no_o_id IS NOT NULL  )) ",
            1,
        ),
        (
            "condition_06",
            228,
            "\nSELECT COUNT(*) FROM\n(SELECT o_ol_cnt, order_line_count FROM orders\n\tLEFT JOIN (SELECT ol_w_id, ol_d_id, ol_o_id, count(*) order_line_count FROM order_line GROUP BY ol_w_id, ol_d_id, ol_o_id ORDER by ol_w_id, ol_d_id, ol_o_id) AS order_line\n\tON orders.o_w_id = order_line.ol_w_id AND orders.o_d_id = order_line.ol_d_id AND orders.o_id = order_line.ol_o_id\n\tWHERE orders.o_w_id = ?) AS T\nWHERE T.o_ol_cnt != T.order_line_count",
            1,
        ),
        (
            "condition_07",
            264,
            "SELECT count(*) FROM orders, order_line WHERE o_id=ol_o_id AND o_d_id=ol_d_id AND ol_w_id=o_w_id AND o_w_id = ? AND ((ol_delivery_d IS NULL and o_carrier_id IS NOT NULL) or (o_carrier_id IS NULL and ol_delivery_d IS NOT NULL ))",
            1,
        ),
        (
            "condition_08",
            294,
            "SELECT count(*) cn FROM (SELECT w_id,w_ytd,SUM(h_amount) sm FROM history,warehouse WHERE h_w_id=w_id and w_id = ? GROUP BY w_id) t1 WHERE w_ytd<>sm",
            1,
        ),
        (
            "condition_09",
            324,
            "SELECT COUNT(*) FROM (select d_id,d_w_id,sum(d_ytd) s1 from district group by d_id,d_w_id) d,(select h_d_id,h_w_id,sum(h_amount) s2 from history WHERE  h_w_id = ? group by h_d_id, h_w_id) h WHERE h_d_id=d_id AND d_w_id=h_w_id and d_w_id= ? and s1<>s2",
            2,
        ),
        (
            "condition_10",
            354,
            "SELECT count(*) \n\tFROM (  SELECT  c.c_id, c.c_d_id, c.c_w_id, c.c_balance c1, \n\t\t\t\t   (SELECT sum(ol_amount) FROM orders, order_line \n\t\t\t\t\t WHERE OL_W_ID=O_W_ID \n\t\t\t\t\t   AND OL_D_ID = O_D_ID \n\t\t\t\t\t   AND OL_O_ID = O_ID \n\t\t\t\t\t   AND OL_DELIVERY_D IS NOT NULL \n\t\t\t\t\t   AND O_W_ID=? \n\t\t\t\t\t   AND O_D_ID=c.C_D_ID \n\t\t\t\t\t   AND O_C_ID=c.C_ID) sm, (SELECT  sum(h_amount)  from  history \n\t\t\t\t\t\t\t\t\t\t\t\tWHERE H_C_W_ID=? \n\t\t\t\t\t\t\t\t\t\t\t\t  AND H_C_D_ID=c.C_D_ID \n\t\t\t\t\t\t\t\t\t\t\t\t  AND H_C_ID=c.C_ID) smh \n\t\t\t FROM customer c \n\t\t\tWHERE  c.c_w_id = ? ) t\n   WHERE c1<>sm-smh",
            3,
        ),
        (
            "condition_11",
            402,
            "\nSELECT count(*) FROM\n\t(SELECT * FROM\n\t\t(SELECT o_w_id, o_d_id, count(*) order_count FROM orders GROUP BY o_w_id, o_d_id) orders\n        JOIN (SELECT no_w_id, no_d_id, count(*) new_order_count FROM new_order GROUP BY no_w_id, no_d_id) new_order\n        ON orders.o_w_id = new_order.no_w_id AND orders.o_d_id = new_order.no_d_id\n\t) order_new_order\nJOIN (SELECT c_w_id, c_d_id, count(*) customer_count FROM customer GROUP BY c_w_id, c_d_id) customer\nON order_new_order.no_w_id = customer.c_w_id AND order_new_order.no_d_id = customer.c_d_id\nWHERE c_w_id = ? AND order_count - 2100 != new_order_count",
            1,
        ),
        (
            "condition_12",
            440,
            "SELECT count(*) FROM (SELECT  c.c_id, c.c_d_id, c.c_balance c1, c_ytd_payment, \n\t\t(SELECT sum(ol_amount) FROM orders, order_line \n\t\tWHERE OL_W_ID=O_W_ID AND OL_D_ID = O_D_ID AND OL_O_ID = O_ID AND OL_DELIVERY_D IS NOT NULL AND \n\t\tO_W_ID=? AND O_D_ID=c.C_D_ID AND O_C_ID=c.C_ID) sm FROM customer c WHERE  c.c_w_id = ?) t1 \n\t\tWHERE c1+c_ytd_payment <> sm",
            2,
        ),
    ]
    return [
        plan_case(
            f"tpcc.check.{name}",
            "tpcc",
            "check",
            f"tpcc/check.go:{line}",
            sql,
            ints(parameter_count),
        )
        for name, line, sql, parameter_count in specs
    ]


SYSBENCH_TABLES = 32
SYSBENCH_THREADS = 16
SYSBENCH_TABLE_SIZE = 10_000_000


def sysbench_common_table_run_cases(table_number: int) -> list[dict[str, Any]]:
    read_workloads = ["oltp_read_only.lua", "oltp_read_write.lua"]
    write_workloads = ["oltp_write_only.lua", "oltp_read_write.lua"]
    table_name = f"sbtest{table_number}"
    identity_suffix = f".table_{table_number}"
    return [
        plan_case(
            "sysbench.run.point_select" + identity_suffix,
            "sysbench",
            "run",
            "src/lua/oltp_common.lua:246",
            f"SELECT c FROM {table_name} WHERE id=?",
            [parameter("i32", 1)],
            workloads=read_workloads + ["oltp_point_select.lua"],
        ),
        plan_case(
            "sysbench.run.simple_range" + identity_suffix,
            "sysbench",
            "run",
            "src/lua/oltp_common.lua:249",
            f"SELECT c FROM {table_name} WHERE id BETWEEN ? AND ?",
            [parameter("i32", 1), parameter("i32", 100)],
            workloads=read_workloads,
        ),
        plan_case(
            "sysbench.run.sum_range" + identity_suffix,
            "sysbench",
            "run",
            "src/lua/oltp_common.lua:252",
            f"SELECT SUM(k) FROM {table_name} WHERE id BETWEEN ? AND ?",
            [parameter("i32", 1), parameter("i32", 100)],
            workloads=read_workloads,
        ),
        plan_case(
            "sysbench.run.order_range" + identity_suffix,
            "sysbench",
            "run",
            "src/lua/oltp_common.lua:255",
            f"SELECT c FROM {table_name} WHERE id BETWEEN ? AND ? ORDER BY c",
            [parameter("i32", 1), parameter("i32", 100)],
            workloads=read_workloads,
        ),
        plan_case(
            "sysbench.run.distinct_range" + identity_suffix,
            "sysbench",
            "run",
            "src/lua/oltp_common.lua:258",
            f"SELECT DISTINCT c FROM {table_name} WHERE id BETWEEN ? AND ? ORDER BY c",
            [parameter("i32", 1), parameter("i32", 100)],
            workloads=read_workloads,
        ),
        plan_case(
            "sysbench.run.index_update" + identity_suffix,
            "sysbench",
            "run",
            "src/lua/oltp_common.lua:261",
            f"UPDATE {table_name} SET k=k+1 WHERE id=?",
            [parameter("i32", 1)],
            workloads=write_workloads + ["oltp_update_index.lua"],
        ),
        plan_case(
            "sysbench.run.non_index_update" + identity_suffix,
            "sysbench",
            "run",
            "src/lua/oltp_common.lua:264",
            f"UPDATE {table_name} SET c=? WHERE id=?",
            [parameter("string", "x" * 120), parameter("i32", 1)],
            workloads=write_workloads + ["oltp_update_non_index.lua"],
        ),
        plan_case(
            "sysbench.run.delete" + identity_suffix,
            "sysbench",
            "run",
            "src/lua/oltp_common.lua:267",
            f"DELETE FROM {table_name} WHERE id=?",
            [parameter("i32", 1)],
            workloads=write_workloads,
        ),
        plan_case(
            "sysbench.run.insert_after_delete" + identity_suffix,
            "sysbench",
            "run",
            "src/lua/oltp_common.lua:270",
            f"INSERT INTO {table_name} (id, k, c, pad) VALUES (?, ?, ?, ?)",
            [
                parameter("i32", 1),
                parameter("i32", 1),
                parameter("string", "x" * 120),
                parameter("string", "x" * 60),
            ],
            workloads=write_workloads,
        ),
    ]


def sysbench_worker_local_run_cases(table_number: int) -> list[dict[str, Any]]:
    table_name = f"sbtest{table_number}"
    identity_suffix = f".table_{table_number}"
    return [
        plan_case(
            "sysbench.run.random_points.width_10" + identity_suffix,
            "sysbench",
            "run",
            "src/lua/select_random_points.lua:39 + compatibility patch",
            f"SELECT id, k, c, pad FROM {table_name} WHERE k IN ("
            + ", ".join("?" for _ in range(10))
            + ")",
            [parameter("i32", value) for value in range(1, 11)],
            workloads=["select_random_points.lua"],
        ),
        plan_case(
            "sysbench.run.random_ranges.width_10" + identity_suffix,
            "sysbench",
            "run",
            "src/lua/select_random_ranges.lua:43 + compatibility patch",
            f"SELECT count(k) FROM {table_name} WHERE "
            + " OR ".join("k BETWEEN ? AND ?" for _ in range(10)),
            [
                parameter("i32", value)
                for start in range(1, 11)
                for value in (start, start + 5)
            ],
            workloads=["select_random_ranges.lua"],
        ),
        plan_case(
            "sysbench.run.oltp_insert.direct" + identity_suffix,
            "sysbench",
            "run",
            "src/lua/oltp_insert.lua:61 + compatibility patch",
            f"INSERT INTO {table_name} (id, k, c, pad) VALUES (10000001, 1, 'c', 'pad')",
            protocol="direct",
            workloads=["oltp_insert.lua"],
        ),
    ]


def sysbench_run_cases() -> list[dict[str, Any]]:
    read_workloads = ["oltp_read_only.lua", "oltp_read_write.lua"]
    write_workloads = ["oltp_write_only.lua", "oltp_read_write.lua"]
    all_transactional = sorted(set(read_workloads + write_workloads))
    cases = [
        non_plan_case(
            "sysbench.run.transaction.begin",
            "sysbench",
            "run",
            "src/lua/oltp_common.lua:275",
            "BEGIN",
            all_transactional,
            protocol="prepared",
        ),
        non_plan_case(
            "sysbench.run.transaction.commit",
            "sysbench",
            "run",
            "src/lua/oltp_common.lua:279",
            "COMMIT",
            all_transactional,
            protocol="prepared",
        ),
    ]
    for table_number in range(1, SYSBENCH_TABLES + 1):
        cases.extend(sysbench_common_table_run_cases(table_number))
    for table_number in range(1, SYSBENCH_THREADS + 1):
        cases.extend(sysbench_worker_local_run_cases(table_number))
    return cases


def tpcc_prepare_plan_families() -> list[dict[str, Any]]:
    return [
        multi_row_insert_family(
            "tpcc.prepare.insert.item",
            "tpcc",
            "tpcc/load.go:26 + pkg/sink/sql.go:30",
            "INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) VALUES  ",
            "({row},1,'item',1.00,'data')",
            [672, 1024],
            proof="100000 rows with maxBatchRows=1024 emit 1024-row batches and one 672-row tail",
        ),
        multi_row_insert_family(
            "tpcc.prepare.insert.warehouse",
            "tpcc",
            "tpcc/load.go:51 + pkg/sink/sql.go:30",
            "INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES  ",
            "({row},'w','s1','s2','city','ST','123456789',0.1000,300000.00)",
            [1],
            proof="each warehouse loader creates a fresh sink and flushes exactly one row",
        ),
        multi_row_insert_family(
            "tpcc.prepare.insert.stock",
            "tpcc",
            "tpcc/load.go:78 + pkg/sink/sql.go:30",
            "INSERT INTO stock (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) VALUES  ",
            "({row},1,50,'d01','d02','d03','d04','d05','d06','d07','d08','d09','d10',0,0,0,'data')",
            [672, 1024],
            proof="100000 rows per warehouse with maxBatchRows=1024 emit 1024-row batches and one 672-row tail",
        ),
        multi_row_insert_family(
            "tpcc.prepare.insert.district",
            "tpcc",
            "tpcc/load.go:119 + pkg/sink/sql.go:30",
            "INSERT INTO district (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES  ",
            "({row},1,'district','s1','s2','city','ST','123456789',0.1000,30000.00,3001)",
            [10],
            proof="each warehouse has exactly ten districts in one sink flush",
        ),
        multi_row_insert_family(
            "tpcc.prepare.insert.customer",
            "tpcc",
            "tpcc/load.go:154 + pkg/sink/sql.go:30",
            "INSERT INTO customer (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) VALUES  ",
            "({row},1,1,'first','OE','ABLEABLEABLE','s1','s2','city','ST','123456789','1234567890123456','2026-08-09 00:00:00','GC',50000.00,0.1000,-10.00,10.00,1,0,'data')",
            [952, 1024],
            proof="3000 rows per district emit two 1024-row batches and one 952-row tail",
        ),
        multi_row_insert_family(
            "tpcc.prepare.insert.history",
            "tpcc",
            "tpcc/load.go:210 + pkg/sink/sql.go:30",
            "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES  ",
            "({row},1,1,1,1,'2026-08-09 00:00:00',10.00,'history')",
            [952, 1024],
            proof="3000 rows per district emit two 1024-row batches and one 952-row tail",
        ),
        multi_row_insert_family(
            "tpcc.prepare.insert.orders",
            "tpcc",
            "tpcc/load.go:240 + pkg/sink/sql.go:30",
            "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES  ",
            "({row},1,1,{row},'2026-08-09 00:00:00',1,10,1)",
            [952, 1024],
            proof="3000 rows per district emit two 1024-row batches and one 952-row tail",
        ),
        multi_row_insert_family(
            "tpcc.prepare.insert.new_order",
            "tpcc",
            "tpcc/load.go:284 + pkg/sink/sql.go:30",
            "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES  ",
            "({row},1,1)",
            [900],
            proof="each district has exactly 900 new-order rows in one sink flush",
        ),
        multi_row_insert_family(
            "tpcc.prepare.insert.order_line",
            "tpcc",
            "tpcc/load.go:310 + pkg/sink/sql.go:30",
            "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES  ",
            "({row},1,1,1,1,1,'2026-08-09 00:00:00',5,1.00,'district-info')",
            {"min": 1, "max": 1024},
            proof="3000 independent 5..15 order-line counts can yield every tail width 1..1023; maxBatchRows adds width 1024",
        ),
    ]


def sysbench_plan_families() -> list[dict[str, Any]]:
    families = [
        multi_row_insert_family(
            f"sysbench.prepare.insert.table_{table_number}",
            "sysbench",
            "prepare.lua:35",
            f"INSERT INTO sbtest{table_number} (id, k, c, pad) VALUES ",
            "({row},1,'cccc','pad')",
            [1000],
            separator=",",
            proof=(
                "fixed table_size=10000000 is exactly divisible by "
                "prepare.lua load_batch_rows=1000"
            ),
        )
        for table_number in range(1, SYSBENCH_TABLES + 1)
    ]
    families.extend(
        multi_row_insert_family(
            f"sysbench.run.bulk_insert.table_{table_number}",
            "sysbench",
            "bulk_insert.lua compatibility patch + src/db_driver.c:46,832",
            f"INSERT INTO sbtest{table_number} (id, k, c, pad) VALUES",
            "({row},{row},'','')/*" + "x" * 600 + "*/",
            {"min": 1, "max": 851},
            phase="run",
            separator=",",
            workloads=["bulk_insert.lua"],
            proof=(
                "512KiB BULK_PACKET_SIZE admits at most 851 minimum-width "
                "padded rows; shutdown can flush every width 1..851"
            ),
        )
        for table_number in range(1, SYSBENCH_THREADS + 1)
    )
    return families


def tpcc_prepare_ddl_cases() -> list[dict[str, Any]]:
    definitions = {
        "warehouse": """
CREATE TABLE IF NOT EXISTS warehouse (
    w_id INT NOT NULL,
    w_name VARCHAR(10),
    w_street_1 VARCHAR(20),
    w_street_2 VARCHAR(20),
    w_city VARCHAR(20),
    w_state CHAR(2),
    w_zip CHAR(9),
    w_tax DECIMAL(4, 4),
    w_ytd DECIMAL(12, 2),
    PRIMARY KEY (w_id) /*T![clustered_index] CLUSTERED */
)""",
        "district": """
CREATE TABLE IF NOT EXISTS district (
    d_id INT NOT NULL,
    d_w_id INT NOT NULL,
    d_name VARCHAR(10),
    d_street_1 VARCHAR(20),
    d_street_2 VARCHAR(20),
    d_city VARCHAR(20),
    d_state CHAR(2),
    d_zip CHAR(9),
    d_tax DECIMAL(4, 4),
    d_ytd DECIMAL(12, 2),
    d_next_o_id INT,
    PRIMARY KEY (d_w_id, d_id) /*T![clustered_index] CLUSTERED */
)""",
        "customer": """
CREATE TABLE IF NOT EXISTS customer (
    c_id INT NOT NULL,
    c_d_id INT NOT NULL,
    c_w_id INT NOT NULL,
    c_first VARCHAR(16),
    c_middle CHAR(2),
    c_last VARCHAR(16),
    c_street_1 VARCHAR(20),
    c_street_2 VARCHAR(20),
    c_city VARCHAR(20),
    c_state CHAR(2),
    c_zip CHAR(9),
    c_phone CHAR(16),
    c_since DATETIME,
    c_credit CHAR(2),
    c_credit_lim DECIMAL(12, 2),
    c_discount DECIMAL(4,4),
    c_balance DECIMAL(12,2),
    c_ytd_payment DECIMAL(12,2),
    c_payment_cnt INT,
    c_delivery_cnt INT,
    c_data VARCHAR(500),
    PRIMARY KEY(c_w_id, c_d_id, c_id) /*T![clustered_index] CLUSTERED */,
    INDEX idx_customer (c_w_id, c_d_id, c_last, c_first)
)""",
        "history": """
CREATE TABLE IF NOT EXISTS history (
    h_c_id INT NOT NULL,
    h_c_d_id INT NOT NULL,
    h_c_w_id INT NOT NULL,
    h_d_id INT NOT NULL,
    h_w_id INT NOT NULL,
    h_date DATETIME,
    h_amount DECIMAL(6, 2),
    h_data VARCHAR(24),
    INDEX idx_h_w_id (h_w_id),
    INDEX idx_h_c_w_id (h_c_w_id)
)""",
        "new_order": """
CREATE TABLE IF NOT EXISTS new_order (
    no_o_id INT NOT NULL,
    no_d_id INT NOT NULL,
    no_w_id INT NOT NULL,
    PRIMARY KEY(no_w_id, no_d_id, no_o_id) /*T![clustered_index] CLUSTERED */
)""",
        "orders": """
CREATE TABLE IF NOT EXISTS orders (
    o_id INT NOT NULL,
    o_d_id INT NOT NULL,
    o_w_id INT NOT NULL,
    o_c_id INT,
    o_entry_d DATETIME,
    o_carrier_id INT,
    o_ol_cnt INT,
    o_all_local INT,
    PRIMARY KEY(o_w_id, o_d_id, o_id) /*T![clustered_index] CLUSTERED */,
    INDEX idx_order (o_w_id, o_d_id, o_c_id, o_id)
)""",
        "order_line": """
CREATE TABLE IF NOT EXISTS order_line (
    ol_o_id INT NOT NULL,
    ol_d_id INT NOT NULL,
    ol_w_id INT NOT NULL,
    ol_number INT NOT NULL,
    ol_i_id INT NOT NULL,
    ol_supply_w_id INT,
    ol_delivery_d DATETIME,
    ol_quantity INT,
    ol_amount DECIMAL(6, 2),
    ol_dist_info CHAR(24),
    PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number) /*T![clustered_index] CLUSTERED */
)""",
        "stock": """
CREATE TABLE IF NOT EXISTS stock (
    s_i_id INT NOT NULL,
    s_w_id INT NOT NULL,
    s_quantity INT,
    s_dist_01 CHAR(24),
    s_dist_02 CHAR(24),
    s_dist_03 CHAR(24),
    s_dist_04 CHAR(24),
    s_dist_05 CHAR(24),
    s_dist_06 CHAR(24),
    s_dist_07 CHAR(24),
    s_dist_08 CHAR(24),
    s_dist_09 CHAR(24),
    s_dist_10 CHAR(24),
    s_ytd INT,
    s_order_cnt INT,
    s_remote_cnt INT,
    s_data VARCHAR(50),
    PRIMARY KEY(s_w_id, s_i_id) /*T![clustered_index] CLUSTERED */
)""",
        "item": """
CREATE TABLE IF NOT EXISTS item (
    i_id INT NOT NULL,
    i_im_id INT,
    i_name VARCHAR(24),
    i_price DECIMAL(5, 2),
    i_data VARCHAR(50),
    PRIMARY KEY(i_id) /*T![clustered_index] CLUSTERED */
)""",
    }
    return [
        non_plan_case(
            f"tpcc.prepare.create_table.{table}",
            "tpcc",
            "prepare",
            "tpcc/ddl.go:createTables mysql, parts=1, use-fk=false",
            sql.strip(),
        )
        for table, sql in definitions.items()
    ]


def sysbench_schema_cases() -> list[dict[str, Any]]:
    cases = [
        non_plan_case(
            "sysbench.prepare.create_database",
            "sysbench",
            "prepare",
            "prepare.lua:72 + prepare_baseline_populated_dataset.sh:87",
            "CREATE DATABASE sbtest",
        )
    ]
    for table_number in range(1, SYSBENCH_TABLES + 1):
        table_name = f"sbtest{table_number}"
        index_name = f"k_{table_number}"
        cases.extend(
            [
                non_plan_case(
                    f"sysbench.prepare.create_table.table_{table_number}",
                    "sysbench",
                    "prepare",
                    "prepare.lua:29",
                    (
                        f"CREATE TABLE IF NOT EXISTS {table_name} ("
                        "id INT NOT NULL, k INT NOT NULL, c CHAR(120) NOT NULL, "
                        "pad CHAR(60) NOT NULL, PRIMARY KEY (id), "
                        f"INDEX {index_name} (k))"
                    ),
                ),
                non_plan_case(
                    f"sysbench.prepare.split_table.table_{table_number}",
                    "sysbench",
                    "prepare",
                    "prepare_baseline_populated_dataset.sh:102",
                    (
                        f"SPLIT TABLE {table_name} BETWEEN (0) AND "
                        f"({SYSBENCH_TABLE_SIZE + 1}) REGIONS 8"
                    ),
                ),
                non_plan_case(
                    f"sysbench.prepare.split_index.table_{table_number}",
                    "sysbench",
                    "prepare",
                    "prepare_baseline_populated_dataset.sh:104",
                    (
                        f"SPLIT TABLE {table_name} INDEX {index_name} "
                        f"BETWEEN (0) AND ({SYSBENCH_TABLE_SIZE + 1}) REGIONS 8"
                    ),
                ),
            ]
        )
    return cases


def cleanup_cases() -> list[dict[str, Any]]:
    tpcc_tables = (
        "item",
        "customer",
        "district",
        "history",
        "new_order",
        "order_line",
        "orders",
        "stock",
        "warehouse",
    )
    cases = [
        non_plan_case(
            f"tpcc.cleanup.drop_{table}",
            "tpcc",
            "cleanup",
            "tpcc/ddl.go:687",
            f"DROP TABLE IF EXISTS {table}",
        )
        for table in tpcc_tables
    ]
    cases.extend(
        non_plan_case(
            f"sysbench.cleanup.drop_table.table_{table_number}",
            "sysbench",
            "cleanup",
            "src/lua/oltp_common.lua:392",
            f"DROP TABLE IF EXISTS sbtest{table_number}",
        )
        for table_number in range(1, SYSBENCH_TABLES + 1)
    )
    return cases


def build_manifest() -> dict[str, Any]:
    cases = (
        tpcc_run_cases()
        + tpcc_check_cases()
        + sysbench_run_cases()
        + tpcc_prepare_ddl_cases()
        + sysbench_schema_cases()
        + cleanup_cases()
    )
    plan_families = tpcc_prepare_plan_families() + sysbench_plan_families()
    return {
        "schema_version": "1.0",
        "coverage_status": "incomplete",
        "coverage": {
            "tpcc": {
                "run": "candidate inventory generated; runtime coverage pending",
                "check": "candidate inventory generated; runtime coverage pending",
                "prepare": "all loader INSERT width families and exact default MySQL DDL generated; runtime coverage pending",
                "cleanup": "drop-table inventory generated; execution compatibility pending",
            },
            "sysbench": {
                "run": "32 common prepared-table variants, 16 worker-local variants, and all bulk widths generated; runtime coverage pending",
                "prepare": "32 exact 1000-row INSERT, DDL, table split, and index split variants generated; runtime coverage pending",
                "cleanup": "all 32 drop-table variants generated; execution compatibility pending",
            },
        },
        "benchmark_contract": {
            "tpcc": {
                "warehouses": 100,
                "threads": 16,
                "parts": 1,
                "partition_type": 1,
                "use_fk": False,
            },
            "sysbench": {
                "tables": SYSBENCH_TABLES,
                "threads": SYSBENCH_THREADS,
                "table_size": SYSBENCH_TABLE_SIZE,
                "split_regions": 8,
            },
        },
        "source_pins": {
            "go_tpc": {
                "commit": "688d62f3be7ea6b68c2bb5fbbeb925bde681fb05",
                "version": "v1.0.12",
            },
            "sysbench": {
                "commit": "ebf1c90da05dea94648165e4f149abc20c979557",
                "version": "1.0.20",
            },
        },
        "source_files": {
            "go_tpc": {
                "tpcc/delivery.go": "43df258c84239b8bfdcccfe3adad8cdeac12093a0c024ae119e9dd0807f56f62",
                "tpcc/check.go": "3e8c6ce5c7ebf97544ab2361bd5f7b66c5136faba798f689d8db9a0184f23cfd",
                "tpcc/ddl.go": "29b13c1c2de13ae21faec28cadb0eee5914b135c0f5456d851404b2bdc033bbc",
                "tpcc/load.go": "31360fe20235bd0f08d7de5f5993ed037f8064156249c28e76a5144499c2ffc6",
                "tpcc/new_order.go": "e08c42327418e17afef3c09d5c70a4c05d589fade0865ec1c45dfa39e19a9a44",
                "tpcc/order_status.go": "1a6a8acfd7119fcfbc18a1bc049f6ba7300c55374a2a0b36d7282cd4f3b94705",
                "tpcc/payment.go": "468e884081f4841c483c1e9e108276e19e3841b270317c2ee80839492b16202d",
                "tpcc/stock_level.go": "5624c11048aae3e12a55b08956c11cf8aaadeab53898e48cb4346dd736928195",
                "tpcc/workload.go": "f0be33eae3330efecafb4ba40b8f880f99faea33c33e8febb7c3acbbca051f6b",
            },
            "sysbench": {
                "src/lua/bulk_insert.lua": "b183f7a112ce78e7d6f5d1c151c0192f83318593db8a51122f2e93e344b7cee6",
                "src/db_driver.c": "bde5e20b40301ca57f7d0a20b60e797dd34fdbc4a477b266a30cbc49fe26c5c0",
                "src/lua/oltp_common.lua": "710c3b38027668c83e46f3c0814dc7cfd67cc8c4012df6cab461e46d62b5996a",
                "src/lua/oltp_insert.lua": "2f7f740ef7281b0ce77236d979ff67e16580b6de5082dd711d6a91560ccdbdb3",
                "src/lua/select_random_points.lua": "c84d035191a8c895f31b9d6e92089105041aa6a1cf21f3dec07aca8c15d62306",
                "src/lua/select_random_ranges.lua": "aba6f88f3fe18a24a9ff2926e0fd535164f3b22866e96db32b7d6319bd288593",
            },
            "sysbench_contract": {
                "manifest.json": "bb36351184a6fff15a309244d9757a3d225661d7c02cb57009f41b626235a663",
                "prepare.lua": "6cad222dd1a6c6a08af171358b80cadcc4fad984328b91997a38df765913a413",
                "prepare_baseline_populated_dataset.sh": "f803e531806b1e8734e5fe9601c49b062a13905fa0dc8ffcdebe1e4926664ca0",
                "patches/bulk_insert.patch": "eb680c6bdfcd4ef2ba5de99c2c6c5e7df34b1c89cbc04843c7b44bb17a7dd3e0",
                "patches/oltp_common.patch": "c7ceae0d3f8af5947dccd009a33bd641da8519725625c35f3f27d00a3599f08e",
                "patches/oltp_insert.patch": "e26dd998eaf5c9b808c6f6be80442582b0953f78b6d4b33e2130af9a521ecd20",
                "patches/select_random_points.patch": "e6499503793d2347e9fcccf8b02951f3979f8743cdd6a62d2ebc4bf78c9e9170",
                "patches/select_random_ranges.patch": "895661f5297cf9ae440313f6bf3b5d91ea9c5454e9805e0c1cdb4bdb60f07c10",
            },
        },
        "static_case_count": len(cases),
        "plan_family_count": len(plan_families),
        "expanded_case_count": len(cases)
        + sum(
            len(family["row_counts"])
            if isinstance(family["row_counts"], list)
            else family["row_counts"]["max"] - family["row_counts"]["min"] + 1
            for family in plan_families
        ),
        "cases": cases,
        "plan_families": plan_families,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    rendered = json.dumps(build_manifest(), indent=2, sort_keys=True) + "\n"
    if args.check:
        try:
            current = args.output.read_text()
        except OSError as error:
            print(f"cannot read generated manifest: {error}", file=sys.stderr)
            return 1
        if current != rendered:
            print(f"generated manifest is stale: {args.output}", file=sys.stderr)
            return 1
        return 0
    args.output.write_text(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
