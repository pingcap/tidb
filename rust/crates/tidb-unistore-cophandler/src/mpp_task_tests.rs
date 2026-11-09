// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Two MPP tasks over the tunnels (Go `HandleMPPDAGReq`, the exchange
//! executors, `MPPTaskHandler`): a scan task hash-sends its rows to a
//! receiver task, which passes them to TiDB.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use prost::Message;
use tidb_datatype::Datum;
use tidb_proto::{coprocessor, mpp, tipb};
use tidb_unistore_tikv::mvcc_store::MvccStore;

use crate::mpp_task::{MppCtx, MppRpcClient, MppTaskHandler};

/// The store's tasks by id, answering an `EstablishMPPConnection` as Go's
/// `EstablishMPPConnectionWithStoreID` does: the sender task's tunnel to
/// the receiver, drained into packets.
#[derive(Default)]
struct Tasks(Mutex<HashMap<i64, Arc<MppTaskHandler>>>);

impl MppRpcClient for Tasks {
    fn establish_mpp_conn(&self, _: &str, req: mpp::EstablishMppConnectionRequest) -> Result<Vec<mpp::MppDataPacket>, String> {
        let sender = req.sender_meta.as_ref().map_or(0, |meta| meta.task_id);
        let handler = Arc::clone(self.0.lock().unwrap().get(&sender).ok_or("task not found")?);
        let tunnel = handler.handle_establish_conn(&req)?;
        let mut packets = Vec::new();
        loop {
            let (chunk, err) = tunnel.recv_chunk();
            if let Some(err) = err {
                packets.push(mpp::MppDataPacket { error: Some(mpp::Error { msg: err, ..Default::default() }), ..Default::default() });
                break;
            }
            let Some(chunk) = chunk else { break };
            let data = tipb::SelectResponse { chunks: vec![chunk], ..Default::default() }.encode_to_vec();
            packets.push(mpp::MppDataPacket { data, ..Default::default() });
        }
        Ok(packets)
    }
}

fn meta(task_id: i64) -> mpp::TaskMeta {
    mpp::TaskMeta { task_id, gather_id: 1, start_ts: 20, address: "tiflash0".to_owned(), ..Default::default() }
}

fn int_field_type() -> tipb::FieldType {
    tipb::FieldType { tp: Some(8), flen: Some(20), collate: Some(63), charset: Some("binary".to_owned()), ..Default::default() }
}

fn column(id: i64) -> tipb::ColumnInfo {
    tipb::ColumnInfo { column_id: Some(id), tp: Some(8), pk_handle: Some(id == 1), ..Default::default() }
}

fn sender(tp: tipb::ExchangeType, targets: &[mpp::TaskMeta], partition_keys: Vec<tipb::Expr>, child: tipb::Executor) -> tipb::Executor {
    tipb::Executor {
        tp: Some(tipb::ExecType::TypeExchangeSender as i32),
        exchange_sender: Some(Box::new(tipb::ExchangeSender {
            tp: Some(tp as i32),
            encoded_task_meta: targets.iter().map(Message::encode_to_vec).collect(),
            partition_keys,
            child: Some(Box::new(child)),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn dag(root: tipb::Executor) -> coprocessor::Request {
    let dag = tipb::DagRequest { root_executor: Some(root), output_offsets: vec![0, 1], ..Default::default() };
    let (start, end) = tidb_tablecodec::table_key::get_table_handle_key_range(90);
    coprocessor::Request {
        tp: crate::REQ_TYPE_DAG,
        data: dag.encode_to_vec(),
        ranges: vec![coprocessor::KeyRange { start, end }],
        start_ts: 20,
        ..Default::default()
    }
}

fn put(store: &mut MvccStore, handle: i64, b: i64) {
    let key = tidb_tablecodec::table_key::encode_row_key_with_handle(90, &tidb_tablecodec::table_key::RecordHandle::Int(handle));
    let value = tidb_codec::encode_value(&[Datum::Int(2), Datum::Int(b)]).unwrap();
    store
        .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
            mutations: vec![tidb_proto::KvrpcMutation {
                op: tidb_proto::KvrpcOp::Put as i32,
                key: key.clone(),
                value,
                ..Default::default()
            }],
            primary_lock: key.clone(),
            start_version: 10,
            ..Default::default()
        })
        .unwrap();
    store.commit(&[key], 10, 11).unwrap();
}

#[test]
fn a_scan_task_hash_sends_through_a_receiver_task_to_tidb() {
    let mut store = MvccStore::new();
    for handle in 1..=6 {
        put(&mut store, handle, handle * 10);
    }
    let tasks = Arc::new(Tasks::default());
    let rpc: Arc<dyn MppRpcClient> = Arc::clone(&tasks) as Arc<dyn MppRpcClient>;
    let (scan_task, recv_task, tidb_task) = (meta(1), meta(2), meta(-1));
    for task in [&scan_task, &recv_task] {
        tasks.0.lock().unwrap().insert(task.task_id, Arc::new(MppTaskHandler::new(task.clone(), Arc::clone(&rpc))));
    }
    let ctx = |task: &mpp::TaskMeta| {
        Arc::new(MppCtx {
            rpc_client: Arc::clone(&rpc),
            store_addr: "tiflash0".to_owned(),
            task_handler: Arc::clone(&tasks.0.lock().unwrap()[&task.task_id]),
        })
    };

    let scan = tipb::Executor {
        tp: Some(tipb::ExecType::TypeTableScan as i32),
        tbl_scan: Some(tipb::TableScan { table_id: Some(90), columns: vec![column(1), column(2)], ..Default::default() }),
        ..Default::default()
    };
    let mut key = Vec::new();
    tidb_codec::encode_int(&mut key, 1);
    let hash_key = tipb::Expr { tp: Some(tipb::ExprType::ColumnRef as i32), val: Some(key), field_type: Some(int_field_type()), ..Default::default() };
    let scan_dag = dag(sender(tipb::ExchangeType::Hash, std::slice::from_ref(&recv_task), vec![hash_key], scan));
    let receiver = tipb::Executor {
        tp: Some(tipb::ExecType::TypeExchangeReceiver as i32),
        exchange_receiver: Some(tipb::ExchangeReceiver {
            encoded_task_meta: vec![scan_task.encode_to_vec()],
            field_types: vec![int_field_type(), int_field_type()],
            ..Default::default()
        }),
        ..Default::default()
    };
    let recv_dag = dag(sender(tipb::ExchangeType::PassThrough, std::slice::from_ref(&tidb_task), Vec::new(), receiver));

    let packets = std::thread::scope(|scope| {
        let store = &store;
        scope.spawn(|| crate::handle_mpp_dag_req(store, &scan_dag, ctx(&scan_task)));
        scope.spawn(|| crate::handle_mpp_dag_req(store, &recv_dag, ctx(&recv_task)));
        rpc.establish_mpp_conn(
            "tiflash0",
            mpp::EstablishMppConnectionRequest { sender_meta: Some(recv_task.clone()), receiver_meta: Some(tidb_task.clone()), ..Default::default() },
        )
        .unwrap()
    });
    let mut rows = Vec::new();
    for packet in packets {
        assert!(packet.error.is_none(), "{:?}", packet.error);
        for chunk in tipb::SelectResponse::decode(packet.data.as_slice()).unwrap().chunks {
            let mut rest = chunk.rows_data.as_deref().unwrap_or_default();
            while !rest.is_empty() {
                let (remaining, datum) = tidb_codec::decode_one(rest).unwrap();
                rows.push(datum);
                rest = remaining;
            }
        }
    }
    let mut pairs: Vec<(Datum, Datum)> = rows.chunks(2).map(|row| (row[0].clone(), row[1].clone())).collect();
    pairs.sort_by_key(|(a, _)| match a {
        Datum::Int(a) => *a,
        _ => 0,
    });
    assert_eq!(pairs, (1..=6).map(|h| (Datum::Int(h), Datum::Int(h * 10))).collect::<Vec<_>>());
}
