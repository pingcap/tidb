// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/framework/metering/metering_test.go`.
use super::*;
use crate::{CLUSTER_READ_BYTES_FIELD, DataValues, GET_REQUESTS_FIELD, PUT_REQUESTS_FIELD};
use std::sync::atomic::{AtomicUsize, Ordering};

static TEST_LOCK: Mutex<()> = Mutex::new(());
type WriteHook = Arc<dyn Fn(&Context, &MeteringData) -> Result<(), String> + Send + Sync>;
#[derive(Default)]
struct Writer {
    calls: Mutex<Vec<MeteringData>>,
    hook: Mutex<Option<WriteHook>>,
    closes: AtomicUsize,
    close_error: Mutex<Option<String>>,
}
impl MeteringWriter for Writer {
    fn write(&self, ctx: &Context, data: &MeteringData) -> Result<(), String> {
        self.calls.lock().unwrap().push(data.clone());
        let hook = self.hook.lock().unwrap().clone();
        hook.map_or(Ok(()), |hook| hook(ctx, data))
    }
    fn close(&self) -> Result<(), String> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        self.close_error.lock().unwrap().clone().map_or(Ok(()), Err)
    }
}
fn setup() -> (Arc<Meter>, Arc<Writer>) {
    let writer = Arc::new(Writer::default());
    let meter = Arc::new(Meter::new_meter_with_writer(Some(writer.clone())));
    set_metering(Some(meter.clone()));
    (meter, writer)
}
fn recorder(id: i64) -> Arc<Recorder> {
    register_recorder(&TaskBase {
        id,
        ..TaskBase::default()
    })
}
fn get(r: &Recorder, n: u64) {
    r.obj_store_access
        .requests
        .get
        .fetch_add(n, Ordering::SeqCst);
}
fn put(r: &Recorder, n: u64) {
    r.obj_store_access
        .requests
        .put
        .fetch_add(n, Ordering::SeqCst);
}
fn contains(meter: &Meter, id: i64) -> bool {
    meter.state.lock().unwrap().recorders.contains_key(&id)
}

/// Go `TestMeterRegisterUnregisterRecorderInClassic`.
#[test]
fn test_meter_register_unregister_recorder_in_classic() {
    if !tidb_config_kerneltype::is_classic() {
        return;
    }
    let _lock = TEST_LOCK.lock().unwrap();
    set_metering(None);
    recorder(1);
    assert!(METERING_INSTANCE.lock().unwrap().is_none());
    let (meter, _) = setup();
    recorder(1);
    assert!(meter.state.lock().unwrap().recorders.is_empty());
    set_metering(None);
}

/// Go `TestMeterRegisterUnregisterRecorder`, all five subtests.
#[test]
fn test_meter_register_unregister_recorder() {
    if tidb_config_kerneltype::is_classic() {
        return;
    }
    let _lock = TEST_LOCK.lock().unwrap();
    let ctx = Context::default();
    let (meter, writer) = setup();
    get(&recorder(1), 1);
    meter.flush(&ctx, 1_000_000);
    assert!(contains(&meter, 1));
    assert_eq!(writer.calls.lock().unwrap().len(), 1);

    let (meter, writer) = setup();
    get(&recorder(1), 1);
    unregister_recorder(1);
    assert!(contains(&meter, 1));
    meter.flush(&ctx, 1_000_000);
    assert!(!contains(&meter, 1));
    recorder(2);
    unregister_recorder(2);
    meter.flush(&ctx, 2_000_000);
    assert!(!contains(&meter, 2));
    assert_eq!(writer.calls.lock().unwrap().len(), 1);

    let (meter, writer) = setup();
    let r = recorder(1);
    get(&r, 1);
    *writer.hook.lock().unwrap() = Some(Arc::new(move |_, data| {
        assert_eq!(data.data[0][GET_REQUESTS_FIELD], 1);
        assert!(!data.data[0].contains_key(PUT_REQUESTS_FIELD));
        put(&r, 2);
        unregister_recorder(1);
        Ok(())
    }));
    meter.flush(&ctx, 1_000_000);
    assert!(contains(&meter, 1));
    *writer.hook.lock().unwrap() = Some(Arc::new(|_, data| {
        assert_eq!(data.data[0][PUT_REQUESTS_FIELD], 2);
        Ok(())
    }));
    meter.flush(&ctx, 2_000_000);
    assert!(!contains(&meter, 1));

    let (meter, writer) = setup();
    let r = recorder(1);
    get(&r, 1);
    unregister_recorder(1);
    *writer.hook.lock().unwrap() = Some(Arc::new(move |_, data| {
        assert_eq!(data.data[0][GET_REQUESTS_FIELD], 1);
        assert!(Arc::ptr_eq(&r, &recorder(1)));
        Ok(())
    }));
    meter.flush(&ctx, 1_000_000);
    assert!(contains(&meter, 1));
    assert!(!meter.state.lock().unwrap().recorders[&1].unregistered);

    let (meter, writer) = setup();
    recorder(1).inc_cluster_read_bytes(123_456_789);
    *writer.hook.lock().unwrap() = Some(Arc::new(|_, data| {
        assert_eq!(data.data[0][CLUSTER_READ_BYTES_FIELD], 123_456_789);
        unregister_recorder(1);
        Ok(())
    }));
    meter.flush(&ctx, 1_000_000);
    assert!(!contains(&meter, 1));
    assert!(
        !meter
            .state
            .lock()
            .unwrap()
            .last_flushed_data
            .contains_key(&1)
    );
    recorder(1).inc_cluster_read_bytes(123);
    *writer.hook.lock().unwrap() = Some(Arc::new(|_, data| {
        assert_eq!(data.data[0][CLUSTER_READ_BYTES_FIELD], 123);
        Ok(())
    }));
    meter.flush(&ctx, 2_000_000);
    assert!(contains(&meter, 1));
    unregister_recorder(1);
    meter.flush(&ctx, 3_000_000);
    assert!(!contains(&meter, 1));
    set_metering(None);
}

/// Go `TestMeterFlush`: normal delta writes and failed-write snapshots.
#[test]
fn test_meter_flush() {
    if tidb_config_kerneltype::is_classic() {
        return;
    }
    let _lock = TEST_LOCK.lock().unwrap();
    let ctx = Context::default();
    let (meter, writer) = setup();
    let r = recorder(1);
    get(&r, 1);
    put(&r, 2);
    r.obj_store_access.traffic.read.store(11, Ordering::SeqCst);
    r.obj_store_access.traffic.write.store(22, Ordering::SeqCst);
    r.inc_cluster_read_bytes(3);
    r.inc_cluster_write_bytes(4);
    meter.flush(&ctx, 1_000_000);
    let expected = Data {
        task_id: 1,
        values: DataValues {
            get_requests: 1,
            put_requests: 2,
            obj_store_read_bytes: 11,
            obj_store_write_bytes: 22,
            cluster_read_bytes: 3,
            cluster_write_bytes: 4,
        },
        ..Data::default()
    };
    assert_eq!(
        writer.calls.lock().unwrap()[0].data,
        [expected.cal_meter_data_item(&Data::default()).unwrap()]
    );
    meter.flush(&ctx, 2_000_000);
    assert_eq!(writer.calls.lock().unwrap().len(), 1);
    put(&r, 100);
    meter.flush(&ctx, 3_000_000);
    let data = writer.calls.lock().unwrap()[1].data[0].clone();
    assert_eq!(data[PUT_REQUESTS_FIELD], 100);
    assert!(!data.contains_key(GET_REQUESTS_FIELD));

    let (meter, writer) = setup();
    let r = recorder(1);
    get(&r, 1);
    *writer.hook.lock().unwrap() = Some(Arc::new(|_, _| Err("some err".into())));
    meter.flush(&ctx, 1_000_000);
    assert_eq!(
        meter.state.lock().unwrap().last_flushed_data[&1]
            .values
            .get_requests,
        1
    );
    assert!(
        meter
            .pending_retry_data
            .lock()
            .unwrap()
            .contains_key(&1_000_000)
    );
    *writer.hook.lock().unwrap() = None;
    get(&r, 3);
    meter.flush(&ctx, 2_000_000);
    assert_eq!(
        writer.calls.lock().unwrap()[1].data[0][GET_REQUESTS_FIELD],
        3
    );
    assert_eq!(
        meter.state.lock().unwrap().last_flushed_data[&1]
            .values
            .get_requests,
        4
    );
    set_metering(None);
}

/// Go `TestMeterRetryWrite`: ten permanent failures, one success, five retries.
#[test]
fn test_meter_retry_write() {
    if tidb_config_kerneltype::is_classic() {
        return;
    }
    let _lock = TEST_LOCK.lock().unwrap();
    let (meter, writer) = setup();
    let ctx = Context::default();
    meter.retry_write(&ctx);
    assert!(writer.calls.lock().unwrap().is_empty());
    for (ts, count) in [(1_000_000, 12), (2_000_000, 23), (3_000_000, 34)] {
        meter.pending_retry_data.lock().unwrap().insert(
            ts,
            Arc::new(WriteFailData {
                retry_cnt: AtomicUsize::new(0),
                items: vec![MeterItem::from([("get_requests".into(), count.into())])],
            }),
        );
    }
    let third = AtomicUsize::new(0);
    *writer.hook.lock().unwrap() = Some(Arc::new(move |_, data| {
        if data.timestamp == 1_000_000
            || (data.timestamp == 3_000_000 && third.fetch_add(1, Ordering::SeqCst) < 4)
        {
            Err("some err".into())
        } else {
            Ok(())
        }
    }));
    for count in 1..=10 {
        meter.retry_write(&ctx);
        let pending = meter.pending_retry_data.lock().unwrap();
        assert_eq!(
            pending.len(),
            if count < 5 {
                2
            } else if count == 10 {
                0
            } else {
                1
            }
        );
        if count < 10 {
            assert_eq!(pending[&1_000_000].retry_cnt.load(Ordering::SeqCst), count);
        }
        if count < 5 {
            assert_eq!(pending[&3_000_000].retry_cnt.load(Ordering::SeqCst), count);
        }
    }
    let calls = writer.calls.lock().unwrap();
    assert_eq!(calls.len(), 16);
    assert!(
        calls
            .iter()
            .all(|call| call.self_id == meter.uuid && call.category == "dxf")
    );
    assert!(!meter.uuid.contains('-'));
    set_metering(None);
}

/// Go `TestMeterStartFlushLoop`: drive Go's synctest schedule explicitly,
/// then exercise the real loops' cancellation and final writer close.
#[test]
fn test_meter_start_flush_loop() {
    if tidb_config_kerneltype::is_classic() {
        return;
    }
    let _lock = TEST_LOCK.lock().unwrap();
    let (meter, writer) = setup();
    let attempts = Mutex::new(BTreeMap::<i64, usize>::new());
    let written = Arc::new(Mutex::new(BTreeMap::<i64, u64>::new()));
    let lost = Arc::new(Mutex::new(0u64));
    let first_three = Mutex::new(None);
    let totals = written.clone();
    let lost_hook = lost.clone();
    *writer.hook.lock().unwrap() = Some(Arc::new(move |_, data| {
        let mut attempts = attempts.lock().unwrap();
        let count = attempts.entry(data.timestamp).or_default();
        let only_three = data.data.len() == 1 && data.data[0]["task_id"] == 3;
        let mut first_three = first_three.lock().unwrap();
        if only_three && first_three.is_none() {
            *first_three = Some(data.timestamp);
        }
        if *count < 5 {
            *count += 1;
            return Err("some err".into());
        }
        if only_three && *first_three == Some(data.timestamp) {
            *lost_hook.lock().unwrap() = data.data[0][GET_REQUESTS_FIELD].as_u64().unwrap();
            return Err("some err".into());
        }
        for item in &data.data {
            *totals
                .lock()
                .unwrap()
                .entry(item["task_id"].as_i64().unwrap())
                .or_default() += item[GET_REQUESTS_FIELD].as_u64().unwrap();
        }
        Ok(())
    }));
    let ctx = Context::default();
    let r1 = recorder(1);
    let r2 = recorder(2);
    let mut r3 = None;
    for second in (0..=3600).step_by(5) {
        if second < 2400 && second % 30 == 0 {
            get(&r1, 11);
            get(&r2, 33);
        }
        if second == 2400 {
            unregister_recorder(1);
            unregister_recorder(2);
        }
        if second == 3000 {
            r3 = Some(recorder(3));
        }
        if (3000..3100).contains(&second) && second % 10 == 0 {
            get(r3.as_ref().unwrap(), 23);
        }
        if second == 3100 {
            unregister_recorder(3);
        }
        if second > 0 && second % 60 == 0 {
            meter.flush(&ctx, second);
        }
        meter.retry_write(&ctx);
    }
    assert_eq!(r1.curr_data().values.get_requests, 880);
    assert_eq!(r2.curr_data().values.get_requests, 2640);
    assert_eq!(r3.unwrap().curr_data().values.get_requests, 230);
    let actual = written.lock().unwrap();
    assert_eq!(actual[&1], 880);
    assert_eq!(actual[&2], 2640);
    assert!(*lost.lock().unwrap() > 0);
    assert_eq!(actual[&3], 230 - *lost.lock().unwrap());
    assert!(meter.pending_retry_data.lock().unwrap().is_empty());
    let (ctx, cancel) = Context::with_cancel(&Context::default());
    cancel.cancel();
    meter.start_flush_loop(&ctx);
    assert_eq!(writer.closes.load(Ordering::SeqCst), 1);
    set_metering(None);
}

/// Go `TestMeterClose`.
#[test]
fn test_meter_close() {
    let _lock = TEST_LOCK.lock().unwrap();
    let (meter, writer) = setup();
    meter.close().unwrap();
    assert_eq!(writer.closes.load(Ordering::SeqCst), 1);
    *writer.close_error.lock().unwrap() = Some("some err".into());
    assert_eq!(meter.close().unwrap_err(), "some err");
    set_metering(None);
}
