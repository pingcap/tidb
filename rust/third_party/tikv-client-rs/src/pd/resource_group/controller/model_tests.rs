// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `model_test.go`.

use std::borrow::Cow;
use std::sync::Arc;

use super::config::{default_ru_config, DEFAULT_AVG_BATCH_PROPORTION};
use super::model::{
    add, equal_ru, get_ru_value_from_consumption, new_kv_calculator, paging_read_estimate,
    reported_request_consumption, reported_response_consumption, sub, update_delta_consumption,
    AccessLocationType, RequestInfo, ResourceCalculator,
};
use super::testutil::{TestRequestInfo, TestResponseInfo};
use crate::proto::resource_manager::Consumption;

/// A request info implementing none of the optional providers.
#[derive(Default)]
struct LegacyRequestInfo {
    is_write: bool,
}

impl RequestInfo for LegacyRequestInfo {
    fn is_write(&self) -> bool {
        self.is_write
    }
    fn write_bytes(&self) -> u64 {
        0
    }
    fn replica_number(&self) -> i64 {
        0
    }
    fn store_id(&self) -> u64 {
        0
    }
    fn request_size(&self) -> u64 {
        0
    }
    fn access_location_type(&self) -> AccessLocationType {
        AccessLocationType::AccessUnknown
    }
}

/// A cop request info without the prediction provider.
#[derive(Default)]
struct CopRequestInfoWithoutPrediction {
    legacy: LegacyRequestInfo,
}

impl RequestInfo for CopRequestInfoWithoutPrediction {
    fn is_write(&self) -> bool {
        self.legacy.is_write()
    }
    fn write_bytes(&self) -> u64 {
        self.legacy.write_bytes()
    }
    fn replica_number(&self) -> i64 {
        self.legacy.replica_number()
    }
    fn store_id(&self) -> u64 {
        self.legacy.store_id()
    }
    fn request_size(&self) -> u64 {
        self.legacy.request_size()
    }
    fn access_location_type(&self) -> AccessLocationType {
        self.legacy.access_location_type()
    }
    fn is_cop(&self) -> bool {
        true
    }
}

fn base_read_cost() -> f64 {
    let cfg = default_ru_config();
    cfg.read_base_cost + cfg.read_per_batch_base_cost * DEFAULT_AVG_BATCH_PROPORTION
}

fn assert_in_delta(expected: f64, actual: f64, delta: f64) {
    assert!(
        (expected - actual).abs() <= delta,
        "expected {expected}, actual {actual}"
    );
}

#[test]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_model_test_TestGetRUValueFromConsumption() {
    let custom = Consumption {
        r_r_u: 2.5,
        w_r_u: 3.5,
        ..Consumption::default()
    };
    assert_eq!(get_ru_value_from_consumption(Some(&custom)), 6.0);
    assert_eq!(get_ru_value_from_consumption(None), 0.0);
}

#[test]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_model_test_TestAdd() {
    let mut custom1 = Consumption {
        r_r_u: 2.5,
        w_r_u: 3.5,
        read_cross_az_traffic_bytes: 10,
        write_cross_az_traffic_bytes: 20,
        tikv_r_u_v2: 1.0,
        tidb_r_u_v2: 2.0,
        tiflash_r_u_v2: 3.0,
        ..Consumption::default()
    };
    let custom2 = Consumption {
        r_r_u: 1.5,
        w_r_u: 2.5,
        read_cross_az_traffic_bytes: 30,
        write_cross_az_traffic_bytes: 40,
        tikv_r_u_v2: 3.0,
        tidb_r_u_v2: 4.0,
        tiflash_r_u_v2: 5.0,
        ..Consumption::default()
    };
    let expected = Consumption {
        r_r_u: 4.0,
        w_r_u: 6.0,
        read_cross_az_traffic_bytes: 40,
        write_cross_az_traffic_bytes: 60,
        tikv_r_u_v2: 4.0,
        tidb_r_u_v2: 6.0,
        tiflash_r_u_v2: 8.0,
        ..Consumption::default()
    };
    add(Some(&mut custom1), Some(&custom2));
    assert_eq!(custom1, expected);

    // A nil first operand is left nil.
    let custom2 = Consumption {
        r_r_u: 1.5,
        w_r_u: 2.5,
        ..Consumption::default()
    };
    add(None, Some(&custom2));

    // A nil second operand changes nothing.
    let mut custom1 = Consumption {
        r_r_u: 2.5,
        w_r_u: 3.5,
        ..Consumption::default()
    };
    add(Some(&mut custom1), None);
    assert_eq!(
        custom1,
        Consumption {
            r_r_u: 2.5,
            w_r_u: 3.5,
            ..Consumption::default()
        }
    );
}

#[test]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_model_test_TestSub() {
    let mut custom1 = Consumption {
        r_r_u: 2.5,
        w_r_u: 3.5,
        read_cross_az_traffic_bytes: 5,
        write_cross_az_traffic_bytes: 10,
        tikv_r_u_v2: 7.0,
        tidb_r_u_v2: 9.0,
        tiflash_r_u_v2: 11.0,
        ..Consumption::default()
    };
    let custom2 = Consumption {
        r_r_u: 1.5,
        w_r_u: 2.5,
        read_cross_az_traffic_bytes: 1,
        write_cross_az_traffic_bytes: 2,
        tikv_r_u_v2: 3.0,
        tidb_r_u_v2: 4.0,
        tiflash_r_u_v2: 5.0,
        ..Consumption::default()
    };
    let expected = Consumption {
        r_r_u: 1.0,
        w_r_u: 1.0,
        read_cross_az_traffic_bytes: 4,
        write_cross_az_traffic_bytes: 8,
        tikv_r_u_v2: 4.0,
        tidb_r_u_v2: 5.0,
        tiflash_r_u_v2: 6.0,
        ..Consumption::default()
    };
    sub(Some(&mut custom1), Some(&custom2));
    assert_eq!(custom1, expected);

    let custom2 = Consumption {
        r_r_u: 1.5,
        w_r_u: 2.55,
        read_cross_az_traffic_bytes: 1,
        write_cross_az_traffic_bytes: 2,
        ..Consumption::default()
    };
    sub(None, Some(&custom2));

    let mut custom1 = Consumption {
        r_r_u: 2.5,
        w_r_u: 3.5,
        ..Consumption::default()
    };
    sub(Some(&mut custom1), None);
    assert_eq!(
        custom1,
        Consumption {
            r_r_u: 2.5,
            w_r_u: 3.5,
            ..Consumption::default()
        }
    );
}

#[test]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_model_test_TestUpdateDeltaConsumption() {
    let mut last = Consumption {
        tikv_r_u_v2: 2.0,
        tidb_r_u_v2: 3.0,
        tiflash_r_u_v2: 5.0,
        ..Consumption::default()
    };
    let now = Consumption {
        tikv_r_u_v2: 5.0,
        tidb_r_u_v2: 11.0,
        tiflash_r_u_v2: 18.0,
        ..Consumption::default()
    };
    let delta = update_delta_consumption(&mut last, &now);
    assert_eq!(
        delta,
        Consumption {
            tikv_r_u_v2: 3.0,
            tidb_r_u_v2: 8.0,
            tiflash_r_u_v2: 13.0,
            ..Consumption::default()
        }
    );
    assert_eq!(now.tikv_r_u_v2, last.tikv_r_u_v2);
    assert_eq!(now.tidb_r_u_v2, last.tidb_r_u_v2);
    assert_eq!(now.tiflash_r_u_v2, last.tiflash_r_u_v2);
}

#[test]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_model_test_TestUpdateDeltaConsumptionReportsEmptyConsumption(
) {
    let mut last = Consumption {
        r_r_u: 10.0,
        w_r_u: 20.0,
        read_bytes: 30.0,
        ..Consumption::default()
    };
    let now = last.clone();
    let delta = update_delta_consumption(&mut last, &now);
    assert_eq!(delta, Consumption::default());
    assert_eq!(now, last);
}

#[test]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_model_test_TestUpdateDeltaConsumptionIgnoresDecreasedRequestUnits(
) {
    let mut last = Consumption {
        r_r_u: 10.0,
        w_r_u: 20.0,
        read_bytes: 100.0,
        write_bytes: 200.0,
        total_cpu_time_ms: 300.0,
        sql_layer_cpu_time_ms: 400.0,
        kv_read_rpc_count: 500.0,
        kv_write_rpc_count: 600.0,
        read_cross_az_traffic_bytes: 700,
        write_cross_az_traffic_bytes: 800,
        tikv_r_u_v2: 900.0,
        tidb_r_u_v2: 1000.0,
        tiflash_r_u_v2: 1100.0,
    };
    let now = Consumption {
        r_r_u: 7.0,
        w_r_u: 15.0,
        read_bytes: 90.0,
        write_bytes: 190.0,
        total_cpu_time_ms: 290.0,
        sql_layer_cpu_time_ms: 390.0,
        kv_read_rpc_count: 490.0,
        kv_write_rpc_count: 590.0,
        read_cross_az_traffic_bytes: 690,
        write_cross_az_traffic_bytes: 790,
        tikv_r_u_v2: 890.0,
        tidb_r_u_v2: 990.0,
        tiflash_r_u_v2: 1090.0,
    };
    let delta = update_delta_consumption(&mut last, &now);
    assert_eq!(delta, Consumption::default());
    assert_eq!(last.r_r_u, 10.0);
    assert_eq!(last.w_r_u, 20.0);
    assert_eq!(last.read_bytes, 100.0);
    assert_eq!(last.write_bytes, 200.0);
    assert_eq!(last.total_cpu_time_ms, 300.0);
    assert_eq!(last.sql_layer_cpu_time_ms, 400.0);
    assert_eq!(last.kv_read_rpc_count, 500.0);
    assert_eq!(last.kv_write_rpc_count, 600.0);
    assert_eq!(last.read_cross_az_traffic_bytes, 700);
    assert_eq!(last.write_cross_az_traffic_bytes, 800);
    assert_eq!(last.tikv_r_u_v2, 900.0);
    assert_eq!(last.tidb_r_u_v2, 1000.0);
    assert_eq!(last.tiflash_r_u_v2, 1100.0);
}

#[test]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_model_test_TestRequestInfoPagingProvidersAreOptional() {
    let cfg = Arc::new(default_ru_config());
    let kv_calc = new_kv_calculator(cfg.clone());
    let req = LegacyRequestInfo::default();

    let (bytes_for_est, ok) = paging_read_estimate(&req);
    assert!(!ok);
    assert_eq!(bytes_for_est, 0);

    let mut consumption = Consumption::default();
    kv_calc.before_kv_request(&mut consumption, &req);
    assert_in_delta(base_read_cost(), consumption.r_r_u, 1e-6);

    let resp = TestResponseInfo {
        read_bytes: 1024,
        succeed: true,
        ..TestResponseInfo::default()
    };
    let mut settle = Consumption::default();
    kv_calc.after_kv_request(&mut settle, &req, &resp);
    assert_in_delta(cfg.read_bytes_cost * 1024.0, settle.r_r_u, 1e-6);
}

#[test]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_model_test_TestRequestInfoMissingPredictionProviderReturnsZeroHint(
) {
    let req = CopRequestInfoWithoutPrediction::default();
    let (bytes_for_est, ok) = paging_read_estimate(&req);
    assert!(ok);
    assert_eq!(bytes_for_est, 0);
}

#[test]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_model_test_TestReportedConsumptionStripsPagingPrecharge() {
    let cfg = Arc::new(default_ru_config());
    let calculators: Vec<Box<dyn ResourceCalculator>> =
        vec![Box::new(new_kv_calculator(cfg.clone()))];
    let req = TestRequestInfo {
        is_write: false,
        is_cop: true,
        predicted_read_bytes: 1024,
        ..TestRequestInfo::default()
    };
    let token_delta = Consumption {
        r_r_u: base_read_cost() + cfg.read_bytes_cost * 1024.0,
        ..Consumption::default()
    };
    let reported = reported_request_consumption(&calculators, &req, &token_delta);
    assert_in_delta(base_read_cost(), reported.r_r_u, 1e-6);
    assert_in_delta(
        token_delta.r_r_u,
        reported.r_r_u + cfg.read_bytes_cost * 1024.0,
        1e-6,
    );
}

#[test]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_model_test_TestReportedConsumptionRestoresPagingActualUsage()
{
    let cfg = Arc::new(default_ru_config());
    let calculators: Vec<Box<dyn ResourceCalculator>> =
        vec![Box::new(new_kv_calculator(cfg.clone()))];
    let req = TestRequestInfo {
        is_write: false,
        is_cop: true,
        predicted_read_bytes: 4096,
        ..TestRequestInfo::default()
    };
    let actual_read_bytes = 1024u64;
    let token_delta = Consumption {
        r_r_u: cfg.read_bytes_cost * (actual_read_bytes as f64 - req.predicted_read_bytes as f64),
        read_bytes: actual_read_bytes as f64,
        ..Consumption::default()
    };
    let reported = reported_response_consumption(&calculators, &req, &token_delta);
    assert_in_delta(
        cfg.read_bytes_cost * actual_read_bytes as f64,
        reported.r_r_u,
        1e-6,
    );
    assert_eq!(reported.read_bytes, actual_read_bytes as f64);
    assert!(token_delta.r_r_u < 0.0);
    assert!(reported.r_r_u >= 0.0);
}

#[test]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_model_test_TestReportedConsumptionReusesDeltaWithoutPagingPrecharge(
) {
    let calculators: Vec<Box<dyn ResourceCalculator>> =
        vec![Box::new(new_kv_calculator(Arc::new(default_ru_config())))];
    let req = TestRequestInfo {
        is_write: false,
        is_cop: false,
        predicted_read_bytes: 1024,
        ..TestRequestInfo::default()
    };
    let token_delta = Consumption {
        r_r_u: 1.0,
        read_bytes: 64.0,
        ..Consumption::default()
    };
    let reported_request = reported_request_consumption(&calculators, &req, &token_delta);
    let reported_response = reported_response_consumption(&calculators, &req, &token_delta);
    assert!(
        matches!(reported_request, Cow::Borrowed(reported) if std::ptr::eq(reported, &token_delta))
    );
    assert!(
        matches!(reported_response, Cow::Borrowed(reported) if std::ptr::eq(reported, &token_delta))
    );
}

#[test]
#[allow(non_snake_case)]
fn source_go_resource_group_controller_model_test_TestEqualRU() {
    let consumption = |r_r_u, w_r_u, tikv_r_u_v2, tidb_r_u_v2| Consumption {
        r_r_u,
        w_r_u,
        tikv_r_u_v2,
        tidb_r_u_v2,
        ..Consumption::default()
    };
    assert!(equal_ru(
        &consumption(1.0, 2.0, 3.0, 4.0),
        &consumption(1.0, 2.0, 3.0, 4.0)
    ));
    assert!(!equal_ru(
        &consumption(1.0, 2.0, 3.0, 4.0),
        &consumption(1.0, 2.0, 5.0, 4.0)
    ));
    assert!(!equal_ru(
        &consumption(1.0, 2.0, 3.0, 4.0),
        &consumption(1.0, 2.0, 3.0, 6.0)
    ));
}
