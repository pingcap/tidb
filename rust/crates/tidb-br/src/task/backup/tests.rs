//! Go `br/pkg/task` backup tests, and the Go capture of `BackupConfig.Hash`
//! (`zzprobe/e07task`, `testdata/task_go.jsonl`).

use serde_json::Value as Json;
use tidb_proto::backup::CompressionType;
use tidb_proto::encryptionpb::EncryptionMethod;

use super::*;

fn go_hashes() -> Vec<Json> {
    include_str!("../../../testdata/task_go.jsonl")
        .lines()
        .map(|l| serde_json::from_str::<Json>(l).unwrap())
        .collect()
}

/// Go `TestDefaultBackup` and `DefaultBackupConfig`'s flag defaults (the
/// values Go master's `json.Marshal` of the default config shows).
#[test]
fn default_backup() {
    let cfg = default_backup_config(crate::task::common::default_config());
    assert_eq!((cfg.backup_ts, cfg.last_backup_ts, cfg.time_ago), (0, 0, 0));
    assert!(
        cfg.ignore_stats && cfg.use_backup_meta_v2 && cfg.use_checkpoint && !cfg.remove_schedulers
    );
    assert_eq!(
        (cfg.gc_ttl, cfg.range_limit, cfg.table_concurrency),
        (300, 30_000_000, 64)
    );
    assert_eq!(
        cfg.compression_config.compression_type,
        CompressionType::Zstd
    );
    assert_eq!(cfg.compression_config.compression_level, 0);
    assert!(cfg.config.check_requirements && !cfg.config.no_creds && !cfg.config.log_progress);
    assert_eq!(cfg.config.grpc_keepalive_time, Duration::from_secs(10));
    assert_eq!(cfg.config.grpc_keepalive_timeout, Duration::from_secs(3));
    assert_eq!(cfg.config.metadata_download_batch_size, 128);
    assert_eq!(cfg.config.concurrency, 0);
}

/// Go `BackupConfig.Adjust`: the default concurrency, its cap, the rate
/// limit forcing one request at a time, and the GC TTL.
#[test]
fn adjust_backup_config() {
    let mut cfg = default_backup_config(crate::task::common::default_config());
    cfg.gc_ttl = 0;
    cfg.config.checksum_concurrency = 0;
    cfg.compression_config.compression_type = CompressionType::Unknown;
    cfg.adjust();
    assert_eq!(cfg.config.concurrency, 4);
    assert_eq!(cfg.gc_ttl, 300);
    assert_eq!(cfg.config.checksum_concurrency, 4);
    assert_eq!(
        cfg.compression_config.compression_type,
        CompressionType::Zstd
    );
    cfg.config.concurrency = 1000;
    cfg.adjust();
    assert_eq!(cfg.config.concurrency, 256);
    cfg.config.concurrency = 8;
    cfg.config.rate_limit = 1 << 20;
    cfg.adjust();
    assert_eq!(cfg.config.concurrency, 1);
}

/// Go `BackupConfig.Hash` over the captured cases: the same JSON and so the
/// same checkpoint hash as Go.
#[test]
fn hash_matches_go() {
    let cases = go_hashes();
    assert_eq!(cases.len(), 3);
    for case in cases {
        let mut cfg = default_backup_config(crate::task::common::default_config());
        match case["case"].as_i64().unwrap() {
            0 => {
                cfg.config.storage = "local:///tmp/e07/bk1".into();
            }
            1 => {
                cfg.config.storage = "local:///tmp/e07/bk2".into();
                cfg.config.filter_str = vec!["*.*".into(), "`test`.*".into()];
                cfg.last_backup_ts = 446_012_345_678_901_248;
                cfg.ignore_stats = false;
                cfg.config.cipher_info.cipher_type = EncryptionMethod::Aes256Ctr as i32;
                cfg.config.cipher_info.cipher_key = b"0123456789abcdef0123456789abcdef".to_vec();
                cfg.config.pd = vec!["127.0.0.1:44379".into(), "127.0.0.1:44380".into()];
                cfg.config.send_creds = false;
            }
            2 => {
                cfg.config.storage = "s3://bucket/prefix".into();
                cfg.config.filter_str =
                    vec!["*.*".into(), "`test`.`t1`".into(), "`db``x`.`t\"2`".into()];
                let s3 = &mut cfg.config.backend_options.s3;
                s3.endpoint = "http://127.0.0.1:9000".into();
                s3.access_key = "ak".into();
                s3.secret_access_key = "sk".into();
                s3.region = "us-east-1".into();
            }
            _ => unreachable!(),
        }
        assert_eq!(
            cfg.immutable_json(),
            case["json"].as_str().unwrap(),
            "case {}",
            case["case"]
        );
        assert_eq!(hex::encode(cfg.hash()), case["hash"].as_str().unwrap());
    }
}
