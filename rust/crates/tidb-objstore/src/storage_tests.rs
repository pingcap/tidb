//! Go `local_test.go`, `memstore_test.go`, `compress_test.go` and
//! `storage_test.go`, for the storages ported here.

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;

use tidb_gocontext::Context;

use crate::compressedio::{self, CompressType, DecompressConfig};
use crate::storeapi::{ReaderOption, Storage, WalkOption};

fn local(dir: &std::path::Path) -> Arc<dyn Storage> {
    let backend = crate::parse_backend(&format!("file://{}", dir.display()), None).unwrap();
    crate::create(&Context::background(), &backend, true).unwrap()
}

fn walk(store: &dyn Storage, opt: &WalkOption) -> Vec<(String, i64)> {
    let mut files = Vec::new();
    store
        .walk_dir(&Context::background(), Some(opt), &mut |path, size| {
            files.push((path.to_owned(), size));
            Ok(())
        })
        .unwrap();
    files
}

#[test]
fn test_delete_file() {
    let dir = tempfile::tempdir().unwrap();
    let store = local(dir.path());
    let ctx = Context::background();
    assert!(!store.file_exists(&ctx, "test_delete").unwrap());
    store.create(&ctx, "test_delete", None).unwrap();
    assert!(store.file_exists(&ctx, "test_delete").unwrap());
    store.delete_file(&ctx, "test_delete").unwrap();
    assert!(!store.file_exists(&ctx, "test_delete").unwrap());
}

#[test]
fn test_walk_dir_skip_sub_dir() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("test1.txt"), "test1").unwrap();
    std::fs::create_dir_all(dir.path().join("sub")).unwrap();
    std::fs::write(dir.path().join("sub/test2.txt"), "test2").unwrap();
    let store = local(dir.path());
    let names: Vec<String> = walk(store.as_ref(), &WalkOption::default()).into_iter().map(|f| f.0).collect();
    assert_eq!(names, ["sub/test2.txt", "test1.txt"]);
    let names: Vec<String> = walk(store.as_ref(), &WalkOption { skip_sub_dir: true, ..WalkOption::default() })
        .into_iter()
        .map(|f| f.0)
        .collect();
    assert_eq!(names, ["test1.txt"]);
}

#[test]
fn test_walk_dir_start_after() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("meta")).unwrap();
    for (name, data) in [("0001.meta", "1"), ("0002.meta", "2"), ("0003.meta", "3")] {
        std::fs::write(dir.path().join("meta").join(name), data).unwrap();
    }
    let store = local(dir.path());
    let opt = WalkOption { sub_dir: "meta".into(), start_after: "meta/0001.meta".into(), ..WalkOption::default() };
    let names: Vec<String> = walk(store.as_ref(), &opt).into_iter().map(|f| f.0).collect();
    assert_eq!(names, ["meta/0002.meta", "meta/0003.meta"]);
}

#[test]
fn test_local_uri() {
    let url = "file:///tmp/folder";
    let backend = crate::parse_backend(url, None).unwrap();
    let store = crate::create(&Context::background(), &backend, true).unwrap();
    assert_eq!(store.uri(), url);
}

#[test]
fn test_local_file_read_range() {
    let dir = tempfile::tempdir().unwrap();
    let store = local(dir.path());
    let ctx = Context::background();
    let mut w = store.create(&ctx, "test_read_range", None).unwrap();
    w.write(&ctx, b"0123456789").unwrap();
    w.close(&ctx).unwrap();
    let read = |start: Option<i64>, end: Option<i64>| {
        let mut r = store
            .open(&ctx, "test_read_range", Some(&ReaderOption { start_offset: start, end_offset: end, prefetch_size: 0 }))
            .unwrap();
        let mut buf = [0u8; 10];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(r.read(&mut buf).unwrap(), 0);
        String::from_utf8(buf[..n].to_vec()).unwrap()
    };
    assert_eq!(read(Some(2), Some(6)), "2345");
    assert_eq!(read(None, None), "0123456789");
    assert_eq!(read(Some(5), None), "56789");
    assert_eq!(read(None, Some(5)), "01234");
    let mut r = store
        .open(&ctx, "test_read_range", Some(&ReaderOption { start_offset: Some(2), end_offset: Some(6), prefetch_size: 0 }))
        .unwrap();
    let mut small = [0u8; 2];
    let n = r.read(&mut small).unwrap();
    assert_eq!(&small[..n], b"23");
}

#[test]
fn test_walk_broken_sym_link() {
    let dir = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(dir.path().join("non-existing-file"), dir.path().join("file-that-should-be-ignored")).unwrap();
    let store = local(dir.path());
    assert_eq!(walk(store.as_ref(), &WalkOption::default()), [("file-that-should-be-ignored".to_owned(), 0)]);
}

#[test]
fn test_mem_store_basic() {
    let store = crate::MemStorage::new();
    let ctx = Context::background();
    assert!(!store.file_exists(&ctx, "/hello.txt").unwrap());
    assert_eq!(store.read_file(&ctx, "/hello.txt").unwrap_err().to_string(), "cannot find the file: /hello.txt");
    store.write_file(&ctx, "/hello.txt", b"hello world").unwrap();
    assert_eq!(store.read_file(&ctx, "/hello.txt").unwrap(), b"hello world");
    store.rename(&ctx, "/hello.txt", "/hello2.txt").unwrap();
    assert!(!store.file_exists(&ctx, "/hello.txt").unwrap());
    let mut w = store.create(&ctx, "/new.txt", None).unwrap();
    w.write(&ctx, b"aa").unwrap();
    assert_eq!(store.read_file(&ctx, "/new.txt").unwrap(), b"");
    w.close(&ctx).unwrap();
    assert_eq!(store.read_file(&ctx, "/new.txt").unwrap(), b"aa");
    store.delete_file(&ctx, "/new.txt").unwrap();
    assert!(store.delete_file(&ctx, "/new.txt").is_err());
    let (cancel_ctx, cancel) = Context::with_cancel(&ctx);
    cancel.cancel();
    assert!(store.read_file(&cancel_ctx, "/hello2.txt").is_err());
}

#[test]
fn test_mem_store_open_range_seek() {
    use std::io::{Seek, SeekFrom};
    let store = crate::MemStorage::new();
    let ctx = Context::background();
    store.write_file(&ctx, "/a", b"0123456789").unwrap();
    let mut r = store.open(&ctx, "/a", Some(&ReaderOption { start_offset: Some(3), end_offset: Some(6), prefetch_size: 0 })).unwrap();
    let mut out = String::new();
    r.read_to_string(&mut out).unwrap();
    assert_eq!(out, "345");
    let mut r = store.open(&ctx, "/a", None).unwrap();
    r.seek(SeekFrom::Start(7)).unwrap();
    out.clear();
    r.read_to_string(&mut out).unwrap();
    assert_eq!(out, "789");
    assert_eq!(r.get_file_size().unwrap(), 10);
}

#[test]
fn test_mem_store_walk_dir() {
    let store = crate::MemStorage::new();
    let ctx = Context::background();
    let files: BTreeMap<&str, &str> = [
        ("/hello.txt", "hello world"),
        ("/hello2.txt", "hello world 2"),
        ("/aaa/hello.txt", "aaa: hello world"),
        ("/aaa/world.txt", "aaa: world"),
        ("/dummy.txt", "dummy"),
    ]
    .into();
    for (name, content) in &files {
        store.write_file(&ctx, name, content.as_bytes()).unwrap();
    }
    let names = |opt: WalkOption| -> Vec<String> { walk(&store, &opt).into_iter().map(|f| f.0).collect() };
    assert_eq!(names(WalkOption::default()).len(), files.len());
    assert_eq!(names(WalkOption { sub_dir: "/aaa".into(), ..WalkOption::default() }), ["/aaa/hello.txt", "/aaa/world.txt"]);
    assert_eq!(
        names(WalkOption { obj_prefix: "hello".into(), ..WalkOption::default() }),
        ["/aaa/hello.txt", "/hello.txt", "/hello2.txt"]
    );
}

#[test]
fn test_with_compress_read_write_file() {
    let dir = tempfile::tempdir().unwrap();
    let backend = crate::parse_backend(&format!("local://{}", dir.path().display()), None).unwrap();
    let ctx = Context::background();
    let storage = crate::compress::with_compression(
        crate::create(&ctx, &backend, true).unwrap(),
        CompressType::Gzip,
        DecompressConfig::default(),
    );
    let file_name = "with-compress-test.txt.gz";
    storage.write_file(&ctx, file_name, b"hello,world!").unwrap();
    let file = std::fs::File::open(dir.path().join(file_name)).unwrap();
    let mut content = String::new();
    compressedio::new_reader(CompressType::Gzip, DecompressConfig::default(), file)
        .unwrap()
        .unwrap()
        .read_to_string(&mut content)
        .unwrap();
    assert_eq!(content, "hello,world!");
    assert_eq!(storage.read_file(&ctx, file_name).unwrap(), b"hello,world!");
    // Go `TestCompressReaderWriter`: a compressed writer read back through
    // the decompressing reader.
    let mut w = storage.create(&ctx, "w.txt.gz", None).unwrap();
    w.write(&ctx, b"abc").unwrap();
    w.write(&ctx, b"def").unwrap();
    w.close(&ctx).unwrap();
    let mut r = storage.open(&ctx, "w.txt.gz", None).unwrap();
    content.clear();
    r.read_to_string(&mut content).unwrap();
    assert_eq!(content, "abcdef");
    assert!(r.get_file_size().unwrap_err().to_string().contains("compressReader doesn't support GetFileSize now"));
}

#[test]
fn test_new_mem_storage() {
    let store = crate::new_from_url(&Context::background(), "memstore://").unwrap();
    assert_eq!(store.uri(), "memstore://");
}

/// A storage against a live S3 endpoint. Set `TIDB_TEST_S3_URL` (for example
/// `s3://bucket/prefix?endpoint=http://127.0.0.1:9000&access-key=..&secret-access-key=..`
/// against MinIO) to run it.
#[test]
#[ignore = "needs an S3 endpoint in TIDB_TEST_S3_URL"]
fn s3_storage_round_trip() {
    let url = std::env::var("TIDB_TEST_S3_URL").expect("TIDB_TEST_S3_URL");
    let ctx = Context::background();
    let store = crate::new_from_url(&ctx, &url).unwrap();
    store.write_file(&ctx, "dir/a.csv", b"1,2\n").unwrap();
    store.write_file(&ctx, "dir/b.csv", b"3,4\n5,6\n").unwrap();
    store.write_file(&ctx, "other.csv", b"x").unwrap();
    let mut w = store.create(&ctx, "dir/c.csv", None).unwrap();
    w.write(&ctx, b"7,8\n").unwrap();
    w.close(&ctx).unwrap();
    assert_eq!(
        walk(store.as_ref(), &WalkOption { sub_dir: "dir".into(), ..WalkOption::default() }),
        [("dir/a.csv".to_owned(), 4), ("dir/b.csv".to_owned(), 8), ("dir/c.csv".to_owned(), 4)]
    );
    assert_eq!(
        walk(store.as_ref(), &WalkOption { sub_dir: "dir".into(), obj_prefix: "b".into(), ..WalkOption::default() }),
        [("dir/b.csv".to_owned(), 8)]
    );
    assert_eq!(
        walk(store.as_ref(), &WalkOption { start_after: "dir/a.csv".into(), ..WalkOption::default() })
            .into_iter()
            .map(|f| f.0)
            .collect::<Vec<_>>(),
        ["dir/b.csv", "dir/c.csv", "other.csv"]
    );
    let mut buf = [0u8; 4];
    crate::read_data_in_range(&ctx, store.as_ref(), "dir/b.csv", 4, &mut buf).unwrap();
    assert_eq!(&buf, b"5,6\n");
    assert!(store.file_exists(&ctx, "dir/a.csv").unwrap());
    store.delete_files(&ctx, &["dir/a.csv".into(), "dir/b.csv".into(), "dir/c.csv".into(), "other.csv".into()]).unwrap();
    assert!(!store.file_exists(&ctx, "dir/a.csv").unwrap());
    assert!(store.open(&ctx, "missing", None).is_err());
}
