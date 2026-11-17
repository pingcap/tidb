//! Go `pkg/objstore/hdfs.go`: writes through the `hdfs dfs` command; the
//! other operations are unsupported, as in Go.

use std::io::Write;
use std::process::{Command, Stdio};
use std::time::Duration;

use tidb_gocontext::Context;

use crate::error::{berrors, Error, Result};
use crate::objectio::{Reader, Writer};
use crate::storeapi::{ReaderOption, Storage, WalkOption, WriterOption};

/// Go `HDFSStorage`.
#[derive(Clone, Debug)]
pub struct HdfsStorage {
    remote: String,
}

impl HdfsStorage {
    /// Go `NewHDFSStorage`.
    #[must_use]
    pub fn new(remote: &str) -> Self {
        Self { remote: remote.to_owned() }
    }
}

/// Go `dfsCommand`.
fn dfs_command(args: &[&str]) -> Result<Command> {
    let hadoop_home = std::env::var("HADOOP_HOME").map_err(|_| {
        berrors::annotate(
            "[BR:Common:ErrEnvNotSpecified]environment variable not found",
            "please specify environment variable HADOOP_HOME",
        )
    })?;
    let bin = crate::gopath::join(&[&hadoop_home, "bin/hdfs"]);
    let mut cmd = match std::env::var("HADOOP_LINUX_USER") {
        Ok(user) => {
            let mut c = Command::new("sudo");
            c.args(["-u", &user, &bin]);
            c
        }
        Err(_) => Command::new(&bin),
    };
    cmd.arg("dfs").args(args);
    Ok(cmd)
}

fn unsupported() -> Error {
    berrors::annotate(berrors::ERR_UNSUPPORTED_OPERATION, "currently HDFS backend only support rawkv backup")
}

impl Storage for HdfsStorage {
    fn write_file(&self, _ctx: &Context, name: &str, data: &[u8]) -> Result<()> {
        let file_path = format!("{}/{name}", self.remote);
        let mut child = dfs_command(&["-put", "-", &file_path])?
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(Error::from)?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(data).map_err(Error::from)?;
        }
        let out = child.wait_with_output().map_err(Error::from)?;
        if !out.status.success() {
            let mut text = out.stdout;
            text.extend_from_slice(&out.stderr);
            return Err(Error::other(format!("{}: {}", String::from_utf8_lossy(&text), out.status)));
        }
        Ok(())
    }

    fn read_file(&self, _: &Context, _: &str) -> Result<Vec<u8>> {
        Err(unsupported())
    }

    fn file_exists(&self, _ctx: &Context, name: &str) -> Result<bool> {
        let file_path = format!("{}/{name}", self.remote);
        let out = dfs_command(&["-ls", &file_path])?.output().map_err(Error::from)?;
        Ok(out.status.success())
    }

    fn delete_file(&self, _: &Context, _: &str) -> Result<()> {
        Err(unsupported())
    }

    fn delete_files(&self, _: &Context, _: &[String]) -> Result<()> {
        Err(unsupported())
    }

    fn open(&self, _: &Context, _: &str, _: Option<&ReaderOption>) -> Result<Box<dyn Reader>> {
        Err(unsupported())
    }

    fn walk_dir(&self, _: &Context, _: Option<&WalkOption>, _: &mut dyn FnMut(&str, i64) -> Result<()>) -> Result<()> {
        Err(unsupported())
    }

    fn uri(&self) -> String {
        self.remote.clone()
    }

    fn create(&self, _: &Context, _: &str, _: Option<&WriterOption>) -> Result<Box<dyn Writer>> {
        Err(unsupported())
    }

    fn rename(&self, _: &Context, _: &str, _: &str) -> Result<()> {
        Err(unsupported())
    }

    fn presign_file(&self, _: &Context, _: &str, _: Duration) -> Result<String> {
        Err(Error::other("HDFS backend does not support PresignFile"))
    }
}
