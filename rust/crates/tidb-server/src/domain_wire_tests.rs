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

//! Wire-level checks of the session API's error verdicts: what the MySQL front
//! end writes (or refuses to write) for an error the domain produced.

#[cfg(test)]
mod tests {
    use crate::connection_writers::{write_query_error, ConnectionPacketOutput};
    use crate::mysql_connection::MysqlConnectionError;
    use tidb_exec::real_tikv_ddl::ClusterDdlError;
    use tidb_exec::real_tikv_dml::ConfiguredWriteError;
    use tidb_session::domain::session_api::{cluster_ddl_error, configured_write_error};
    use tidb_txnkv::retry::RegionBackoffKind;
    use tidb_txnkv::transaction::TransactionCause;

    #[derive(Default)]
    struct RecordingPacketOutput(Vec<Vec<u8>>);

    impl ConnectionPacketOutput for RecordingPacketOutput {
        fn write_packet(
            &mut self,
            sequence: u8,
            payload: &[u8],
        ) -> Result<u8, MysqlConnectionError> {
            self.0.push(payload.to_vec());
            Ok(sequence.wrapping_add(1))
        }
    }

    #[test]
    fn an_undetermined_cluster_ddl_is_connection_fatal_without_an_error_packet() {
        let query_error = cluster_ddl_error(ClusterDdlError::Undetermined(
            "commit response lost".to_owned(),
        ));
        let mut output = RecordingPacketOutput::default();
        let write_error = write_query_error(&mut output, &query_error, true)
            .expect_err("an unknown DDL verdict must close the connection");
        assert!(matches!(
            write_error,
            MysqlConnectionError::ResultUndetermined(message)
                if message == tidb_error::terror::ERR_RESULT_UNDETERMINED.message()
        ));
        assert!(
            output.0.is_empty(),
            "an unknown DDL verdict must not write an ERR packet or any bytes"
        );
    }

    #[test]
    fn configured_write_backoff_error_is_coded_on_the_wire() {
        let driver_error = tidb_exec::pessimistic_lock_error::transaction_cause_to_sql_error(
            &TransactionCause::BackoffExhausted {
                kind: RegionBackoffKind::StaleCommand,
                detail: "staleCommand backoffer exhausted".to_owned(),
            },
        );
        let query_error = configured_write_error(&ConfiguredWriteError::Commit(driver_error));
        let mut output = RecordingPacketOutput::default();
        write_query_error(&mut output, &query_error, true)
            .expect("a determinate stale-command error writes an ERR packet");
        assert_eq!(output.0.len(), 1);
        let packet = &output.0[0];
        assert_eq!(packet[0], 0xff);
        assert_eq!(
            u16::from_le_bytes([packet[1], packet[2]]),
            tidb_error::tidb::errcode::ErrTiKVStaleCommand
        );
        assert!(String::from_utf8_lossy(&packet[9..]).contains("TiKV server reports stale command"));
    }

}
