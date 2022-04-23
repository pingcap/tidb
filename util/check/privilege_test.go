package check

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyPrivileges(t *testing.T) {
	cases := []struct {
		grants           []string
		dumpState        State
		replicationState State
	}{
		{
			grants:           nil, // non grants
			dumpState:        StateFailure,
			replicationState: StateFailure,
		},
		{
			grants:           []string{"invalid SQL statement"},
			dumpState:        StateFailure,
			replicationState: StateFailure,
		},
		{
			grants:           []string{"CREATE DATABASE db1"}, // non GRANT statement
			dumpState:        StateFailure,
			replicationState: StateFailure,
		},
		{
			grants:           []string{"GRANT SELECT ON *.* TO 'user'@'%'"}, // lack necessary privilege
			dumpState:        StateFailure,
			replicationState: StateFailure,
		},
		{
			grants:           []string{"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'"}, // lack necessary privilege
			dumpState:        StateFailure,
			replicationState: StateFailure,
		},
		{
			grants: []string{ // lack optional privilege
				"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'",
				"GRANT REPLICATION CLIENT ON *.* TO 'user'@'%'",
				"GRANT EXECUTE ON FUNCTION db1.anomaly_score TO 'user1'@'domain-or-ip-address1'",
			},
			dumpState:        StateFailure,
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // have privileges
				"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'",
				"GRANT REPLICATION CLIENT ON *.* TO 'user'@'%'",
				"GRANT RELOAD ON *.* TO 'user'@'%'",
				"GRANT EXECUTE ON FUNCTION db1.anomaly_score TO 'user1'@'domain-or-ip-address1'",
			},
			dumpState:        StateFailure,
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // have privileges
				"GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD, SELECT ON *.* TO 'user'@'%'",
			},
			dumpState:        StateSuccess,
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // have privileges
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%'",
			},
			dumpState:        StateSuccess,
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // lower case
				"GRANT all privileges ON *.* TO 'user'@'%'",
			},
			dumpState:        StateSuccess,
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // IDENTIFIED BY PASSWORD
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD 'secret'",
			},
			dumpState:        StateSuccess,
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // IDENTIFIED BY PASSWORD
				"GRANT ALL PRIVILEGES ON *.* TO 'user'@'%' IDENTIFIED BY PASSWORD 'password' WITH GRANT OPTION",
			},
			dumpState:        StateSuccess,
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // Aurora have `LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA`
				"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA, INVOKE SAGEMAKER, INVOKE COMPREHEND ON *.* TO 'root'@'%' WITH GRANT OPTION",
			},
			dumpState:        StateSuccess,
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // Aurora have `LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA`
				"GRANT INSERT, UPDATE, DELETE, CREATE, DROP, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, LOAD FROM S3, SELECT INTO S3, INVOKE LAMBDA, INVOKE SAGEMAKER, INVOKE COMPREHEND ON *.* TO 'root'@'%' WITH GRANT OPTION",
			},
			dumpState:        StateFailure,
			replicationState: StateFailure,
		},
		{
			grants: []string{ // test `LOAD FROM S3, SELECT INTO S3` not at end
				"GRANT INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, LOAD FROM S3, SELECT INTO S3, SELECT ON *.* TO 'root'@'%' WITH GRANT OPTION",
			},
			dumpState:        StateSuccess,
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // ... and `LOAD FROM S3` at beginning, as well as not adjacent with `SELECT INTO S3`
				"GRANT LOAD FROM S3, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, PROCESS, REFERENCES, INDEX, ALTER, SHOW DATABASES, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, SELECT INTO S3, SELECT ON *.* TO 'root'@'%' WITH GRANT OPTION",
			},
			dumpState:        StateSuccess,
			replicationState: StateSuccess,
		},
		{
			grants: []string{ // privilege on db/table level is not enough to execute SHOW MASTER STATUS
				"GRANT ALL PRIVILEGES ON `medz`.* TO `zhangsan`@`10.8.1.9` WITH GRANT OPTION",
			},
			dumpState:        StateFailure,
			replicationState: StateFailure,
		},
		{
			grants: []string{ // privilege on column level is not enough to execute SHOW CREATE TABLE
				"GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'user'@'%'",
				"GRANT SELECT (c) ON `lance`.`t` TO 'user'@'%'",
			},
			dumpState:        StateFailure,
			replicationState: StateSuccess,
		},
		{
			grants: []string{
				"GRANT RELOAD ON *.* TO `u1`@`localhost`",
				"GRANT SELECT, INSERT, UPDATE, DELETE ON `db1`.* TO `u1`@`localhost`",
				"GRANT `r1`@`%`,`r2`@`%` TO `u1`@`localhost`",
			},
			dumpState:        StateSuccess,
			replicationState: StateFailure,
		},
		{
			grants: []string{
				"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, RELOAD, SHUTDOWN, PROCESS, FILE, REFERENCES, INDEX, ALTER, SHOW DATABASES, SUPER, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT, TRIGGER, CREATE TABLESPACE, CREATE ROLE, DROP ROLE ON *.* TO `root`@`localhost` WITH GRANT OPTION",
				"GRANT APPLICATION_PASSWORD_ADMIN,AUDIT_ADMIN,BACKUP_ADMIN,BINLOG_ADMIN,BINLOG_ENCRYPTION_ADMIN,CLONE_ADMIN,CONNECTION_ADMIN,ENCRYPTION_KEY_ADMIN,GROUP_REPLICATION_ADMIN,INNODB_REDO_LOG_ARCHIVE,PERSIST_RO_VARIABLES_ADMIN,REPLICATION_APPLIER,REPLICATION_SLAVE_ADMIN,RESOURCE_GROUP_ADMIN,RESOURCE_GROUP_USER,ROLE_ADMIN,SERVICE_CONNECTION_ADMIN,SESSION_VARIABLES_ADMIN,SET_USER_ID,SYSTEM_USER,SYSTEM_VARIABLES_ADMIN,TABLE_ENCRYPTION_ADMIN,XA_RECOVER_ADMIN ON *.* TO `root`@`localhost` WITH GRANT OPTION",
				"GRANT PROXY ON ''@'' TO 'root'@'localhost' WITH GRANT OPTION",
			},
			dumpState:        StateSuccess,
			replicationState: StateSuccess,
		},
	}

	for _, cs := range cases {
		result := &Result{
			State: StateFailure,
		}
		verifyPrivileges(result, cs.grants, dumpPrivileges)
		require.Equal(t, cs.dumpState, result.State)
		verifyPrivileges(result, cs.grants, replicationPrivileges)
		require.Equal(t, cs.replicationState, result.State)
	}
}
