package session

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestUpgradeCleansStaleRoutineMetadata(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "set global tidb_enable_procedure = on")
	MustExec(t, se, "drop database if exists Foo")
	MustExec(t, se, "drop database if exists foo")
	MustExec(t, se, "create database foo")
	MustExec(t, se, "use foo")
	MustExec(t, se, "create procedure p() begin select 1; end;")
	MustExec(t, se, `insert into mysql.routines
		select
			'Foo', name, type, definition, definition_utf8, parameter_str,
			is_deterministic, sql_data_access, security_type, definer, sql_mode,
			character_set_client, connection_collation, schema_collation,
			created, last_altered, comment, options, external_language
		from mysql.routines
		where route_schema = 'foo' and name = 'p' and type = 'PROCEDURE'`)
	MustExec(t, se, "commit")
	rows := MustExecToRecodeSet(t, se, `select route_schema, name, type
		from mysql.routines
		where lower(route_schema) = 'foo' and name = 'p' and type = 'PROCEDURE'
		order by route_schema`)
	chk := rows.NewChunk(nil)
	require.NoError(t, rows.Next(context.Background(), chk))
	require.Equal(t, 2, chk.NumRows())
	require.Equal(t, "Foo", chk.GetRow(0).GetString(0))
	require.Equal(t, "foo", chk.GetRow(1).GetString(0))

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrapEE(eeversion15)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, se, fmt.Sprintf(
		"update mysql.tidb set variable_value='%d' where variable_name='tidb_enterprise_edition_server_version'",
		eeversion15,
	))

	dom.Close()
	unsetStoreBootstrapped(store.UUID())
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()

	seCurVer := CreateSessionAndSetID(t, store)
	ver, err := getBootstrapEEVersion(seCurVer)
	require.NoError(t, err)
	require.Equal(t, currentEEBootstrapVersion, ver)
	rows = MustExecToRecodeSet(t, seCurVer, `select route_schema, name, type
		from mysql.routines
		where lower(route_schema) = 'foo' and name = 'p' and type = 'PROCEDURE'
		order by route_schema`)
	chk = rows.NewChunk(nil)
	require.NoError(t, rows.Next(context.Background(), chk))
	require.Equal(t, 1, chk.NumRows())
	require.Equal(t, "foo", chk.GetRow(0).GetString(0))
}

func TestUpgradeKeepsLiveRoutineMetadataWithoutExactCaseMatch(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "set global tidb_enable_procedure = on")
	MustExec(t, se, "drop database if exists Foo")
	MustExec(t, se, "drop database if exists foo")
	MustExec(t, se, "create database Foo")
	MustExec(t, se, "create procedure Foo.p() begin select 1; end;")
	MustExec(t, se, `update mysql.routines
		set route_schema = 'foo'
		where route_schema = 'Foo' and name = 'p' and type = 'PROCEDURE'`)
	MustExec(t, se, "commit")

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMutator(txn)
	err = m.FinishBootstrapEE(eeversion15)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	MustExec(t, se, fmt.Sprintf(
		"update mysql.tidb set variable_value='%d' where variable_name='tidb_enterprise_edition_server_version'",
		eeversion15,
	))

	dom.Close()
	unsetStoreBootstrapped(store.UUID())
	domCurVer, err := BootstrapSession(store)
	require.NoError(t, err)
	defer domCurVer.Close()

	seCurVer := CreateSessionAndSetID(t, store)
	rows := MustExecToRecodeSet(t, seCurVer, `select route_schema, name, type
		from mysql.routines
		where lower(route_schema) = 'foo' and name = 'p' and type = 'PROCEDURE'
		order by route_schema`)
	chk := rows.NewChunk(nil)
	require.NoError(t, rows.Next(context.Background(), chk))
	require.Equal(t, 1, chk.NumRows())
	require.Equal(t, "foo", chk.GetRow(0).GetString(0))
}
