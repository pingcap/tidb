package audit

import (
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNormalizeUserAndHost(t *testing.T) {
	cases := []struct {
		input      string
		user       string
		host       string
		normalized string
		errstr     string
	}{
		{
			"foo@bar",
			"foo",
			"bar",
			"foo@bar",
			"",
		},
		{
			" foo2@bar",
			"foo2",
			"bar",
			"foo2@bar",
			"",
		},
		{
			"'foo3'@'bar'",
			"foo3",
			"bar",
			"foo3@bar",
			"",
		},
		{
			"foo4",
			"foo4",
			"%",
			"foo4",
			"",
		},
		{
			"foo@bar@baz",
			"foo@bar",
			"baz",
			"foo@bar@baz",
			"",
		},
		{
			"@@@",
			"",
			"",
			"",
			"illegal user format",
		},
	}
	for _, c := range cases {
		normalized, user, host, err := normalizeUserAndHost(c.input)
		if c.errstr == "" {
			require.NoError(t, err)
			require.Equal(t, c.user, user)
			require.Equal(t, c.host, host)
			require.Equal(t, c.normalized, normalized)
		} else {
			require.ErrorContains(t, err, c.errstr)
		}
	}
}

func TestEntryPool(t *testing.T) {
	poolCap := 8
	pool := createLogFieldsPool(poolCap)
	require.Equal(t, time.Duration(0), pool.IdleTimeout())
	// cnt = poolCap * 2 to make sure the pool will be exhausted
	cnt := poolCap * 2
	resources := make([]logFields, 0, cnt)
	closeFuncs := make([]func(), 0, cnt)
	dedup := make(map[uintptr]logFields)
	for i := 0; i < cnt; i++ {
		fields, fn := fieldsFromPool(pool)
		require.Len(t, fields, 0)
		fields = append(fields, zap.String("foo", "bar"))
		resources = append(resources, fields)
		closeFuncs = append(closeFuncs, fn)
		ptr := uintptr(unsafe.Pointer(unsafe.SliceData(fields)))
		dedup[ptr] = fields
		if i < poolCap {
			require.Equal(t, int64(poolCap-i-1), pool.Available())
		} else {
			require.Equal(t, int64(0), pool.Available())
		}
	}
	// All new fields should be different.
	require.Equal(t, cnt, len(dedup))
	for _, fn := range closeFuncs {
		fn()
	}
	// fields should be reused
	require.Equal(t, int64(poolCap), pool.Available())
	fields, fn := fieldsFromPool(pool)
	require.Len(t, fields, 0)
	require.Contains(t, dedup, uintptr(unsafe.Pointer(unsafe.SliceData(fields))))
	fn()
}

func TestIsPasswordStmt(t *testing.T) {
	cases := []struct {
		sql         string
		hasPassword bool
	}{
		{
			sql:         "create user u1",
			hasPassword: false,
		},
		{
			sql:         "create user u1 identified by '123'",
			hasPassword: true,
		},
		{
			sql:         "create user u1 identified with authentication_ldap_sasl as 'uid=u1_ldap,ou=People,dc=example,dc=com'",
			hasPassword: true,
		},
		{
			sql:         "alter user u1",
			hasPassword: false,
		},
		{
			sql:         "alter user u1 identified by '123'",
			hasPassword: true,
		},
		{
			sql:         "alter user u1 identified with authentication_ldap_sasl as 'uid=u1_ldap,ou=People,dc=example,dc=com'",
			hasPassword: true,
		},
		{
			sql:         "set password for u1 = '123'",
			hasPassword: true,
		},
		{
			sql:         "set @a=1",
			hasPassword: false,
		},
		{
			sql:         "select * from t",
			hasPassword: false,
		},
	}
	p := parser.New()
	for _, c := range cases {
		stmt, err := p.ParseOneStmt(c.sql, "", "")
		require.NoError(t, err)
		require.Equal(t, c.hasPassword, isPasswordStmt(stmt))
	}
}
