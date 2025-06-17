package audit

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	tablefilter "github.com/pingcap/tidb/pkg/util/table-filter"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// UserHostSep is a separator to separate user and hot
const UserHostSep = "@"

// TrimChars is the trimset for usernames
//
// examples:
// 'foo'@'bar' => "foo@bar" (trim single quote)
// " foo@bar"  => "foo@bar" (trim space)
const TrimChars = " '"

type keyWatcher struct {
	ctx          context.Context
	etcd         *clientv3.Client
	prefix       string
	Chan         clientv3.WatchChan
	chCreateTime time.Time
}

func newKeyWatcher(ctx context.Context, etcd *clientv3.Client, prefix string) *keyWatcher {
	return &keyWatcher{
		ctx:          ctx,
		etcd:         etcd,
		prefix:       prefix,
		Chan:         etcd.Watch(ctx, prefix, clientv3.WithPrefix()),
		chCreateTime: time.Now(),
	}
}

func (w *keyWatcher) RenewClosedChan() {
	var sleep time.Duration
	if interval := time.Since(w.chCreateTime); interval < MinEtcdReWatchInterval {
		sleep = interval
	}

	logutil.BgLogger().Info("audit watch chan closed, renew it", zap.String("watch", w.prefix), zap.Duration("sleep", sleep))
	i := 0
	for time.Since(w.chCreateTime) < MinEtcdReWatchInterval {
		if err := w.ctx.Err(); err != nil {
			return
		}

		i++
		if i <= 5 {
			time.Sleep(10 * time.Millisecond)
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}

	w.Chan = w.etcd.Watch(w.ctx, w.prefix, clientv3.WithPrefix())
	w.chCreateTime = time.Now()
}

type entryIDGenerator struct {
	base string
	seq  uint
}

func newEntryIDGenerator() *entryIDGenerator {
	return &entryIDGenerator{
		base: uuid.NewString(),
		seq:  1,
	}
}

func (g *entryIDGenerator) Next() (id string) {
	if g.seq < math.MaxUint16 {
		g.seq++
	} else {
		g.base = uuid.NewString()
		g.seq = 1
	}
	return fmt.Sprintf("%s-%04x", g.base, g.seq)
}

type errLogWriter struct{}

func (*errLogWriter) Write(err []byte) (int, error) {
	logutil.BgLogger().Error("audit log error", zap.ByteString("err", err))
	return len(err), nil
}

func normalizeIP(ip string) string {
	switch ip {
	case "localhost":
		return "127.0.0.1"
	default:
		return ip
	}
}

func parseTableFilter(table string) (tablefilter.Filter, error) {
	// forbid wildcards '!' and '@' for safety,
	// please see https://github.com/pingcap/tidb-tools/tree/master/pkg/table-filter for more details.
	arg := strings.TrimLeft(table, " \t")
	if arg == "" || arg[0] == '!' || arg[0] == '@' || arg[0] == '/' {
		return nil, errors.Errorf("invalid table format: '%s'", table)
	}

	f, err := tablefilter.Parse([]string{arg})
	if err != nil {
		return nil, errors.Errorf("invalid table format: '%s'", table)
	}

	return f, nil
}

func normalizeUserAndHost(userAndHost string) (normalized string, user string, host string, err error) {
	sepIndex := strings.LastIndex(userAndHost, UserHostSep)
	switch sepIndex {
	case -1:
		user = strings.Trim(userAndHost, TrimChars)
		host = "%"
		normalized = user
	default:
		user = strings.Trim(userAndHost[:sepIndex], TrimChars)
		host = strings.Trim(userAndHost[sepIndex+1:], TrimChars)
		normalized = user + UserHostSep + host
	}
	if len(user) == 0 || len(host) == 0 {
		return "", "", "", errors.Errorf("illegal user format: '%s'", userAndHost)
	}
	return
}

func toStringArgs(args []types.Datum) []string {
	if len(args) == 0 {
		return nil
	}

	stringArgs := make([]string, len(args))
	for i, arg := range args {
		if arg.IsNull() {
			stringArgs[i] = "NULL"
		} else {
			stringArgs[i] = fmt.Sprintf("%v", arg.GetValue())
		}
	}
	return stringArgs
}

func executeSQL(ctx context.Context, exec sqlexec.SQLExecutor, sql string, args ...any) ([]chunk.Row, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	rs, err := exec.ExecuteInternal(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	if rs != nil {
		defer func() {
			terror.Log(rs.Close())
		}()
		return sqlexec.DrainRecordSet(ctx, rs, 8)
	}

	return nil, nil
}
