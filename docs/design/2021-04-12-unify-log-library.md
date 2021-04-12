# Proposal: Unifying Log Library

- Author(s): [Yifan Xu](https://github.com/SabaPing)
- Last updated:  Apr 12, 2021
- Discussion at: N/A

## Motivation or Background

Tidb as a distributed system can be divided into several different components, such as tikv, pd, tidb, tiflash, etc. Each component hits a different log. Except for slow query logs, all other logs must satisfy the unified-log-format rfc standard.
However, during practice, it was found that the format of logs is confusing, as shown in the following four points.

- There are fewer logging configuration instructions in the documentation. The future needs to write clearly what logs each component will hit, and what the format of the logs are.
- The configured logging parameters do not match the runtime logging, e.g. tidb_stderr is configured with text format but the logging is in json format.
- The logs of some components do not meet the unified-log-format rfc standard, e.g. tiflash_cluster_manager.
- Duplicate logs, e.g. pd_stderr will hit both text and json logs with duplicate content (but with a few subtle differences in timestamps).

## Logging code for each component

### Pingcap/log

First, the address: [pingcap/log (github.com)](https://github.com/pingcap/log).

As a common logging library for pingcap golang, it does the following things:

- Provides the standard config schema
- provides a factory method for creating log handlers
- hard code the log format according to rfcs/2018-12-19-unified-log-format.md at master - tikv/rfcs (github.com)
- encapsulates the logic of the rolling file
- Provides global log handler and related methods for package dimension
  
Pingcap/log has a strong limitation -- it cannot customize the encoder for the text format; this problem has been fixed by the big guys, see [Feature/register zap encoder by 9547 - Pull Request #14 - pingcap/log ( github.com)](https://github.com/pingcap/log/pull/14).

When PD and TiDB-operator were using pingcap/log, there was no custom encoder function yet, so they implemented one by themselves respectively, but accidentally wrote out a circular dependency. This problem is also fixed by a big guys, see [logutil: replace etcd.defaultlogger with pingcap's text encoder by 9547 - Pull Request #3480 - tikv/pd (github.com)](https://github.com/tikv/pd/pull/3480), for details. Same big guy as above.

### TiDB

Log library dependencies:
![tidb-log-dependency](./imgs/tidb-log-dependency.png)

For historical reasons, TiDB has two third-party logging libraries -- logrus and pingcap/log, with pingcap/log wrapping another layer on top of zap.

TiDB's logs can be divided by business into two types: slow query logs and remaining other logs. As mentioned above, the two types of logs are typed through two different logging repositories, which results in separate configurations for the two types of logs and requires writing additional configuration conversion code.

TiDB-specific logging logic -- such as logger initialization, logger configuration, etc. -- is written inside util/logutil/log.go. Note this file, which is one of the main culprits of circular dependencies. The following briefly describes the key logic in util/logutil/log.go -- the two init methods and the four log handlers.

#### Logrus

The init method of Logrus -- func InitLogger(cfg *LogConfig) error -- may initialize two logrus handlers.

First, there is necessarily the standard log handler (package level handler) of logrus. initLogger first initializes the standard logger according to the configuration.

```go
func InitLogger(cfg *LogConfig) error {
   log.SetLevel(stringToLogLevel(cfg.Level))
   log.AddHook(&contextHook{})

   if cfg.Format == "" {
      cfg.Format = DefaultLogFormat
   }
   formatter := stringToLogFormatter(cfg.Format, cfg.DisableTimestamp)
   log.SetFormatter(formatter)

   if len(cfg.File.Filename) != 0 {
      if err := initFileLog(&cfg.File, nil); err != nil {
         return errors.Trace(err)
      }
   }

// The rest is omitted.
```

Then, determine whether the configuration has enabled slow query log, and if so, create a log handler specific to slow query.

```go
if len(cfg.SlowQueryFile) != 0 {
   SlowQueryLogger = log.New()
   tmp := cfg.File
   tmp.Filename = cfg.SlowQueryFile
   if err := initFileLog(&tmp, SlowQueryLogger); err != nil {
      return errors.Trace(err)
   }
   SlowQueryLogger.Formatter = &slowLogFormatter{}
}
```

Regarding where these two handlers are used.

- Some historical legacy code, such as cmd/importer/parser.go, which uses the standard logger by logrus.
- Slow query log all uses the slow query log handler created by logrus, code in executor/adapter.go.

#### Pingcap/log (Zap)

Pingcap/log is a wrapper around zap, and as mentioned below the two terms are equivalently interchangeable.

Similar to logrus, the init method of zap -- func InitZapLogger(cfg *LogConfig) error -- may initialize two zap handlers.

- The global zap handler, the default log handler for the entire repo, through which the vast majority of logs are typed.
- Slow query zap handler, which is only initialized and not used.
  InitZapLogger's logic is very similar to logrus' above, so I won't repeat it here.

#### GRPC Logger

I almost forgot that there is a fish in the net, which is not in util/logutil/log.go. In main.go there is a bunch of grpc logger initialization code.

```go
if len(os.Getenv("GRPC_DEBUG")) > 0 {
   grpclog.SetLoggerV2(grpclog.NewLoggerV2WithVerbosity(os.Stderr, os.Stderr, os.Stderr, 999))
} else {
   grpclog.SetLoggerV2(grpclog.NewLoggerV2(ioutil.Discard, ioutil.Discard, os.Stderr))
}
```

The NewLoggerV2 method creates a go native logger handler and is only used in grpc.

## What to do?

There must be something wrong with the engineering of these codes above, and they must be changed, but the cost of changing them is not small.

Principle: For long-term consideration, you can't 💩 on 💩. The speed of output can be sacrificed in time if necessary.

1. Modify tidb to unify the use of pingcap/log log libraries.
2. Then modify other components, such as br, pd, etc.
