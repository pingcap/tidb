package sessionctx

import (
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/tracing"
	"golang.org/x/net/context"
)

const (
	// span_idx    INT NOT NULL,        -- The span's index.
	TraceSpanIdxCol = iota
	// message_idx INT NOT NULL,        -- The message's index within its span.
	TraceMsgIdxCol
	// timestamp   TIMESTAMPTZ NOT NULL,-- The message's timestamp.
	TraceTimestampCol
	// duration    INTERVAL,            -- The span's duration.
	//                                  -- NULL if the span was not finished at the time
	//                                  -- the trace has been collected.
	TraceDurationCol
	// operation   STRING NULL,         -- The span's operation.
	TraceOpCol
	// loc         STRING NOT NULL,     -- The file name / line number prefix, if any.
	TraceLocCol
	// tag         STRING NOT NULL,     -- The logging tag, if any.
	TraceTagCol
	// message     STRING NOT NULL,     -- The logged message.
	TraceMsgCol
	// age         INTERVAL NOT NULL    -- The age of the message.
	TraceAgeCol
	// traceNumCols must be the last item in the enumeration.
	TraceNumCols
)

func init() {
	TraceFieldTps[TraceSpanIdxCol] = types.NewFieldType(mysql.TypeLong)
	TraceFieldTps[TraceMsgIdxCol] = types.NewFieldType(mysql.TypeLong)
	TraceFieldTps[TraceTimestampCol] = types.NewFieldType(mysql.TypeTimestamp)
	TraceFieldTps[TraceDurationCol] = types.NewFieldType(mysql.TypeDuration)
	TraceFieldTps[TraceOpCol] = types.NewFieldType(mysql.TypeString)
	TraceFieldTps[TraceLocCol] = types.NewFieldType(mysql.TypeString)
	TraceFieldTps[TraceTagCol] = types.NewFieldType(mysql.TypeString)
	TraceFieldTps[TraceMsgCol] = types.NewFieldType(mysql.TypeString)
	TraceFieldTps[TraceAgeCol] = types.NewFieldType(mysql.TypeString)
}

var TraceFieldTps = make([]*types.FieldType, TraceNumCols)
var TraceColNames = make([]string, TraceNumCols)

// SessionTracing is just a tmp interface for aoviding cycle import. This will be deleted after we finish
// refactoring package.
type SessionTracing interface {
	StartTracing(recType tracing.RecordingType, kvTracingEnabled, showResults bool) error
	StopTracing() error
	RecordingType() tracing.RecordingType
	Enabled() bool
	TraceParseStart(ctx context.Context, stmtTag string)
	TraceParseEnd(ctx context.Context, err error)
	TracePlanStart(ctx context.Context, stmtTag string)
	TracePlanEnd(ctx context.Context, err error)
	TraceExecStart(ctx context.Context, engine string)
	TraceExecConsume(ctx context.Context) (context.Context, func())
	TraceExecRowsResult(ctx context.Context, values []types.Datum)
	TraceExecEnd(ctx context.Context, err error, count int)
	GetSessionTrace() (*chunk.Chunk, error)
	Ctx() context.Context
}
