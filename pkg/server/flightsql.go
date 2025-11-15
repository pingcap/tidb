package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/server/internal/column"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
)

type FlightSQLServer struct {
	flightsql.BaseServer
	server       *Server
	flightServer flight.Server

	tidbAllocator  chunk.Allocator
	arrowAllocator memory.Allocator
}

func NewFlightSQLServer(server *Server) (*FlightSQLServer, error) {
	ret := &FlightSQLServer{
		server:         server,
		tidbAllocator:  chunk.NewAllocator(),
		arrowAllocator: memory.NewGoAllocator(),
	}
	ret.Alloc = memory.DefaultAllocator

	return ret, nil
}

func (s *FlightSQLServer) Serve(l net.Listener) error {
	// Create server with auth middleware that uses our ServerAuthHandler
	server := flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		createFlightSQLAuthMiddleware(s),
	})
	server.RegisterFlightService(flightsql.NewFlightServer(s))
	server.InitListener(l)

	s.flightServer = server

	return server.Serve()
}

func (s *FlightSQLServer) Shutdown() {
	s.flightServer.Shutdown()
}

func (s *FlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	// Database selection is optional - clients can use fully-qualified table names
	// Extract database from metadata if provided
	// dbs := metadata.ValueFromIncomingContext(ctx, "database")
	// Database name is not used in this method, only stored in the ticket

	query, txnid := cmd.GetQuery(), cmd.GetTransactionId()
	tkt, err := encodeTransactionQuery(query, txnid)
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: tkt}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (s *FlightSQLServer) GetFlightInfoSubstraitPlan(ctx context.Context, plan flightsql.StatementSubstraitPlan, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, errors.New("substrait plans are not supported")
}

func (s *FlightSQLServer) GetSchemaStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	panic("GetSchemaStatement not implemented")

}

func (s *FlightSQLServer) GetSchemaSubstraitPlan(ctx context.Context, plan flightsql.StatementSubstraitPlan, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	return nil, errors.New("substrait plans are not supported")
}

func (s *FlightSQLServer) DoGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	_, query, err := decodeTransactionQuery(cmd.GetStatementHandle())
	if err != nil {
		return nil, nil, err
	}
	logutil.BgLogger().Info("DoGetStatement", zap.String("query", query))

	// Extract database from metadata, default to empty string if not provided
	var dbName string
	dbs := metadata.ValueFromIncomingContext(ctx, "database")
	if len(dbs) > 0 {
		dbName = dbs[0]
	}

	ct, err := s.server.driver.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), dbName, nil, nil)
	if err != nil {
		return nil, nil, err
	}

	// Only execute USE statement if a database was specified
	if dbName != "" {
		useStmt, err := ct.Parse(ctx, "use `"+dbName+"`")
		if err != nil {
			return nil, nil, err
		}
		_, err = ct.ExecuteStmt(ctx, useStmt[0])
		if err != nil {
			return nil, nil, err
		}
	}

	stmts, err := ct.Parse(ctx, query)
	if err != nil {
		return nil, nil, err
	}

	if len(stmts) != 1 {
		return nil, nil, errors.New("run multiple statements is not supported")
	}

	rs, err := ct.Session.ExecuteStmt(ctx, stmts[0])
	if err != nil {
		return nil, nil, err
	}

	rdr, err := NewResultSetRecordReader(rs, s.tidbAllocator, s.arrowAllocator)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go flight.StreamChunksFromReader(rdr, ch)

	return rdr.Schema(), ch, nil
}

func (s *FlightSQLServer) GetFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	// Decode the handle to get the query
	query, _, err := decodePreparedHandle(cmd.GetPreparedStatementHandle())
	if err != nil {
		return nil, err
	}

	logutil.BgLogger().Info("GetFlightInfoPreparedStatement", zap.String("query", query))

	// Use helper to get schema
	schema, err := s.prepareAndGetSchema(ctx, query)
	if err != nil {
		return nil, err
	}

	// Use desc.Cmd as the ticket (Porter pattern)
	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.arrowAllocator),
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: desc.Cmd},
		}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (s *FlightSQLServer) GetSchemaPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	panic("GetSchemaPreparedStatement not implemented")

}

func (s *FlightSQLServer) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// Decode the handle to get query and parameters
	query, params, err := decodePreparedHandle(cmd.GetPreparedStatementHandle())
	if err != nil {
		return nil, nil, err
	}

	logutil.BgLogger().Info("DoGetPreparedStatement", zap.String("query", query), zap.Bool("has_params", len(params) > 0))

	// Extract database from metadata if provided
	var dbName string
	dbs := metadata.ValueFromIncomingContext(ctx, "database")
	if len(dbs) > 0 {
		dbName = dbs[0]
	}

	ct, err := s.server.driver.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), dbName, nil, nil)
	if err != nil {
		return nil, nil, err
	}

	// Only execute USE statement if a database was specified
	if dbName != "" {
		useStmt, err := ct.Parse(ctx, "use `"+dbName+"`")
		if err != nil {
			return nil, nil, err
		}
		_, err = ct.ExecuteStmt(ctx, useStmt[0])
		if err != nil {
			return nil, nil, err
		}
	}

	// Prepare the statement using TiDB's Prepare API
	stmt, _, _, err := ct.Prepare(query)
	if err != nil {
		return nil, nil, err
	}
	defer stmt.Close()

	// Deserialize parameters if present
	var paramExprs []expression.Expression
	if len(params) > 0 {
		paramExprs, err = deserializeExpressions(params)
		if err != nil {
			return nil, nil, err
		}
	}

	// Execute the prepared statement with parameters
	rs, err := stmt.Execute(ctx, paramExprs)
	if err != nil {
		return nil, nil, err
	}

	// Convert column.Info to Arrow schema using helper
	columns := rs.Columns()
	resultFields := convertColumnsToResultFields(columns)
	schema, err := adaptSchema(resultFields)
	if err != nil {
		return nil, nil, err
	}

	// Stream data chunks
	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		defer rs.Close() // Close after we're done reading

		builder := array.NewRecordBuilder(s.arrowAllocator, schema)
		defer builder.Release()

		chk := rs.NewChunk(s.tidbAllocator)
		defer chk.Reset()

		for {
			err := rs.Next(ctx, chk)
			if err != nil {
				ch <- flight.StreamChunk{Err: err}
				return
			}
			if chk.NumRows() == 0 {
				break
			}

			// Convert chunk to Arrow record using shared helper
			convertChunkToRecord(chk, rs.FieldTypes(), builder)

			rec := builder.NewRecord()
			ch <- flight.StreamChunk{Data: rec}
			chk.Reset()
		}
	}()

	return schema, ch, nil
}

func (s *FlightSQLServer) GetFlightInfoCatalogs(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	panic("GetFlightInfoCatalogs not implemented")

}

func (s *FlightSQLServer) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	panic("DoGetCatalogs not implemented")

}

func (s *FlightSQLServer) GetFlightInfoXdbcTypeInfo(ctx context.Context, cmd flightsql.GetXdbcTypeInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	panic("GetFlightInfoXdbcTypeInfo not implemented")

}

func (s *FlightSQLServer) DoGetXdbcTypeInfo(ctx context.Context, cmd flightsql.GetXdbcTypeInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	panic("DoGetXdbcTypeInfo not implemented")

}

func (s *FlightSQLServer) GetFlightInfoSqlInfo(ctx context.Context, cmd flightsql.GetSqlInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, errors.New("GetFlightInfoSqlInfo not implemented")
}

func (s *FlightSQLServer) DoGetSqlInfo(ctx context.Context, cmd flightsql.GetSqlInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	panic("DoGetSqlInfo not implemented")

}

func (s *FlightSQLServer) GetFlightInfoSchemas(ctx context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	panic("GetFlightInfoSchemas not implemented")

}

func (s *FlightSQLServer) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	panic("DoGetDBSchemas not implemented")

}

func (s *FlightSQLServer) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	panic("GetFlightInfoTables not implemented")

}

func (s *FlightSQLServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	panic("DoGetTables not implemented")

}

func (s *FlightSQLServer) GetFlightInfoTableTypes(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	panic("GetFlightInfoTableTypes not implemented")

}

func (s *FlightSQLServer) DoGetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	panic("DoGetTableTypes not implemented")

}

func (s *FlightSQLServer) GetFlightInfoPrimaryKeys(ctx context.Context, ref flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	panic("GetFlightInfoPrimaryKeys not implemented")

}

func (s *FlightSQLServer) DoGetPrimaryKeys(ctx context.Context, ref flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	panic("DoGetPrimaryKeys not implemented")

}

func (s *FlightSQLServer) GetFlightInfoExportedKeys(ctx context.Context, ref flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	panic("GetFlightInfoExportedKeys not implemented")

}

func (s *FlightSQLServer) DoGetExportedKeys(ctx context.Context, ref flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	panic("DoGetExportedKeys not implemented")

}

func (s *FlightSQLServer) GetFlightInfoImportedKeys(ctx context.Context, ref flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	panic("GetFlightInfoImportedKeys not implemented")

}

func (s *FlightSQLServer) DoGetImportedKeys(ctx context.Context, ref flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	panic("DoGetImportedKeys not implemented")

}

func (s *FlightSQLServer) GetFlightInfoCrossReference(ctx context.Context, ref flightsql.CrossTableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	panic("GetFlightInfoCrossReference not implemented")

}

func (s *FlightSQLServer) DoGetCrossReference(ctx context.Context, ref flightsql.CrossTableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	panic("DoGetCrossReference not implemented")

}

func (s *FlightSQLServer) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	query := cmd.GetQuery()
	logutil.BgLogger().Info("DoPutCommandStatementUpdate", zap.String("query", query))

	// Extract database from metadata if provided
	var dbName string
	dbs := metadata.ValueFromIncomingContext(ctx, "database")
	if len(dbs) > 0 {
		dbName = dbs[0]
	}

	ct, err := s.server.driver.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), dbName, nil, nil)
	if err != nil {
		return 0, err
	}
	defer ct.Close()

	// Only execute USE statement if a database was specified
	if dbName != "" {
		useStmt, err := ct.Parse(ctx, "use `"+dbName+"`")
		if err != nil {
			return 0, err
		}
		_, err = ct.ExecuteStmt(ctx, useStmt[0])
		if err != nil {
			return 0, err
		}
	}

	// Parse and execute the statement
	stmts, err := ct.Parse(ctx, query)
	if err != nil {
		return 0, err
	}

	if len(stmts) != 1 {
		return 0, errors.New("run multiple statements is not supported")
	}

	rs, err := ct.Session.ExecuteStmt(ctx, stmts[0])
	if err != nil {
		return 0, err
	}

	// For DML statements, we don't have a result set
	// Return affected rows count (default to 0 for now)
	// TODO: Extract actual affected rows from execution result
	var affectedRows int64 = 0
	if rs != nil {
		rs.Close()
	}

	return affectedRows, nil
}

func (s *FlightSQLServer) DoPutCommandSubstraitPlan(ctx context.Context, plan flightsql.StatementSubstraitPlan) (int64, error) {
	return 0, errors.New("substrait plans are not supported")
}

func (s *FlightSQLServer) CreatePreparedStatement(ctx context.Context, cmd flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	query := cmd.GetQuery()
	logutil.BgLogger().Info("CreatePreparedStatement", zap.String("query", query))

	// Create stateless handle containing the query
	// No parameters yet - will be added when client calls DoPutPreparedStatementQuery
	handleBytes := encodePreparedHandle(query, nil)

	// Use helper to prepare and get schema
	schema, err := s.prepareAndGetSchema(ctx, query)
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}

	// Return result with handle and schema
	return flightsql.ActionCreatePreparedStatementResult{
		Handle:        handleBytes,
		DatasetSchema: schema,
	}, nil
}

func (s *FlightSQLServer) CreatePreparedSubstraitPlan(ctx context.Context, cmd flightsql.ActionCreatePreparedSubstraitPlanRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	return flightsql.ActionCreatePreparedStatementResult{}, errors.New("substrait plans are not supported")
}

func (s *FlightSQLServer) ClosePreparedStatement(ctx context.Context, cmd flightsql.ActionClosePreparedStatementRequest) error {
	// NOP - prepared statements are stateless, so nothing to clean up
	return nil
}

func (s *FlightSQLServer) DoPutPreparedStatementQuery(ctx context.Context, cmd flightsql.PreparedStatementQuery, r flight.MessageReader, w flight.MetadataWriter) ([]byte, error) {
	// Decode the current handle
	query, _, err := decodePreparedHandle(cmd.GetPreparedStatementHandle())
	if err != nil {
		return nil, err
	}

	logutil.BgLogger().Info("DoPutPreparedStatementQuery", zap.String("query", query))

	// Read parameter records from the reader and accumulate them
	// Parameters may come in multiple batches
	var (
		builder      *array.RecordBuilder
		paramRecord  arrow.Record
		paramSchema  *arrow.Schema
	)

	for {
		rec, err := r.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			if builder != nil {
				builder.Release()
			}
			if paramRecord != nil {
				paramRecord.Release()
			}
			return nil, err
		}
		if rec == nil {
			continue
		}

		// Initialize builder with first record's schema
		if builder == nil {
			paramSchema = rec.Schema()
			builder = array.NewRecordBuilder(s.arrowAllocator, paramSchema)
		}

		// Validate schema consistency
		if !rec.Schema().Equal(paramSchema) {
			rec.Release()
			builder.Release()
			if paramRecord != nil {
				paramRecord.Release()
			}
			return nil, errors.New("parameter batch schema mismatch")
		}

		// Append columns from this batch to builder
		for i := 0; i < int(rec.NumCols()); i++ {
			appendArrowColumn(builder.Field(i), rec.Column(i))
		}
		rec.Release()
	}

	// Build final parameter record
	if builder != nil {
		paramRecord = builder.NewRecord()
		builder.Release()
	}

	// Convert Arrow record to TiDB expressions
	var paramsBytes []byte
	if paramRecord != nil {
		defer paramRecord.Release()

		exprs, err := arrowRecordToExpressions(paramRecord)
		if err != nil {
			return nil, fmt.Errorf("failed to convert parameters: %w", err)
		}

		// Serialize expressions to bytes
		paramsBytes, err = serializeExpressions(exprs)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize parameters: %w", err)
		}
	}

	// Create new handle with parameters encoded
	newHandle := encodePreparedHandle(query, paramsBytes)

	return newHandle, nil
}

func (s *FlightSQLServer) DoPutPreparedStatementUpdate(ctx context.Context, cmd flightsql.PreparedStatementUpdate, r flight.MessageReader) (int64, error) {
	// Decode the handle to get query and parameters
	query, params, err := decodePreparedHandle(cmd.GetPreparedStatementHandle())
	if err != nil {
		return 0, err
	}

	logutil.BgLogger().Info("DoPutPreparedStatementUpdate", zap.String("query", query), zap.Bool("has_params", len(params) > 0))

	// Read additional parameter records from the reader and accumulate them
	var (
		builder      *array.RecordBuilder
		paramRecord  arrow.Record
		paramSchema  *arrow.Schema
	)

	for {
		rec, err := r.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			if builder != nil {
				builder.Release()
			}
			if paramRecord != nil {
				paramRecord.Release()
			}
			return 0, err
		}
		if rec == nil {
			continue
		}

		// Initialize builder with first record's schema
		if builder == nil {
			paramSchema = rec.Schema()
			builder = array.NewRecordBuilder(s.arrowAllocator, paramSchema)
		}

		// Validate schema consistency
		if !rec.Schema().Equal(paramSchema) {
			rec.Release()
			builder.Release()
			if paramRecord != nil {
				paramRecord.Release()
			}
			return 0, errors.New("parameter batch schema mismatch")
		}

		// Append columns from this batch to builder
		for i := 0; i < int(rec.NumCols()); i++ {
			appendArrowColumn(builder.Field(i), rec.Column(i))
		}
		rec.Release()
	}

	// Build final parameter record
	if builder != nil {
		paramRecord = builder.NewRecord()
		builder.Release()
	}

	// Deserialize parameters from handle or use from reader
	var paramExprs []expression.Expression
	if paramRecord != nil {
		defer paramRecord.Release()
		paramExprs, err = arrowRecordToExpressions(paramRecord)
		if err != nil {
			return 0, fmt.Errorf("failed to convert parameters: %w", err)
		}
	} else if len(params) > 0 {
		paramExprs, err = deserializeExpressions(params)
		if err != nil {
			return 0, fmt.Errorf("failed to deserialize parameters: %w", err)
		}
	}

	// Execute the update statement
	ct, err := s.server.driver.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "", nil, nil)
	if err != nil {
		return 0, err
	}
	defer ct.Close()

	// Prepare and execute the statement with parameters
	stmt, _, _, err := ct.Prepare(query)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	rs, err := stmt.Execute(ctx, paramExprs)
	if err != nil {
		return 0, err
	}

	// For DML statements (INSERT, UPDATE, DELETE), return affected rows
	// For DDL statements, return 0
	var affectedRows int64 = 0
	if rs != nil {
		rs.Close()
	}

	return affectedRows, nil
}

func (s *FlightSQLServer) BeginTransaction(ctx context.Context, cmd flightsql.ActionBeginTransactionRequest) (id []byte, err error) {
	panic("BeginTransaction not implemented")

}

func (s *FlightSQLServer) BeginSavepoint(ctx context.Context, cmd flightsql.ActionBeginSavepointRequest) (id []byte, err error) {
	panic("BeginSavepoint not implemented")

}

func (s *FlightSQLServer) EndSavepoint(ctx context.Context, cmd flightsql.ActionEndSavepointRequest) error {
	panic("EndSavepoint not implemented")

}

func (s *FlightSQLServer) EndTransaction(ctx context.Context, cmd flightsql.ActionEndTransactionRequest) error {
	panic("EndTransaction not implemented")

}

func (s *FlightSQLServer) CancelFlightInfo(ctx context.Context, cmd *flight.CancelFlightInfoRequest) (flight.CancelFlightInfoResult, error) {
	panic("CancelFlightInfo not implemented")

}

func (s *FlightSQLServer) RenewFlightEndpoint(ctx context.Context, cmd *flight.RenewFlightEndpointRequest) (*flight.FlightEndpoint, error) {
	panic("RenewFlightEndpoint not implemented")

}

func encodeTransactionQuery(query string, transactionID flightsql.Transaction) ([]byte, error) {
	return flightsql.CreateStatementQueryTicket(
		bytes.Join([][]byte{transactionID, []byte(query)}, []byte(":")))
}

func decodeTransactionQuery(ticket []byte) (txnID, query string, err error) {
	id, queryBytes, found := bytes.Cut(ticket, []byte(":"))
	if !found {
		err = fmt.Errorf("%w: malformed ticket", arrow.ErrInvalid)
		return
	}

	txnID = string(id)
	query = string(queryBytes)
	return
}

// preparedException Handle encoding/decoding
// Format: query + "|" + parameters (optional)
// This makes prepared statements stateless - the query and parameters are passed back and forth with the client

func encodePreparedHandle(query string, params []byte) []byte {
	if len(params) == 0 {
		return []byte(query)
	}
	// Use "|" as separator between query and parameters
	result := make([]byte, 0, len(query)+1+len(params))
	result = append(result, []byte(query)...)
	result = append(result, '|')
	result = append(result, params...)
	return result
}

func decodePreparedHandle(handle []byte) (query string, params []byte, err error) {
	// Split on first "|"
	queryBytes, paramsBytes, found := bytes.Cut(handle, []byte("|"))
	if !found {
		// No parameters, just query
		return string(handle), nil, nil
	}
	return string(queryBytes), paramsBytes, nil
}

// appendArrowColumn appends values from an Arrow array to a builder.
// This is used when accumulating parameter batches.
func appendArrowColumn(builder array.Builder, arr arrow.Array) {
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			builder.AppendNull()
			continue
		}

		switch b := builder.(type) {
		case *array.Int64Builder:
			b.Append(arr.(*array.Int64).Value(i))
		case *array.Int32Builder:
			b.Append(arr.(*array.Int32).Value(i))
		case *array.Int16Builder:
			b.Append(arr.(*array.Int16).Value(i))
		case *array.Int8Builder:
			b.Append(arr.(*array.Int8).Value(i))
		case *array.Uint64Builder:
			b.Append(arr.(*array.Uint64).Value(i))
		case *array.Uint32Builder:
			b.Append(arr.(*array.Uint32).Value(i))
		case *array.Uint16Builder:
			b.Append(arr.(*array.Uint16).Value(i))
		case *array.Uint8Builder:
			b.Append(arr.(*array.Uint8).Value(i))
		case *array.Float32Builder:
			b.Append(arr.(*array.Float32).Value(i))
		case *array.Float64Builder:
			b.Append(arr.(*array.Float64).Value(i))
		case *array.StringBuilder:
			b.Append(arr.(*array.String).Value(i))
		case *array.BinaryBuilder:
			b.Append(arr.(*array.Binary).Value(i))
		case *array.Date64Builder:
			b.Append(arr.(*array.Date64).Value(i))
		default:
			// Unsupported type - append null
			builder.AppendNull()
		}
	}
}

// convertColumnsToResultFields converts column.Info to resolve.ResultField for schema adaptation.
// This is used when working with TiDB's Prepare() API which returns []*column.Info.
func convertColumnsToResultFields(columns []*column.Info) []*resolve.ResultField {
	resultFields := make([]*resolve.ResultField, len(columns))
	for i, col := range columns {
		resultFields[i] = &resolve.ResultField{
			Column: &model.ColumnInfo{
				Name:      ast.NewCIStr(col.Name),
				FieldType: ptypes.FieldType{},
			},
			ColumnAsName: ast.NewCIStr(col.Name),
		}
		// Set the type and flags
		resultFields[i].Column.FieldType.SetType(col.Type)
		resultFields[i].Column.FieldType.SetFlag(uint(col.Flag))
		resultFields[i].Column.FieldType.SetFlen(int(col.ColumnLength))
		resultFields[i].Column.FieldType.SetDecimal(int(col.Decimal))
		charsetName, _, _ := charset.GetCharsetInfoByID(int(col.Charset))
		if charsetName != "" {
			resultFields[i].Column.FieldType.SetCharset(charsetName)
		}
	}
	return resultFields
}

// prepareAndGetSchema prepares a query and returns its Arrow schema.
// Returns nil schema for non-SELECT statements.
func (s *FlightSQLServer) prepareAndGetSchema(ctx context.Context, query string) (*arrow.Schema, error) {
	ct, err := s.server.driver.OpenCtx(uint64(0), 0, uint8(tmysql.DefaultCollationID), "", nil, nil)
	if err != nil {
		return nil, err
	}
	defer ct.Close()

	stmt, columns, _, err := ct.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	// Convert columns to Arrow schema
	if len(columns) > 0 {
		resultFields := convertColumnsToResultFields(columns)
		return adaptSchema(resultFields)
	}
	// For non-SELECT statements (INSERT, UPDATE, DELETE, DDL), no schema
	return arrow.NewSchema([]arrow.Field{}, nil), nil
}

// arrowRecordToExpressions converts an Arrow record (single row) to TiDB expressions.
// The Arrow record should have one row representing the parameter values.
// Returns empty slice if record has 0 rows (no parameters).
func arrowRecordToExpressions(rec arrow.Record) ([]expression.Expression, error) {
	if rec.NumRows() == 0 {
		// No parameters
		return nil, nil
	}

	if rec.NumRows() != 1 {
		return nil, fmt.Errorf("parameter record must have exactly 1 row, got %d", rec.NumRows())
	}

	numParams := int(rec.NumCols())
	exprs := make([]expression.Expression, numParams)

	for i := 0; i < numParams; i++ {
		col := rec.Column(i)

		// Handle NULL
		if col.IsNull(0) {
			exprs[i] = expression.NewNull()
			continue
		}

		// Convert based on Arrow type and create properly typed Constant
		switch col.DataType().ID() {
		case arrow.INT8:
			val := col.(*array.Int8).Value(0)
			exprs[i] = expression.DatumToConstant(types.NewDatum(int64(val)), tmysql.TypeLonglong, 0)
		case arrow.INT16:
			val := col.(*array.Int16).Value(0)
			exprs[i] = expression.DatumToConstant(types.NewDatum(int64(val)), tmysql.TypeLonglong, 0)
		case arrow.INT32:
			val := col.(*array.Int32).Value(0)
			exprs[i] = expression.DatumToConstant(types.NewDatum(int64(val)), tmysql.TypeLonglong, 0)
		case arrow.INT64:
			val := col.(*array.Int64).Value(0)
			exprs[i] = expression.DatumToConstant(types.NewDatum(val), tmysql.TypeLonglong, 0)
		case arrow.UINT8:
			val := col.(*array.Uint8).Value(0)
			exprs[i] = expression.DatumToConstant(types.NewDatum(uint64(val)), tmysql.TypeLonglong, tmysql.UnsignedFlag)
		case arrow.UINT16:
			val := col.(*array.Uint16).Value(0)
			exprs[i] = expression.DatumToConstant(types.NewDatum(uint64(val)), tmysql.TypeLonglong, tmysql.UnsignedFlag)
		case arrow.UINT32:
			val := col.(*array.Uint32).Value(0)
			exprs[i] = expression.DatumToConstant(types.NewDatum(uint64(val)), tmysql.TypeLonglong, tmysql.UnsignedFlag)
		case arrow.UINT64:
			val := col.(*array.Uint64).Value(0)
			exprs[i] = expression.DatumToConstant(types.NewDatum(val), tmysql.TypeLonglong, tmysql.UnsignedFlag)
		case arrow.FLOAT32:
			val := col.(*array.Float32).Value(0)
			exprs[i] = expression.DatumToConstant(types.NewDatum(float64(val)), tmysql.TypeFloat, 0)
		case arrow.FLOAT64:
			val := col.(*array.Float64).Value(0)
			exprs[i] = expression.DatumToConstant(types.NewDatum(val), tmysql.TypeDouble, 0)
		case arrow.STRING:
			val := col.(*array.String).Value(0)
			exprs[i] = expression.DatumToConstant(types.NewDatum(val), tmysql.TypeVarString, 0)
		case arrow.BINARY:
			val := col.(*array.Binary).Value(0)
			exprs[i] = expression.DatumToConstant(types.NewDatum(val), tmysql.TypeVarString, 0)
		case arrow.DATE64:
			// Date64 is milliseconds since epoch
			val := col.(*array.Date64).Value(0)
			millis := int64(val)
			goTime := time.UnixMilli(millis)
			t := types.NewTime(types.FromGoTime(goTime), tmysql.TypeDatetime, 0)
			exprs[i] = expression.DatumToConstant(types.NewDatum(t), tmysql.TypeDatetime, 0)
		default:
			return nil, fmt.Errorf("unsupported Arrow type for parameter: %v", col.DataType())
		}
	}

	return exprs, nil
}

// paramValue represents a serializable parameter value
type paramValue struct {
	Kind  byte   // Type indicator
	IVal  int64  // For integers
	UVal  uint64 // For unsigned integers
	FVal  float64 // For floats
	SVal  string // For strings
	BVal  []byte // For binary data
	IsNull bool
}

// serializeExpressions serializes expressions to bytes for storage in prepared handle.
// Format: gob encoding of []paramValue
func serializeExpressions(exprs []expression.Expression) ([]byte, error) {
	if len(exprs) == 0 {
		return nil, nil
	}

	params := make([]paramValue, len(exprs))
	for i, expr := range exprs {
		if constant, ok := expr.(*expression.Constant); ok {
			params[i] = datumToParamValue(constant.Value)
		} else {
			return nil, fmt.Errorf("only constant expressions are supported for parameters")
		}
	}

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(params); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// deserializeExpressions deserializes bytes back to expressions.
func deserializeExpressions(data []byte) ([]expression.Expression, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var params []paramValue
	decoder := gob.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&params); err != nil {
		return nil, err
	}

	exprs := make([]expression.Expression, len(params))
	for i, p := range params {
		exprs[i] = paramValueToExpression(p)
	}

	return exprs, nil
}

// datumToParamValue converts a Datum to a serializable paramValue
func datumToParamValue(d types.Datum) paramValue {
	pv := paramValue{}

	if d.IsNull() {
		pv.IsNull = true
		return pv
	}

	pv.Kind = d.Kind()
	switch d.Kind() {
	case types.KindInt64:
		pv.IVal = d.GetInt64()
	case types.KindUint64:
		pv.UVal = d.GetUint64()
	case types.KindFloat32, types.KindFloat64:
		pv.FVal = d.GetFloat64()
	case types.KindString:
		pv.SVal = d.GetString()
	case types.KindBytes:
		pv.BVal = d.GetBytes()
	default:
		// For other types, convert to string
		pv.SVal = d.String()
		pv.Kind = types.KindString
	}

	return pv
}

// paramValueToDatum converts a paramValue back to a Datum
func paramValueToDatum(pv paramValue) types.Datum {
	if pv.IsNull {
		return types.NewDatum(nil)
	}

	switch pv.Kind {
	case types.KindInt64:
		return types.NewDatum(pv.IVal)
	case types.KindUint64:
		return types.NewDatum(pv.UVal)
	case types.KindFloat32, types.KindFloat64:
		return types.NewDatum(pv.FVal)
	case types.KindString:
		return types.NewDatum(pv.SVal)
	case types.KindBytes:
		return types.NewDatum(pv.BVal)
	default:
		return types.NewDatum(pv.SVal)
	}
}

// paramValueToExpression converts a paramValue to a properly typed expression.Constant
func paramValueToExpression(pv paramValue) expression.Expression {
	d := paramValueToDatum(pv)

	if d.IsNull() {
		return expression.NewNull()
	}

	// Determine MySQL type from the datum kind
	var mysqlType byte
	var flag uint

	switch pv.Kind {
	case types.KindInt64:
		mysqlType = tmysql.TypeLonglong
	case types.KindUint64:
		mysqlType = tmysql.TypeLonglong
		flag = tmysql.UnsignedFlag
	case types.KindFloat32:
		mysqlType = tmysql.TypeFloat
	case types.KindFloat64:
		mysqlType = tmysql.TypeDouble
	case types.KindString:
		mysqlType = tmysql.TypeVarString
	case types.KindBytes:
		mysqlType = tmysql.TypeVarString
	default:
		mysqlType = tmysql.TypeVarString
	}

	return expression.DatumToConstant(d, mysqlType, flag)
}
