package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
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
	panic("GetFlightInfoSubstraitPlan not implemented")

}

func (s *FlightSQLServer) GetSchemaStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	panic("GetSchemaStatement not implemented")

}

func (s *FlightSQLServer) GetSchemaSubstraitPlan(ctx context.Context, plan flightsql.StatementSubstraitPlan, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	panic("GetSchemaSubstraitPlan not implemented")

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
	panic("GetFlightInfoPreparedStatement not implemented")

}

func (s *FlightSQLServer) GetSchemaPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	panic("GetSchemaPreparedStatement not implemented")

}

func (s *FlightSQLServer) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	panic("DoGetPreparedStatement not implemented")

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
	panic("DoPutCommandStatementUpdate not implemented")

}

func (s *FlightSQLServer) DoPutCommandSubstraitPlan(ctx context.Context, plan flightsql.StatementSubstraitPlan) (int64, error) {
	panic("DoPutCommandSubstraitPlan not implemented")

}

func (s *FlightSQLServer) CreatePreparedStatement(ctx context.Context, cmd flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	panic("CreatePreparedStatement not implemented")

}

func (s *FlightSQLServer) CreatePreparedSubstraitPlan(ctx context.Context, cmd flightsql.ActionCreatePreparedSubstraitPlanRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	panic("CreatePreparedSubstraitPlan not implemented")

}

func (s *FlightSQLServer) ClosePreparedStatement(ctx context.Context, cmd flightsql.ActionClosePreparedStatementRequest) error {
	panic("ClosePreparedStatement not implemented")

}

func (s *FlightSQLServer) DoPutPreparedStatementQuery(ctx context.Context, cmd flightsql.PreparedStatementQuery, r flight.MessageReader, w flight.MetadataWriter) ([]byte, error) {
	panic("DoPutPreparedStatementQuery not implemented")

}

func (s *FlightSQLServer) DoPutPreparedStatementUpdate(ctx context.Context, cmd flightsql.PreparedStatementUpdate, r flight.MessageReader) (int64, error) {
	panic("DoPutPreparedStatementUpdate not implemented")

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
