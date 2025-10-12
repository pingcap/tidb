package main

import (
	"fmt"
	"log"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/store/mockstore"
)

// DemoMetrics demonstrates the new Prometheus metrics functionality
func DemoMetrics() {
	fmt.Println("=== TiDB Prometheus Metrics Demo ===")
	
	// Initialize metrics (this would normally be done by the server)
	metrics.InitMetrics()
	metrics.RegisterMetrics()
	
	// Create a mock store and domain (simplified for demo)
	store, err := mockstore.NewMockStore()
	if err != nil {
		log.Fatal("Failed to create mock store:", err)
	}
	defer store.Close()
	
	dom, err := domain.NewDomain(store, 0, 0, 0)
	if err != nil {
		log.Fatal("Failed to create domain:", err)
	}
	defer dom.Close()
	
	// Get the current InfoSchema
	is := dom.InfoSchema()
	
	// Demonstrate the new methods
	fmt.Println("\n1. Database Count:")
	dbCount := is.GetDatabaseCount()
	fmt.Printf("   Number of databases: %d\n", dbCount)
	
	fmt.Println("\n2. Table Count:")
	tableCount := is.GetTableCount()
	fmt.Printf("   Total number of tables: %d\n", tableCount)
	
	fmt.Println("\n3. Schema Version:")
	schemaVersion := is.GetSchemaVersion()
	fmt.Printf("   Current schema version: %d\n", schemaVersion)
	
	// Update metrics (simulating the server's updateMetrics function)
	fmt.Println("\n4. Updating Prometheus Metrics:")
	metrics.DatabaseCount.Set(float64(dbCount))
	metrics.TableCount.Set(float64(tableCount))
	metrics.SchemaVersion.Set(float64(schemaVersion))
	
	fmt.Println("   ✓ Database count metric updated")
	fmt.Println("   ✓ Table count metric updated") 
	fmt.Println("   ✓ Schema version metric updated")
	
	fmt.Println("\n5. Prometheus Metrics Output:")
	fmt.Println("   The following metrics are now available at http://localhost:10080/metrics:")
	fmt.Printf("   tidb_server_database_count %d\n", dbCount)
	fmt.Printf("   tidb_server_table_count %d\n", tableCount)
	fmt.Printf("   tidb_server_schema_version %d\n", schemaVersion)
	
	fmt.Println("\n6. METRICS_SCHEMA Queries:")
	fmt.Println("   You can also query these metrics via SQL:")
	fmt.Println("   SELECT * FROM metrics_schema.database_count;")
	fmt.Println("   SELECT * FROM metrics_schema.table_count;")
	fmt.Println("   SELECT * FROM metrics_schema.schema_version;")
	
	fmt.Println("\n=== Demo Complete ===")
}

func main() {
	DemoMetrics()
}
