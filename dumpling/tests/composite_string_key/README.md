# Composite String Key Integration Test

This test verifies that dumpling correctly handles tables with composite string primary keys using the new streaming string key chunking functionality.

## Test Cases

### comp_str_case_0: Basic Composite String Key
- **Table**: `tenant_id` + `user_id` composite primary key
- **Data**: 10 rows with tenant/user combinations
- **Purpose**: Test basic composite string key chunking with varchar columns

### comp_str_case_1: Special Characters and Edge Cases  
- **Table**: `category` + `item_id` composite primary key
- **Data**: Categories with special characters like spaces, quotes, ampersands
- **Purpose**: Test SQL escaping and special character handling in composite keys

### comp_str_case_2: Three-Column Composite Key
- **Table**: `region` + `country` + `city` composite primary key 
- **Data**: Geographical data with three-level hierarchy
- **Purpose**: Test complex composite keys with more than 2 columns

### comp_str_case_3: Unicode and Emoji Characters
- **Table**: `lang_code` + `message_id` composite primary key
- **Data**: Multi-language content with Unicode and emoji characters
- **Purpose**: Test international character support in string chunking

## Key Features Tested

1. **Composite Key Detection**: Verifies dumpling detects multi-column string primary keys
2. **Boundary Generation**: Tests cursor-based boundary sampling for efficient chunking
3. **WHERE Clause Generation**: Validates complex OR-based WHERE clauses for composite keys
4. **Progress Tracking**: Ensures streaming chunking progress is properly tracked
5. **SQL Escaping**: Tests proper escaping of special characters in boundary values
6. **Chunk Ordering**: Verifies data is dumped in correct primary key order

## Expected Behavior

- Dumpling should automatically detect the composite string primary key
- Tables should be chunked using streaming string key chunking (with --rows 5)
- Each chunk should contain properly ordered data based on the composite key
- Progress tracking should work correctly for streaming chunks
- All special characters should be properly escaped in SQL

## Running the Test

```bash
# Run the specific test
cd dumpling/tests
./run.sh composite_string_key

# Or run all dumpling integration tests
make dumpling_integration_test
```

The test will:
1. Create the test database and tables
2. Insert test data with various composite string key scenarios
3. Run dumpling with chunking enabled (--rows 5)
4. Verify the output matches expected results
5. Clean up test artifacts