-- Campaign 28's deliberately bounded prepared read+write workload for the
-- Stage E live proof. Every event performs one real prepared point read and one
-- real prepared arithmetic UPDATE through the Rust endpoint, exactly the mix the
-- ExecPlan requires (neither a text-query fallback nor --db-ps-mode=disable may
-- satisfy this gate). The real Go TiDB fixture owns DDL and row seeding; this
-- script performs no DDL and only the two frozen C28 prepared shapes.

sysbench.cmdline.options = {
   table_size = {"Number of pre-seeded rows", 16}
}

function thread_init()
   driver = sysbench.sql.driver()
   connection = driver:connect()

   -- One bounded C27 signed-BIGINT prepared point read.
   read_statement = connection:prepare(
      "SELECT balance FROM campaign28.accounts WHERE id = ?")
   read_id = read_statement:bind_create(sysbench.sql.type.BIGINT)
   read_statement:bind_param(read_id)

   -- One bounded C28 signed-BIGINT prepared arithmetic UPDATE.
   write_statement = connection:prepare(
      "UPDATE campaign28.accounts SET balance = balance + ? WHERE id = ?")
   write_addend = write_statement:bind_create(sysbench.sql.type.BIGINT)
   write_id = write_statement:bind_create(sysbench.sql.type.BIGINT)
   write_statement:bind_param(write_addend, write_id)
end

function event()
   local id = sysbench.rand.uniform(1, sysbench.opt.table_size)

   -- (1) A real prepared point read. sysbench 1.0.20's native MySQL prepared
   -- path exposes the buffered row count but leaves mysql_drv_fetch()
   -- unimplemented; the raw prepared client in the same live proof checks exact
   -- values, so here every real driver execution must return exactly one row.
   read_id:set(id)
   local result = read_statement:execute()
   if result.nrows ~= 1 then
      error(string.format(
         "prepared point read for id %d returned %d rows", id, result.nrows))
   end
   result:free()

   -- (2) A real prepared arithmetic UPDATE of the same row. The receipt counts
   -- its PD/TiKV operations and the resulting balances are verified
   -- independently through Go TiDB.
   write_addend:set(1)
   write_id:set(id)
   write_statement:execute()
end

function thread_done()
   read_statement:close()
   write_statement:close()
   connection:disconnect()
end
