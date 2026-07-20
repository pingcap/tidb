-- Campaign 27's deliberately bounded prepared point-read workload.
-- The real Go TiDB fixture owns setup; this script performs no DDL or DML.

sysbench.cmdline.options = {
   table_size = {"Number of pre-seeded rows", 16}
}

function thread_init()
   driver = sysbench.sql.driver()
   connection = driver:connect()
   statement = connection:prepare(
      "SELECT balance FROM campaign27.rows WHERE id = ?")
   parameter = statement:bind_create(sysbench.sql.type.BIGINT)
   statement:bind_param(parameter)
end

function event()
   local id = sysbench.rand.uniform(1, sysbench.opt.table_size)
   parameter:set(id)
   local result = statement:execute()
   -- sysbench 1.0.20's native MySQL prepared path exposes the buffered row
   -- count but intentionally leaves mysql_drv_fetch() unimplemented. Exact
   -- values are checked by the raw prepared client in the same live proof;
   -- here every real driver execution must still return exactly one row.
   if result.nrows ~= 1 then
      error(string.format(
         "prepared point read for id %d returned %d rows",
         id, result.nrows))
   end
   result:free()
end

function thread_done()
   statement:close()
   connection:disconnect()
end
