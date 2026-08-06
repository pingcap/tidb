#!/usr/bin/env ruby
# frozen_string_literal: true

# Re-census the Rust sysvar catalog by behavioral use, not by whether SQL can
# echo a stored value through SELECT @@x. Run from any directory.

repo = File.expand_path(ARGV.fetch(0, File.join(__dir__, "../../..")))

def read(repo, relative)
  File.read(File.join(repo, relative), encoding: "UTF-8")
end

catalog_files = Dir.glob(
  File.join(repo, "rust/crates/tidb-session/src/sysvar/catalog/**/*.rs")
)
definitions = {}
catalog_files.each do |path|
  File.read(path, encoding: "UTF-8").scan(/SysVarDef\s*\{(.*?)\n\s*\}/m).each do |match|
    block = match.fetch(0)
    name = block[/\bname:\s*"([^"]+)"/, 1]
    next unless name

    definitions[name] = {
      scope: block[/\bscope:\s*([^,]+),/, 1].to_s.strip,
      read_only: block[/\bread_only:\s*(true|false)/, 1] == "true"
    }
  end
end

abort "expected 948 catalog entries, found #{definitions.length}" unless definitions.length == 948

constants = {}
read(repo, "rust/crates/tidb-vardef/src/tidb_vars.rs").scan(
  /\bpub\s+const\s+([A-Z][A-Z0-9_]*)\s*:\s*&str\s*=\s*"([^"]+)"/
) do |constant, value|
  constants[constant] = value if definitions.key?(value)
end

# Direct get_system/get_global calls are runtime reads except for the two
# switches whose only consumer is SET validation/routing. Dynamic names are
# deliberately ignored: SELECT @@x and SHOW VARIABLES can echo every entry,
# but that is storage visibility, not a behavioral consumer.
direct_reads = {}
Dir.glob(File.join(repo, "rust/crates/**/*.rs")).each do |path|
  parts = path.delete_prefix("#{repo}/").split("/")
  next if path.include?("/sysvar/catalog/") || path.include?("/benches/") ||
          parts.any? { |part| part == "tests" || part.start_with?("tests_") }

  text = File.read(path, encoding: "UTF-8").split(/^#\[cfg\(test\)\]/, 2).first
  text.scan(
    /\bget_(?:system|global|instance)\s*\(\s*(?:"([^"]+)"|(?:[A-Za-z0-9_]+::)*([A-Z][A-Z0-9_]*))/
  ) do |literal, constant|
    name = literal || constants[constant]
    direct_reads[name] = true if definitions.key?(name)
  end
end

runtime_helper_evidence = {
  "auto_increment_increment" => ["rust/crates/tidb-session/src/stmt_ctx.rs", 'read("auto_increment_increment")'],
  "auto_increment_offset" => ["rust/crates/tidb-session/src/stmt_ctx.rs", 'read("auto_increment_offset")'],
  "identity" => ["rust/crates/tidb-session/src/variables.rs", 'name.eq_ignore_ascii_case("identity")'],
  "last_insert_id" => ["rust/crates/tidb-session/src/variables.rs", 'name.eq_ignore_ascii_case("last_insert_id")'],
  "last_plan_from_binding" => ["rust/crates/tidb-session/src/variables.rs", 'name.eq_ignore_ascii_case("last_plan_from_binding")'],
  "last_plan_from_cache" => ["rust/crates/tidb-session/src/variables.rs", 'name.eq_ignore_ascii_case("last_plan_from_cache")'],
  "rand_seed1" => ["rust/crates/tidb-session/src/variables.rs", 'name.eq_ignore_ascii_case("rand_seed1")'],
  "rand_seed2" => ["rust/crates/tidb-session/src/variables.rs", 'name.eq_ignore_ascii_case("rand_seed2")'],
  "tidb_enable_non_prepared_plan_cache" => ["rust/crates/tidb-session/src/non_prepared_plan_cache.rs", 'session_bool("tidb_enable_non_prepared_plan_cache"'],
  "tidb_enable_plan_cache_for_param_limit" => ["rust/crates/tidb-session/src/non_prepared_plan_cache.rs", 'session_bool("tidb_enable_plan_cache_for_param_limit"'],
  "tidb_use_plan_baselines" => ["rust/crates/tidb-session/src/binding_arm.rs", 'session_bool("tidb_use_plan_baselines"']
}.freeze

set_only_evidence = {
  "offline_mode" => ["rust/crates/tidb-session/src/sysvar.rs", '("offline_mode", true)'],
  "read_only" => ["rust/crates/tidb-session/src/sysvar.rs", '("read_only", false)'],
  "super_read_only" => ["rust/crates/tidb-session/src/sysvar.rs", '("super_read_only", false)'],
  "tidb_capture_plan_baselines" => ["rust/crates/tidb-session/src/sysvar.rs", '"tidb_capture_plan_baselines"'],
  "tidb_enable_fast_analyze" => ["rust/crates/tidb-session/src/variables.rs", 'name.eq_ignore_ascii_case("tidb_enable_fast_analyze")'],
  "tidb_enable_legacy_instance_scope" => ["rust/crates/tidb-session/src/variables.rs", 'get_system("tidb_enable_legacy_instance_scope")'],
  "tidb_enable_list_partition" => ["rust/crates/tidb-session/src/sysvar.rs", 'self.name == "tidb_enable_list_partition"'],
  "tidb_enable_table_partition" => ["rust/crates/tidb-session/src/sysvar.rs", 'self.name == "tidb_enable_table_partition"'],
  "tidb_prepared_plan_cache_size" => ["rust/crates/tidb-session/src/sysvar.rs", '"tidb_prepared_plan_cache_size"'],
  "tidb_session_alias" => ["rust/crates/tidb-session/src/sysvar.rs", 'self.name == "tidb_session_alias"'],
  "tidb_session_plan_cache_size" => ["rust/crates/tidb-session/src/sysvar.rs", '"tidb_session_plan_cache_size"'],
  "tidb_skip_isolation_level_check" => ["rust/crates/tidb-session/src/variables.rs", 'get_system("tidb_skip_isolation_level_check")'],
  "transaction_isolation" => ["rust/crates/tidb-session/src/variables.rs", 'name.eq_ignore_ascii_case("transaction_isolation")'],
  "transaction_read_only" => ["rust/crates/tidb-session/src/sysvar.rs", '("transaction_read_only", false)'],
  "tx_isolation" => ["rust/crates/tidb-session/src/variables.rs", 'name.eq_ignore_ascii_case("tx_isolation")'],
  "tx_read_only" => ["rust/crates/tidb-session/src/sysvar.rs", '("tx_read_only", false)']
}.freeze

(runtime_helper_evidence.merge(set_only_evidence)).each do |name, (path, needle)|
  abort "census evidence for #{name} is stale: #{path} lacks #{needle.inspect}" unless read(repo, path).include?(needle)
  abort "census class names missing registry entry: #{name}" unless definitions.key?(name)
end

set_only = set_only_evidence.keys.sort
runtime = ((direct_reads.keys - set_only) + runtime_helper_evidence.keys).uniq.sort
overlap = runtime & set_only
abort "census classes overlap: #{overlap.join(', ')}" unless overlap.empty?

unread = (definitions.keys - runtime - set_only).sort
writable = lambda do |name|
  definition = definitions.fetch(name)
  definition[:scope] != "0" && !definition[:read_only]
end

puts "census: declared=#{definitions.length} runtime_behavior=#{runtime.length} " \
     "set_or_validation_only=#{set_only.length} behaviorally_unread=#{unread.length} " \
     "sum=#{runtime.length + set_only.length + unread.length}"
puts "writability: writable_declared=#{definitions.keys.count(&writable)} " \
     "writable_behaviorally_unread=#{unread.count(&writable)} " \
     "read_only_or_scope_none_unread=#{unread.count { |name| !writable.call(name) }}"
puts "runtime_behavior: #{runtime.join(',')}"
puts "set_or_validation_only: #{set_only.join(',')}"

priority = %w[
  character_set_client max_execution_time transaction_isolation tx_isolation
  tidb_retry_limit tidb_disable_txn_auto_retry tidb_max_chunk_size
  tidb_init_chunk_size tidb_replica_read tidb_request_source_type
  tidb_scatter_region
]
puts "priority_status:"
priority.each do |name|
  category = if runtime.include?(name)
               "runtime_behavior"
             elsif set_only.include?(name)
               "set_or_validation_only"
             else
               "behaviorally_unread"
             end
  puts "  #{name}=#{category} writable=#{writable.call(name)}"
end
