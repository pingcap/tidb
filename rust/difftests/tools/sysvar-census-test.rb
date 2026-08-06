#!/usr/bin/env ruby
# frozen_string_literal: true

# The census is a measurement, but a stale measurement is still a false
# claim. Keep the current quoted output under a small standard-library gate so
# a source or classifier drift cannot silently leave the operations report
# advertising old numbers.

require "minitest/autorun"
require "open3"

class SysvarCensusTest < Minitest::Test
  ROOT = File.expand_path("../../..", __dir__)
  SCRIPT = File.join(ROOT, "rust/difftests/tools/sysvar-census.rb")

  def census_output
    stdout, stderr, status = Open3.capture3(
      { "LC_ALL" => "en_US.UTF-8", "LANG" => "en_US.UTF-8" },
      "ruby",
      "-EUTF-8:UTF-8",
      SCRIPT,
      ROOT
    )
    assert status.success?, "census failed:\n#{stdout}\n#{stderr}"
    stdout
  end

  def test_current_source_counts_are_pinned
    output = census_output
    assert_includes output,
                    "census: declared=948 runtime_behavior=42 set_or_validation_only=16 " \
                    "behaviorally_unread=890 sum=948"
    assert_includes output,
                    "writability: writable_declared=785 writable_behaviorally_unread=730 " \
                    "read_only_or_scope_none_unread=160"
  end

  def test_priority_classification_is_present
    output = census_output
    assert_includes output, "tidb_retry_limit=behaviorally_unread writable=true"
    assert_includes output, "transaction_isolation=set_or_validation_only writable=true"
    assert_includes output, "tidb_scatter_region=behaviorally_unread writable=true"
  end
end
