#!/usr/bin/env ruby
# frozen_string_literal: true

require "fileutils"
require "minitest/autorun"
require "tmpdir"

class ResolveRatchetConflictTest < Minitest::Test
  SCRIPT = File.expand_path("../resolve-ratchet-conflict.rb", __dir__)

  def setup
    @repo = File.expand_path("../../..", __dir__)
    @test_dir = File.join(@repo, "rust/difftests/result-tests/tests")
    @formatter = File.join(Dir.mktmpdir("ratchet-formatter"), "formatter")
    File.write(
      @formatter,
      "#!/bin/sh\nprintf '%s' \"$1\" > \"$RATCHET_FORMAT_MARKER\"\n"
    )
    FileUtils.chmod("u+x", @formatter)
    @created = []
  end

  def teardown
    @created.each { |path| FileUtils.rm_f(path) }
    FileUtils.rm_rf(File.dirname(@formatter))
  end

  def resolve(source, *values)
    file = File.join(@test_dir, "zz_ratchet_conflict_#{Process.pid}_#{@created.length}.rs")
    marker = "#{file}.formatted"
    @created.concat([file, marker])
    File.write(file, source)
    env = {
      "RATCHET_CONFLICT_FORMATTER" => @formatter,
      "RATCHET_FORMAT_MARKER" => marker
    }
    assert system(env, SCRIPT, file, *values), "resolver failed"
    assert_equal file, File.read(marker)
    File.read(file)
  end

  def test_preserves_same_named_constants_outside_the_conflict
    resolved = resolve(<<~'RUST', "KNOWN_DIVERGENCES=80")
      fn unrelated_gate() {
          const KNOWN_DIVERGENCES: usize = 1;
      }

      fn conflicted_gate() {
      <<<<<<< HEAD
          // Existing stack measurement.
          const KNOWN_DIVERGENCES: usize = 77;
      =======
          // Incoming batch measurement.
          const KNOWN_DIVERGENCES: usize = 75;
      >>>>>>> incoming
      }
    RUST

    assert_includes resolved, "const KNOWN_DIVERGENCES: usize = 1;"
    assert_includes resolved, "const KNOWN_DIVERGENCES: usize = 80;"
    assert_equal 2, resolved.scan("const KNOWN_DIVERGENCES").length
  end

  def test_restores_the_conflict_when_formatting_fails
    source = <<~'RUST'
      fn gate() {
      <<<<<<< HEAD
          const KNOWN_DIVERGENCES: usize = 77;
      =======
          const KNOWN_DIVERGENCES: usize = 75;
      >>>>>>> incoming
      }
    RUST
    file = File.join(@test_dir, "zz_ratchet_conflict_#{Process.pid}_#{@created.length}.rs")
    @created << file
    File.write(file, source)
    env = { "RATCHET_CONFLICT_FORMATTER" => "/usr/bin/false" }

    refute system(env, SCRIPT, file, "KNOWN_DIVERGENCES=80")
    assert_equal source, File.read(file)
  end

  def test_merges_narratives_and_deduplicates_the_integration_ratchet
    resolved = resolve(<<~'RUST', "KNOWN_DIVERGENCES=80")
      fn gate() {
      <<<<<<< HEAD
          // Existing stack measurement.
          const KNOWN_DIVERGENCES: usize = 77;
      =======
          // Incoming batch measurement.
          const KNOWN_DIVERGENCES: usize = 75;
          const KNOWN_DIVERGENCES: usize = 75;
      >>>>>>> incoming
      }
    RUST

    assert_includes resolved, "// Existing stack measurement."
    assert_includes resolved, "// Incoming batch measurement."
    assert_equal 1, resolved.scan("const KNOWN_DIVERGENCES").length
    assert_includes resolved, "const KNOWN_DIVERGENCES: usize = 80;"
    refute_match(/^(?:<<<<<<<|=======|>>>>>>>)/, resolved)
  end

  def test_accepts_all_stacked_join_shape_values_in_one_conflict
    resolved = resolve(
      <<~'RUST',
        fn gate() {
        <<<<<<< HEAD
            // Existing join-shape narrative.
            const COMPARED: usize = 229;
            const BOTH_AGREE: usize = 141;
            const RECORDED_MERGE_PAIRS: usize = 87;
            const AGREED_MERGE_PAIRS: usize = 81;
            const EXTRA_MERGE_PAIRS: usize = 5;
        =======
            // Incoming join-shape narrative.
            const COMPARED: usize = 227;
            const BOTH_AGREE: usize = 139;
            const RECORDED_MERGE_PAIRS: usize = 86;
            const AGREED_MERGE_PAIRS: usize = 80;
            const EXTRA_MERGE_PAIRS: usize = 4;
        >>>>>>> incoming
        }
      RUST
      "COMPARED=232",
      "BOTH_AGREE=149",
      "RECORDED_MERGE_PAIRS=88",
      "AGREED_MERGE_PAIRS=82",
      "EXTRA_MERGE_PAIRS=5"
    )

    assert_includes resolved, "// Existing join-shape narrative."
    assert_includes resolved, "// Incoming join-shape narrative."
    assert_equal 1, resolved.scan("const COMPARED").length
    assert_equal 1, resolved.scan("const BOTH_AGREE").length
    assert_equal 1, resolved.scan("const RECORDED_MERGE_PAIRS").length
    assert_equal 1, resolved.scan("const AGREED_MERGE_PAIRS").length
    assert_equal 1, resolved.scan("const EXTRA_MERGE_PAIRS").length
    assert_includes resolved, "const COMPARED: usize = 232;"
    assert_includes resolved, "const BOTH_AGREE: usize = 149;"
    assert_includes resolved, "const RECORDED_MERGE_PAIRS: usize = 88;"
    assert_includes resolved, "const AGREED_MERGE_PAIRS: usize = 82;"
    assert_includes resolved, "const EXTRA_MERGE_PAIRS: usize = 5;"
  end
end
