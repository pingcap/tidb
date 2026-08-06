#!/usr/bin/env ruby
# frozen_string_literal: true

# Resolve comment-and-constant conflicts in the result-test ratchets. This is
# intentionally narrower than a general merge driver: unexpected code makes it
# stop instead of guessing which side to keep.

ALLOWED_CONSTANTS = %w[
  AGREED_MERGE_PAIRS
  BOTH_AGREE
  COMPARED
  EXTRA_MERGE_PAIRS
  KNOWN_DIVERGENCES
  RECORDED_MERGE_PAIRS
].freeze
CONSTANT = /^(\s*)const\s+([A-Z][A-Z0-9_]*):\s*usize\s*=\s*(\d+);\s*$/

def usage
  abort "usage: #{$PROGRAM_NAME} <rust-file> NAME=VALUE [NAME=VALUE ...]"
end

file_arg = ARGV.shift || usage
requested = {}
ARGV.each do |assignment|
  name, value = assignment.split("=", 2)
  usage unless ALLOWED_CONSTANTS.include?(name) && value&.match?(/\A\d+\z/)
  abort "duplicate stacked value for #{name}" if requested.key?(name)

  requested[name] = value
end
usage if requested.empty?

repo = File.expand_path("../..", __dir__)
file = File.expand_path(file_arg)
allowed_dir = File.join(repo, "rust/difftests/result-tests/tests/")
abort "refusing to edit outside #{allowed_dir}" unless file.start_with?(allowed_dir)
abort "not a Rust source file: #{file}" unless file.end_with?(".rs") && File.file?(file)

def trim_blank_edges(lines)
  lines = lines.drop_while { |line| line.strip.empty? }
  lines.reverse.drop_while { |line| line.strip.empty? }.reverse
end

def resolve_hunk(ours, theirs, requested)
  declarations = []
  narratives = [ours, theirs].map do |side|
    lines = side.each_with_object([]) do |line, kept|
      if (match = CONSTANT.match(line.chomp))
        name = match[2]
        abort "unsupported ratchet constant #{name}" unless ALLOWED_CONSTANTS.include?(name)

        declarations << [name, match[3], match[1]]
      elsif line.strip.empty? || line.lstrip.start_with?("//")
        kept << line
      else
        abort "conflict contains non-narrative code: #{line.strip}"
      end
    end
    trim_blank_edges(lines)
  end

  abort "conflict contains no supported ratchet constant" if declarations.empty?
  found = declarations.map(&:first).uniq
  missing = requested.keys - found
  abort "requested constant not present in conflict: #{missing.join(', ')}" unless missing.empty?

  indentation = declarations.map { |(_, _, indent)| indent }.uniq
  abort "ratchet constants use inconsistent indentation" unless indentation.one?

  resolved_values = {}
  found.each do |name|
    values = declarations.each_with_object([]) do |(candidate, value, _), matches|
      matches << value if candidate == name
    end.uniq
    if requested.key?(name)
      resolved_values[name] = requested.fetch(name)
    elsif values.one?
      resolved_values[name] = values.first
    else
      abort "#{name} differs across the conflict; pass #{name}=<stacked-value>"
    end
  end

  blocks = narratives.reject(&:empty?).uniq
  merged = []
  blocks.each_with_index do |block, index|
    if index.positive? && merged.last&.strip != "//" && block.first&.strip != "//"
      merged << "#{indentation.first}//\n"
    end
    merged.concat(block)
  end
  merged << "#{indentation.first}//\n" if !merged.empty? && merged.last.strip != "//"
  found.each do |name|
    merged << "#{indentation.first}const #{name}: usize = #{resolved_values.fetch(name)};\n"
  end
  [merged, resolved_values]
end

lines = File.readlines(file, encoding: "UTF-8")
output = []
resolved = {}
index = 0
conflicts = 0
while index < lines.length
  unless lines[index].start_with?("<<<<<<< ")
    abort "stray conflict marker at line #{index + 1}" if lines[index].start_with?("=======", ">>>>>>> ")
    output << lines[index]
    index += 1
    next
  end

  conflicts += 1
  separator = lines.index.with_index { |line, at| at > index && line.start_with?("=======") }
  abort "conflict at line #{index + 1} has no separator" unless separator
  finish = lines.index.with_index { |line, at| at > separator && line.start_with?(">>>>>>> ") }
  abort "conflict at line #{index + 1} has no end marker" unless finish

  merged, values = resolve_hunk(lines[(index + 1)...separator], lines[(separator + 1)...finish], requested)
  overlap = resolved.keys & values.keys
  abort "constant appears in multiple conflict hunks: #{overlap.join(', ')}" unless overlap.empty?
  resolved.merge!(values)
  output.concat(merged)
  index = finish + 1
end

abort "no conflict markers found in #{file}" if conflicts.zero?
missing = requested.keys - resolved.keys
abort "requested constant was not resolved: #{missing.join(', ')}" unless missing.empty?

original = lines.join
File.write(file, output.join, encoding: "UTF-8")

formatter = ENV["RATCHET_CONFLICT_FORMATTER"]
formatted = if formatter
              system(formatter, file)
            else
              system(
                "cargo", "fmt",
                "--manifest-path", File.join(repo, "rust/Cargo.toml"),
                "-p", "difftest-result-tests"
              )
            end
unless formatted
  File.write(file, original, encoding: "UTF-8")
  abort "formatter failed; restored original conflict"
end

puts "resolved #{conflicts} conflict(s) in #{file}"
puts resolved.map { |name, value| "  #{name}=#{value}" }
