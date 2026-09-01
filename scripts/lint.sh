#!/usr/bin/env bash
# lint.sh — run the house lint over weft's sources.
#
# The linter is loom's, vendored under tools/lint as its own package (it
# carries glance/glexer/simplifile deps the weft library should not).
# The rules: R0 unparseable source, R1 eager fallbacks, R2 `case`
# nesting depth, R3 catch-all patterns, R4 `panic`/`let assert` in src,
# R5 O(n) answers to bounded questions, R7 a `let assert` naming no
# invariant, R8 moved pyramids. R6 (loom's portable-subset rule) keys on
# loom package paths and never fires here. R0, R2, R4 gate; the rest
# warn and are printed for reading, which is the only thing that makes
# them useful.
#
# The vendored copy is expected to spin out into its own repo later;
# keep local changes to it at zero so the spin-out is a `git mv`.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(pwd)

# The linter's last line is `# <errors> <warnings>`; a non-zero error
# count is the gate, warnings are advisory.
output=$(cd "$root/tools/lint" && gleam run -m lint/cli -- "$root/src")
printf '%s\n' "$output"

errors=$(printf '%s\n' "$output" | tail -n 1 | awk '{print $2}')
[ "${errors:-0}" -eq 0 ] || exit 1
