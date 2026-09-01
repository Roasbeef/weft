#!/usr/bin/env bash
# doc_check.sh — the doc graph's gate, in miniature.
#
# Weft follows loom's documentation discipline: every directory that
# carries a CLAUDE.md carries an AGENTS.md that is a byte-identical
# mirror produced by `cp`, never hand-edited. This script enforces the
# mirror and checks that every public module opens with `////` module
# documentation. Coverage of individual functions is reviewed rather
# than gated: a gate that counts `///` lines rewards padding.
set -euo pipefail
cd "$(dirname "$0")/.."

failures=0

# Every CLAUDE.md must have its AGENTS.md mirror, byte for byte.
while IFS= read -r claude; do
	agents="${claude%CLAUDE.md}AGENTS.md"
	if [ ! -f "$agents" ]; then
		echo "doc-check: missing mirror $agents"
		failures=$((failures + 1))
	elif ! cmp -s "$claude" "$agents"; then
		echo "doc-check: $agents is not a byte-identical mirror of $claude"
		failures=$((failures + 1))
	fi
done < <(find . -path ./build -prune -o -path ./tools/lint/build -prune \
	-o -name CLAUDE.md -print)

# Every non-internal module opens with //// module documentation.
while IFS= read -r module; do
	case "$module" in
	*/internal/*) continue ;;
	esac
	if ! head -n 1 "$module" | grep -q '^////'; then
		echo "doc-check: $module has no //// module documentation"
		failures=$((failures + 1))
	fi
done < <(find src -name '*.gleam' -print 2>/dev/null)

if [ "$failures" -gt 0 ]; then
	echo "doc-check: $failures failure(s)"
	exit 1
fi
echo "doc graph clean"
