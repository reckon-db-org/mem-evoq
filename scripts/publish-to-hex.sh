#!/usr/bin/env bash
#
# Publish mem-evoq to hex.pm.
#
# Runs the full local quality gate (compile, three test runners, doc
# build), then prompts before triggering the irreversible publish.
# Hex allows edits within the first hour only — better to catch
# issues here than there.
#
# Usage:
#   ./scripts/publish-to-hex.sh           # interactive (prompts before publish)
#   ./scripts/publish-to-hex.sh --yes     # skip prompt (CI / scripted use)

set -euo pipefail

cd "$(dirname "$0")/.."

NON_INTERACTIVE=0
if [ "${1:-}" = "--yes" ] || [ "${1:-}" = "-y" ]; then
    NON_INTERACTIVE=1
fi

VERSION=$(grep -E '^\s*\{vsn,' src/mem_evoq.app.src | sed -E 's/.*"([^"]+)".*/\1/')
echo "==> mem-evoq vsn from .app.src: ${VERSION}"

if ! head -20 CHANGELOG.md | grep -q "^## \[${VERSION}\]"; then
    echo
    echo "!! CHANGELOG.md does not contain a heading for [${VERSION}]."
    echo "!! Add a release entry before publishing."
    exit 1
fi

echo
echo "==> Compiling..."
rebar3 compile

echo
echo "==> Running EUnit..."
rebar3 eunit

echo
echo "==> Running Common Test..."
rebar3 ct

echo
echo "==> Running PropEr..."
rebar3 proper

echo
echo "==> Building ex_doc (must pass — EDoc errors silently break hexdocs)..."
rebar3 ex_doc

echo
echo "==> All checks green for mem-evoq ${VERSION}."

if [ "${NON_INTERACTIVE}" -eq 0 ]; then
    echo
    echo "Publishing to hex.pm is IRREVERSIBLE after the 1-hour edit window."
    read -r -p "Proceed? [y/N] " ANS
    case "${ANS}" in
        y|Y|yes|YES) ;;
        *) echo "Aborted."; exit 1 ;;
    esac
fi

echo
echo "==> Publishing to hex.pm..."
rebar3 hex publish

echo
echo "==> Done. mem-evoq ${VERSION} published."
echo "    Hex:     https://hex.pm/packages/mem_evoq/${VERSION}"
echo "    Hexdocs: https://hexdocs.pm/mem_evoq/${VERSION}"
echo
echo "    Don't forget to tag + push: git tag v${VERSION} && git push --tags"
