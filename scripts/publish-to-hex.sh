#!/usr/bin/env bash
set -euo pipefail

# Publish mem-evoq to hex.pm
# Usage: ./scripts/publish-to-hex.sh

cd "$(dirname "$0")/.."

echo "==> Building mem-evoq..."
rebar3 compile

echo "==> Running tests..."
rebar3 eunit

echo "==> Building docs..."
rebar3 ex_doc

echo "==> Publishing to hex.pm..."
rebar3 hex publish

echo "==> Done! mem-evoq published to hex.pm"
