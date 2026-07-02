#!/usr/bin/env bash
# Eval in the running nREPL via clj-nrepl-eval (clojure-mcp-light, a sibling clone).
# Usage: scripts/cljeval.sh '(+ 1 2)'   OR   scripts/cljeval.sh <<'EOF' … EOF
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WS="$(cd "$REPO/.." && pwd)"
MCP="$WS/clojure-mcp-light/src"
# shellcheck source=/dev/null
source "$REPO/scripts/_dev.sh"
[ -d "$MCP" ] || { echo "no $MCP — clone bhauman/clojure-mcp-light beside the repo" >&2; exit 1; }
[ -f "$REPO/.nrepl-port" ] || { echo "no .nrepl-port — start scripts/nrepl.sh" >&2; exit 1; }
cd "$REPO"
in_dev bb -cp "$MCP" -m clojure-mcp-light.nrepl-eval -p "$(cat "$REPO/.nrepl-port")" "$@"
