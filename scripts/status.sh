#!/usr/bin/env bash
# RUN FIRST on resume: reports whether the nREPL is LIVE (via a real eval round-trip).
set -uo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"
[ -f .nrepl-port ] || { echo "nREPL: DOWN (no .nrepl-port). Start: scripts/nrepl.sh"; exit 0; }
echo "nREPL: port $(cat .nrepl-port)"
OUT="$("$REPO/scripts/cljeval.sh" '(+ 40 2)' 2>/dev/null | grep -E '^=>' | head -1 || true)"
[ "$OUT" = "=> 42" ] && echo "nREPL: LIVE (eval round-trip OK)" \
                     || echo "nREPL: stale/dead port — restart scripts/nrepl.sh"
