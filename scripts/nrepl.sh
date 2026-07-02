#!/usr/bin/env bash
# Start the long-lived nREPL server — run this in the BACKGROUND.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$REPO/scripts/_dev.sh"
cd "$REPO"
rm -f .nrepl-port
in_dev clojure -M:nrepl       # writes ./.nrepl-port; let nREPL pick the port
