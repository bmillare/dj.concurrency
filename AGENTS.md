# dj.concurrency — Agent Notes

Small, dependency-free Clojure library (manageable futures). Source is three
namespaces under `src/dj/`: `concurrency.clj` (facade), `concurrency/shell.clj`
(impure runtime), `concurrency/policy.clj` (pure reference policy). Tests in
`test/`, dev-only demo in `dev/`.

Tooling is Nix-based (`nix develop`); `clojure`/`bb` come from the flake, not
your PATH. The library itself has no runtime deps — all tooling lives in
dev-only aliases in `deps.edn`.

## Clojure dev workflow (REPL-light)

Prefer one long-lived nREPL; reload in place, never restart the JVM per change.
`clj-nrepl-eval` comes from `../clojure-mcp-light` (a sibling clone — re-clone
`https://github.com/bhauman/clojure-mcp-light.git` beside this repo if missing).
Scripts in `scripts/` auto-detect Nix, so they work with or without it:

- `scripts/status.sh` — RUN FIRST on resume (reports LIVE + port via a real eval).
- `scripts/nrepl.sh` — start the long-lived server (run in the background;
  writes `./.nrepl-port`).
- `scripts/cljeval.sh '(form)'` / heredoc — eval in the running nREPL.

Tests: `clojure -X:test` (or `nix develop --command clojure -X:test`).

Full runbook (in the agent workspace repo):
`../agent/ledger/2026-06-30-clojure-repl-light-runbook.md`.
