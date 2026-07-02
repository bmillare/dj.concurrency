#!/usr/bin/env bash
# Run a command in the project's dev environment: directly if clojure+bb are on
# PATH, otherwise through the Nix flake devshell.
in_dev() {
  if command -v clojure >/dev/null 2>&1 && command -v bb >/dev/null 2>&1; then
    "$@"
  else
    nix --extra-experimental-features 'nix-command flakes' develop --command "$@"
  fi
}
