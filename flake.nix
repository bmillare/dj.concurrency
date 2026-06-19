{
  description = "dj.concurrency — manageable futures for Clojure";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            # jdk is JDK 21+ on nixos-unstable; required for the virtual
            # threads (Thread/startVirtualThread) used in dj.concurrency.
            packages = with pkgs; [
              clojure
              babashka
              jdk
            ];

            shellHook = ''
              echo "dj.concurrency dev shell ready. Run: clj"
            '';
          };
        });
    };
}
