{
  description = "Dev environment for Spark";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            jdk11
            scala_2_12
            sbt
            metals
          ];

          shellHook = ''
            export JAVA_HOME=${pkgs.jdk11.home}
          '';
        };
      }
    );
}
