{
  description = "CSCE 622: Distributed Systems";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.05";
  };

  outputs = inputs @ {
    self,
    nixpkgs,
    ...
  }: let
    inherit (nixpkgs) lib;
    systems = [
      "x86_64-linux"
      "x86_64-darwin"
      "aarch64-darwin"
    ];
    pkgsFor = lib.genAttrs systems (
      system: import nixpkgs {inherit system;}
    );
    forEachSystem = f: lib.genAttrs systems (system: f pkgsFor.${system});
  in {
    formatter = forEachSystem (pkgs: pkgs.alejandra);
    devShells = forEachSystem (pkgs: import ./shell.nix {inherit pkgs;});
  };
}
