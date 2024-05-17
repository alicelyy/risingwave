{
  perSystem = { pkgs, lib, ... }: {
    devshells.default =
      let
        rust-toolchain = with pkgs;
          [
            ((rust-bin.fromRustupToolchainFile ../rust-toolchain).override
              {
                extensions = [ "rust-src" "rust-analyzer" ];
              })
          ] ++ [
            cargo-make
            cargo-nextest
            cargo-binstall
            cargo-sort
            typos
          ];
        inherit (pkgs.stdenv) isDarwin isLinux;
      in
      {
        packages = with pkgs;
          rust-toolchain
          ++ [
            gnumake
            cmake

            pkg-config
            protobuf
          ] ++ (lib.optionals isDarwin [
            clang
          ]) ++ (lib.optionals isLinux [
            gcc
          ]);

        env = [
          {
            name = "LD_LIBRARY_PATH";
            value = lib.makeLibraryPath (with pkgs; [ stdenv.cc.cc ]);
          }
        ];
      };
  };
}
