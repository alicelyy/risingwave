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
      in
      {
        packages =
          rust-toolchain
          ++ (with pkgs; [
            gcc
            lld

            gnumake
            cmake

            pkg-config
            protobuf
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
