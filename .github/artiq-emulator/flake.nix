{
  description = "Environment for running ndscan tests using the core device emulator";

  inputs = {
    artiq.url = "github:dnadlinger/artiq?ref=dpn/emulator";
    src-oitg = {
      url = "github:OxfordIonTrapGroup/oitg";
      flake = false;
    };
  };
  outputs = { self, artiq, src-oitg }:
    let
      nixpkgs = artiq.nixpkgs;
      sipyco = artiq.inputs.sipyco;
      libartiq-emulator = artiq.packages.x86_64-linux.libartiq-emulator;
      artiqpkg = artiq.packages.x86_64-linux.artiq;
      oitg = nixpkgs.python3Packages.buildPythonPackage {
        name = "oitg";
        src = src-oitg;
        format = "pyproject";
        propagatedBuildInputs = with nixpkgs.python3Packages; [
          h5py
          scipy
          statsmodels
          nixpkgs.python3Packages.poetry-core
          nixpkgs.python3Packages.poetry-dynamic-versioning
        ];
        # Whatever magic `setup.py test` does by default fails for oitg.
        installCheckPhase = ''
          ${nixpkgs.python3.interpreter} -m unittest discover test
        '';
      };
    in {
      devShells.x86_64-linux.default = nixpkgs.mkShell {
        name = "ndscan-dev-shell";
        buildInputs = [ artiqpkg libartiq-emulator oitg ];
        shellHook = ''
          export LIBARTIQ_EMULATOR=${libartiq-emulator}/lib/libartiq_emulator.so
        '';
      };
    };

  nixConfig = {
    extra-trusted-public-keys =
      "nixbld.m-labs.hk-1:5aSRVA5b320xbNvu30tqxVPXpld73bhtOeH6uAjRyHc=";
    extra-substituters = "https://nixbld.m-labs.hk";
  };
}
