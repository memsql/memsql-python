with import <nixpkgs> {}; {
  memsqlPythonEnv = stdenv.mkDerivation {
    name = "memsql-python-env";
    buildInputs = with python27Packages; [
      python27Full
      python27Packages.virtualenv
      mysql55
      zlib
      openssl
    ];
    shellHook = ''
      [ -d venv ] || virtualenv venv
      source venv/bin/activate
    '';
  };
}
