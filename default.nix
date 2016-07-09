with import <nixpkgs> {}; {
  memsqlPythonEnv = stdenv.mkDerivation {
    name = "memsql-python-env";
    buildInputs = with python27Packages; [
      python27Full
      python27Packages.virtualenv
      mysql55
    ];
  };
}
