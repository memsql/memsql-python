with import <nixpkgs> {}; {
  memsqlPython = stdenv.mkDerivation {
    name = "memsql-python-env";
    buildInputs = with python36Packages; [
      python36Full
      python36Packages.virtualenv
      python36Packages.twine
      mysql57
      zlib
      openssl
    ];
    shellHook = ''
      [ -d venv3 ] || virtualenv venv3
      source venv3/bin/activate
    '';
  };
}
