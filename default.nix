with import <nixpkgs> {}; {
  memsqlPython2Env = stdenv.mkDerivation {
    name = "memsql-python-env";
    buildInputs = with python27Packages; [
      python27Full
      python27Packages.virtualenv
      python27Packages.twine
      mysql55
      zlib
      openssl
    ];
    shellHook = ''
      [ -d venv2 ] || virtualenv venv2
      source venv2/bin/activate
      pip list  --format freeze | grep mysqlclient==1.3.6 >/dev/null || pip install mysqlclient==1.3.6
    '';
  };

  memsqlPython3Env = stdenv.mkDerivation {
    name = "memsql-python-env";
    buildInputs = with python36Packages; [
      python36Full
      python36Packages.virtualenv
      python36Packages.twine
      mysql55
      zlib
      openssl
    ];
    shellHook = ''
      [ -d venv3 ] || virtualenv venv3
      source venv3/bin/activate
    '';
  };
}
