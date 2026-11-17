{ pkgs, ... }:

{
  packages = with pkgs; [
    bashInteractive
    coreutils
    devspace
    dotnetCorePackages.sdk_10_0-bin
    gnugrep
    just
    kubectl
    openssl
  ];
}
