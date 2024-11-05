{pkgs}: {
  default = pkgs.mkShell {
    nativeBuildInputs = with pkgs; [
      grpc
      glog
      protobuf
      openssl
      pkg-config
    ];
    env.GLOG_logtostderr = "1";
  };
}
