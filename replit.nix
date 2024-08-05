{pkgs}: {
  deps = [
    pkgs.libgccjit
    pkgs.libGL
    pkgs.opencl-headers
    pkgs.ocl-icd
    pkgs.tk
    pkgs.tcl
    pkgs.qhull
    pkgs.pkg-config
    pkgs.gtk3
    pkgs.gobject-introspection
    pkgs.ghostscript
    pkgs.freetype
    pkgs.ffmpeg-full
    pkgs.cairo
    pkgs.glibcLocales
  ];
  env = {
    PYTHON_LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
      pkgs.gcc.cc.libgcc
    ];
  };
}
