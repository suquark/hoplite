import pathlib

from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

project_dir = pathlib.Path(__file__).parent.absolute().parent
src_dir = project_dir / 'src'
lib_dir = project_dir / 'build'

ext_modules = [
    Extension(
        "hoplite",
        sources=["_client.pyx"],
        include_dirs=[str(src_dir), str(lib_dir)],  # lib_dir contains "object_store.grpc.pb.h"
        library_dirs=[str(project_dir)],
        libraries=["hoplite_client_lib"],
        # this is necessary for the dynamic linking of Linux to
        # be working in a distributed environment
        extra_link_args=['-Wl,-rpath=' + str(lib_dir)],
    )
]

setup(name="hoplite",
      ext_modules=cythonize(ext_modules))
