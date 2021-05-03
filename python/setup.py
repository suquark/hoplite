import pathlib

from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

project_dir = pathlib.Path(__file__).parent.absolute().parent
src_dir = project_dir / 'src'
lib_dir = project_dir / 'build'

import shutil
shutil.copy(lib_dir / 'notification', '.')

ext_modules = [
    Extension(
        "_hoplite_client",
        sources=["_hoplite_client.pyx"],
        include_dirs=[str(src_dir), str(lib_dir)],  # lib_dir contains "object_store.grpc.pb.h"
        library_dirs=[str(lib_dir)],
        libraries=["hoplite_client_lib"],
        # this is necessary for the dynamic linking of Linux to
        # be working in a distributed environment
        extra_link_args=['-Wl,-rpath=' + str(lib_dir)],
    )
]

setup(name='hoplite',
      zip_safe=False,
      packages=['hoplite'],
      package_dir={'hoplite': 'hoplite'},
      package_data={'hoplite': ['_hoplite_lib*.so']},
      include_package_data=True,
      ext_modules=cythonize(ext_modules))
