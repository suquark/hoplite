import os
from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

ext_modules = [
    Extension(
        "py_distributed_object_store",
        sources=["python/_client.pyx"],
        include_dirs=[os.path.abspath("src/")],
        library_dirs=[os.path.abspath(".")],
        libraries=["distributed_object_store"],
        # this is necessary for the dynamic linking of Linux to
        # be working in a distributed environment
        extra_link_args=['-Wl,-rpath='+os.path.abspath("python/hoplite")],
    )
]

setup(name="py_distributed_object_store",
      ext_modules=cythonize(ext_modules))
