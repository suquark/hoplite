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
    )
]

setup(name="py_distributed_object_store",
      ext_modules=cythonize(ext_modules))
