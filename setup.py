from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize
from Cython.Distutils import build_ext
extensions = [
    Extension("consumer", ["consumer.pyx"],
              include_dirs=["/usr/local/include", "/usr/include"],
              libraries=["rdkafka++", "z", "pthread", "rt"],
              library_dirs=["/usr/local/lib", "/usr/lib"],
              language="c++"),
]

setup(
    name="cykafka",
    cmdclass={'build_ext': build_ext},
    ext_modules=cythonize(extensions),
)
