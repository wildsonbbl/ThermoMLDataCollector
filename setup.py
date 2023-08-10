from setuptools import setup
from Cython.Build import cythonize

setup(
    name='thermoml builder ',
    ext_modules=cythonize("ThermoMLBuilder.pyx"),
)