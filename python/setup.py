from __future__ import with_statement
from setuptools import setup
from os.path import exists, join, dirname, realpath

CURR_DIR = dirname(realpath(__file__))
VERSION_FILE = join(CURR_DIR, "version.txt")
README_FILE = join(CURR_DIR, "README.rst")

if exists(VERSION_FILE):
    with open(VERSION_FILE, 'r') as f:
        __version__ = f.read().strip()
else:
    __version__ = 'local'

with open(README_FILE, 'r') as f:
    README = f.read()

setup(
    name="sourced-engine",
    description="Engine to use Spark on top of source code repositories.",
    long_description=README,
    version=__version__,
    license="Apache-2.0",
    author="source{d}",
    author_email="hello@sourced.tech",
    url="https://github.com/src-d/engine/tree/master/python",
    packages=['sourced',
              'sourced.engine'],
    install_requires=["pyspark>=2.0.0"],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3"
    ]
)
