from setuptools import setup

setup(
    name="sourced-spark-api",
    description="API to use Spark on top of source code repositories.",
    version="0.0.1",
    license="Apache-2.0",
    author="Miguel Molina",
    author_email="miguel.a@sourced.tech",
    url="https://github.com/src-d/spark-api/tree/master/python",
    packages=['sourced',
              'sourced.spark'],
    install_requires=["pyspark>=2.0.0"],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Programming Language :: Python :: 2.7"
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6"
    ]
)
