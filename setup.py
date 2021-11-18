#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-mssql",
    version="1.0.3",
    description="Singer.io tap for extracting data from SQL Server - PipelineWise compatible",
    author="Stitch",
    url="https://github.com/wintersrd/pipelinewise-tap-mssql",
    classifiers=[
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Programming Language :: Python :: 3 :: Only",
    ],
    py_modules=["tap_mssql"],
    install_requires=[
        "attrs==16.3.0",
        "pendulum==1.2.0",
        "singer-python~=5.3.1",
        "pymssql==2.2.1",
        "backoff==1.3.2",
    ],
    entry_points="""
          [console_scripts]
          tap-mssql=tap_mssql:main
      """,
    packages=["tap_mssql", "tap_mssql.sync_strategies"],
)
