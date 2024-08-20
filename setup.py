#!/usr/bin/env python3
import os
from distutils.core import setup

from setuptools import find_packages

from inext_cli.version import __version__

author = 'James Robertson'

classifiers = """
Development Status :: 4 - Beta
Environment :: Console
License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)
Intended Audience :: Science/Research
Topic :: Scientific/Engineering
Topic :: Scientific/Engineering :: Bio-Informatics
Programming Language :: Python
Programming Language :: Python :: 3.8
Programming Language :: Python :: 3.9
Programming Language :: Python :: Implementation :: CPython
Operating System :: POSIX :: Linux
""".strip().split('\n')


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


exec(open('inext_cli/version.py').read())

setup(
    name='inext_cli',
    include_package_data=True,
    version=__version__,
    python_requires='>=3.8.2,<4',
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    packages=find_packages(exclude=['tests']),
    url='https://github.com/phac-nml/inext_cli',
    license='GPLv3',
    author='James Robertson',
    author_email='james.robertson@phac-aspc.gc.ca',
    description=(
        'IRIDA Next GraphQL API command line tool kit'),
    keywords='IRIDA Next',
    classifiers=classifiers,
    package_dir={'inext_cli': 'inext_cli'},
    package_data={
        "": ["*.txt","*.csv","*.xlsx","*.fasta","*.fastq"],
    },

    install_requires=[
        'numpy>=1.24.4',
        'tables>=3.8.0',
        'six>=1.16.0',
        'pandas>=2.0.2',
        'openpyxl>=3.1.4',
        'pycurl>=7.45.3',
        'gql>=3.5.0',
        'aiohttp>=3.9.5',
        'requests',
        'requests_toolbelt'


    ],

    entry_points={
        'console_scripts': [
            'inext_cli=inext_cli.main:main',
        ],
    },
)
