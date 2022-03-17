#!/usr/bin/env python

from setuptools import setup
from setuptools.command.test import test as TestCommand

# get version
from memsql import __version__

from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


REQUIREMENTS = [
    'wraptor',
    'simplejson',
    'python-dateutil<3.0',
    'mysqlclient>=1.4,<3.0',
]

class PyTest(TestCommand):
    user_options = [
        ('watch', 'w',
         "watch tests for changes"),
        ('pdb', 'i',
         "start pdb on failures"),
        ('scan=', 's',
         "only search for tests in the specified directory or file"),
        ('exitfirst', 'x',
         "Stop tests on first failure"),
        ('expression=', 'k',
         "Only run tests matching given expression"),
        ('verbose', 'v',
         "Print out all output as it happens"),
    ]
    boolean_options = ['watch']

    def initialize_options(self):
        self.watch = False
        self.verbose = False
        self.pdb = False
        self.scan = None
        self.exitfirst = False
        self.expression = None
        self.test_suite = None
        self.test_module = None
        self.test_loader = None
        self.test_runner = None

    def finalize_options(self):
        TestCommand.finalize_options(self)

        self.test_suite = True
        self.test_args = ['-v']
        if self.watch:
            self.test_args.append('-f')
        if self.verbose:
            self.test_args.append('-s')
        if self.pdb:
            self.test_args.append('--pdb')
        if self.exitfirst:
            self.test_args.append('-x')
        if self.expression:
            self.test_args.extend(['-k', self.expression])
        if self.scan is not None:
            self.test_args.append(self.scan)
        else:
            self.test_args.append('memsql')

    def run_tests(self):
        import os, sys, glob

        MY_PATH = os.path.dirname(__file__)
        sys.path.append(MY_PATH)
        os.environ['PYTHONPATH'] = os.environ.get('PYTHONPATH', '') + ':' + MY_PATH

        egg_dirs = glob.glob('*.egg*')
        ignore_args = ['--ignore=%s' % d for d in egg_dirs]

        import pytest
        errno = pytest.main(ignore_args + self.test_args)
        raise sys.exit(errno)

setup(
    name='memsql',
    version=__version__,
    author='MemSQL',
    author_email='support@memsql.com',
    url='http://github.com/memsql/memsql-python',
    license='LICENSE.txt',
    description='Useful utilities and plugins for MemSQL integration.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    packages=[
        'memsql',
        'memsql.common',
    ],
    zip_safe=False,
    install_requires=REQUIREMENTS,
    tests_require=['pytest', 'mock', 'ordereddict==1.1'],
    cmdclass={ 'test': PyTest },
)
