#!env python

from distutils.core import setup
from setuptools.command.test import test as TestCommand

# get version
with open('memsql/__init__.py') as f:
    exec(f.read())

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest, sys
        errno = pytest.main(self.test_args)
        raise sys.exit(errno)

setup(
    name='MemSQL',
    version=__version__,
    author='MemSQL',
    author_email='support@memsql.com',
    packages=[
        'memsql',
    ],
    url='http://github.com/memsql/memsql-python',
    license='LICENSE.txt',
    description='Useful utilities and plugins for MemSQL integration.',
    long_description=open('README.rst').read(),
    tests_require=['pytest'],
    cmdclass={ 'test': PyTest },
)
