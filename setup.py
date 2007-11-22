#!/usr/bin/env python

# use setuptools or distutils
# ---------------------------

import os
extra_args={'cmdclass': {}}

try:
    from setuptools import setup, find_packages, Command
except ImportError:
    print "WARNING: setuptools not available, falling back to distutils!"
    from distutils.core import setup, Command
    class xtest(Command):
        description = "run tests"
        user_options = []
        def initialize_options(self): pass
        def finalize_options(self): pass
        def run(self):
            import test.test, unittest, sys
            sys.argv = [sys.argv[0]]
            unittest.TextTestRunner(verbosity=2).run(test.test.suite())
            raise SystemExit(0)
    extra_args['cmdclass']['test'] = xtest


# docs
# ----

class doc(Command):
    description = "generate documentation"
    user_options = []

    def initialize_options(self): pass
    def finalize_options(self): pass
    def run(self):
        try: os.makedirs('doc')
        except OSError: pass
        modname = 'hdf5pickle'
        os.system("epydoc -v --no-frames -o doc %s" % modname)

extra_args['cmdclass']['doc'] = doc


# setup
# -----

setup(
    name = "hdf5pickle",
    version = "0.2.1",
    packages = ["hdf5pickle"],

    author = "Pauli Virtanen",
    author_email = "pav@iki.fi",
    description = "Pickle Python objects to HDF5 files",
    license = "BSD & Python Software Foundation License",
    keywords = "hdf5 pickle pytables",
    url = "",

    install_requires = ['tables >= 2.0b1'],

    long_description = """
Create easily interfaceable representations of Python objects in HDF5
files. The aim of this module is to provide both

    (1) convenient Python object persistence
    (2) compatibility with non-Python applications

Point 2 is useful, for example, if results from numerical
calculations should be easily transferable for example to a non-Python
visualization program. For example, if program state is serialized to
a HDF5 file, it can easily be examined with for example Octave_.

.. _Octave: http://www.octave.org/
""",

    test_suite = "test.test",

    **extra_args
)
