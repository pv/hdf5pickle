from setuptools import setup, find_packages
setup(
    name = "hdf5pickle",
    version = "0.1",
    packages = ["hdf5pickle"],

    author = "Pauli Virtanen",
    author_email = "pav@iki.fi",
    description = "Pickle Python objects to HDF5 files",
    license = "PSF",
    keywords = "hdf5 pickle pytables",
    url = "",

    #install_requires = ['PyTables', 'NumArray'],

    long_description = """
Create easily interfaceable representations of Python objects in HDF5
files. The aim of this module is to provide both

    (1) convenient Python object persistence
    (2) compatibility with non-Python applications

Point 2 is important, for example, if results from numerical
calculations should be easily transferable for example to a non-Python
visualization program. For example, if program state is serialized to
a HDF5 file, it can easily be examined with for example Octave_.

.. _Octave: http://www.octave.org/
""",

    test_suite = "test.test",
    
)