# Copyright (c) 2006 Pauli Virtanen <pav@iki.fi>
r"""

==========
hdf5pickle
==========

:author: Pauli Virtanen <pav@iki.fi>


Create easily interoperable representations of Python objects in HDF5_
files. The aim of this module is to provide both

(1) convenient Python object persistence
(2) compatibility with non-Python applications

Point 2 is useful, for example, if results from numerical
calculations should be easily transferable for example to a non-Python
visualization program, such as Octave_. Having a serialized object
format that is directly readable saves some hassle in writing custom
data dumping routines for each object.

Of course, if your data does not fit into memory, you still need to
use full features of PyTables_. But, you can still use hdf5pickle for
other parts of the data.

This module implements `dump` and `load` methods analogous to those in
Python's pickle module. The programming interface corresponds to
pickle protocol 2, although the data is not serialized but saved in
HDF5 files. Additional methods, `dump_many` and `load_many`, are
provided for loading multiple objects at once, to preserve references.


:warning:
    Although this module passes all relevant pickle unit tests from
    Python2.4 plus additional tests, it is still in early stages of
    development.


.. _HDF5: http://hdf.ncsa.uiuc.edu/HDF5/
.. _Octave: http://www.octave.org/
.. _PyTables: http://www.pytables.org/


Data format
===========

The layout of a python object saved to a HDF5 node is described below.
The notation is roughly::

    type-of-hdf5-node [(array shape), array type)] = what's in it
       .attribute-of-node = what's in it
       child-node

The structure of a node corresponding to a Python object varies,
depending on the type of the Python object.

* Basic types (``None``, ``bool``, ``int``, ``float``, ``complex``)::

    array [(1,), int/float] = NONE/BOOL/INT/FLOAT/COMPLEX
        .pickletype         = PICKLE_TYPE

* Basic stream types (``long``, ``str``, ``unicode``).
  Longs and unicodes are converted to strings (`pickle.encode_long`, utf-8),
  and strings are converted to arrays of unsigned 8-bit integers:: 

    array [(n,), uint8] = DATA
        .pickletype    = LONG/STR/UNICODE
        .empty     = 1 #if len(DATA) == 0
    
  :bug:
     At present strings are not stored as HDF5 strings,
     as PyTables appears to chop them off at '\\x00' characters.

* dicts::

    group
        .pickletype  = DICT

        #for KEY, VALUE in DICT:
         #if KEY is a string and a valid python variable name
        KEY          = node for VALUE
         #else
        SURROGATE    = node for VALUE
        __/SURROGATE = node for KEY
         #end if
        #end for
    
* instances::

    group
        .pickletype         = INST/REDUCE
    
        #if through __reduce__ / new-style class
        .has_reduce_content = 1 if state present
        __/args             = arguments for class.__new__ or func
        __/func             = creation func
        __/cls              = creation class
        #else
        __/args             = arguments for class.__init__
        __/cls              = creation class
        #endif
    
        #if state is dict
        insert entries of dict here as in dict
        #else
        __/content          = node for content
        #endif

* globals (classes, etc)::

    array [as for strings] = GLOBAL/EXT4 data locator, as in pickle
        .pickletype        = GLOBAL/EXT4

* reference to an object elsewhere::

    group
        .pickletype        = REF
        .target            = abs. path to the referred object in this file

"""

from base import *

__docformat__ = "restructuredtext en"
__version__ = "0.2.1"
