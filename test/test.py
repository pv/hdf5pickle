#!/usr/bin/python
# -*- encoding: utf-8 -*-
# Copyright (c) 2006 Pauli Virtanen <pav@iki.fi>
r"""

Saved structure:

** native types

dict [group]
  .pickletype = DICT
  __/* = keys  [if isinstance(key, str) and suitable(key): can be omitted]
  *    = values

instance [group]
  .pickletype = INST
  __/class = class
  __/args  = initialization arguments
  __/stuff = contents of 'stuff', if it is not a dict
  *        = contents of 'stuff', if it is a dict

global [string] = name of global
  .pickletype = GLOBAL

none [integer array()]
  .pickletype = NONE
bool [integer array(1)]
  .pickletype = BOOL
int [integer array(1)]
  .pickletype = INT
long [long array(1)]
  .pickletype = LONG
float [float array(1)]
  .pickletype = FLOAT
complex [complex array(1)]
  .pickletype = COMPLEX
string [string array(*)]
  .pickletype = STRING
unicode [utf-8 string array(*)]
  .pickletype = UNICODE

** non-native types

X array [X array]


Pickling tests
==============

    >>> import tables, os

    >>> def saveload(obj):
    ...     try:
    ...         try: os.unlink('hdf5test.h5')
    ...         except IOError: pass
    ...         except OSError: pass
    ...         f = tables.openFile('hdf5test.h5', 'w')
    ...         try: p.dump(obj, f, '/obj')
    ...         finally: f.close()
    ...         f = tables.openFile('hdf5test.h5', 'r')
    ...         try: return p.load(f, '/obj')
    ...         finally: f.close()
    ...     except:
    ...         #os.system('h5ls -rvd hdf5test.h5/obj 1>&2')
    ...         raise

    >>> def loadnode(path='/obj'):
    ...     f = tables.openFile('hdf5test.h5', 'r')
    ...     return f.getNode(path)

    >>> def loaditem(path='/obj'):
    ...     f = tables.openFile('hdf5test.h5', 'r')
    ...     try: return f.getNode(path).read()
    ...     finally: f.close()

    >>> def modulelevel(item):
    ...     m = __import__('__main__')
    ...     item.__module__ = '__main__'
    ...     if hasattr(item, '__name__'):
    ...         name = item.__name__
    ...     else:
    ...         name = str(item).split('.')[-1]
    ...     setattr(m, name, item)

Basic types
-----------

    >>> saveload(None)

    >>> saveload(True)
    True
    >>> saveload(False)
    False

    >>> saveload(42)
    42

    >>> saveload(12345678910111213141516178920L)
    12345678910111213141516178920L

    >>> saveload(0.5)
    0.5

    >>> saveload(0.5 + 0.5j)
    (0.5+0.5j)

    >>> saveload('abbacaca')
    'abbacaca'

    >>> saveload(u'Jørgen Bjürström')
    u'J\xc3\xb8rgen Bj\xc3\xbcrstr\xc3\xb6m'

    >>> saveload('a\x00\x00b\x00\x00c\x00\x00d\x00\x00')
    'a\x00\x00b\x00\x00c\x00\x00d\x00\x00'

Homogenous list should be saved as arrays:

    >>> saveload([1, 2, 3, 4, 5, 6, 7])
    [1, 2, 3, 4, 5, 6, 7]
    >>> map(int, loadnode().shape)
    [7]
    >>> type(loadnode()) # doctest: +ELLIPSIS
    <class 'tables...Array'>

    >>> saveload(['a', 'b', 'c', 'd', 'e', 'f'])
    ['a', 'b', 'c', 'd', 'e', 'f']
    >>> type(loadnode()) # doctest: +ELLIPSIS
    <class 'tables...Group'>

Mixed list not so:

    >>> saveload([1, 2, 'c', 'a', 'b'])
    [1, 2, 'c', 'a', 'b']
    >>> type(loadnode()) # doctest: +ELLIPSIS
    <class 'tables...Group'>

Simple tuples, like lists

    >>> saveload((1, 2, 3, 4, 5, 6, 7))
    (1, 2, 3, 4, 5, 6, 7)
    >>> map(int, loadnode().shape)
    [7]

    >>> saveload(('a', 'b', 'c', 'd', 'e', 'f'))
    ('a', 'b', 'c', 'd', 'e', 'f')
    >>> type(loadnode()) # doctest: +ELLIPSIS
    <class 'tables...Group'>

    >>> saveload((1, 2, 'c', 'a', 'b'))
    (1, 2, 'c', 'a', 'b')
    >>> type(loadnode()) # doctest: +ELLIPSIS
    <class 'tables...Group'>

Empty arrays and tuples should work:

    >>> saveload(())
    ()
    >>> saveload([])
    []

As should empty strings and "empty" longs

    >>> saveload(0L)
    0L
    >>> saveload('')
    ''
    >>> saveload(u'')
    u''


Dicts
-----

Simple dicts should also work

    >>> y = saveload({'a': 3, 'b': 2, 'c': 1})
    >>> y = y.items(); y.sort(); y
    [('a', 3), ('b', 2), ('c', 1)]

Dicts with evil keys:

    >>> y = saveload({'..!!': 3})
    >>> y = y.items(); y.sort(); y
    [('..!!', 3)]

    >>> y = saveload({'class': 3, 'type': 3, 'in': 3})
    >>> y = y.items(); y.sort(); y
    [('class', 3), ('in', 3), ('type', 3)]


Classes
-------

Old-style class

    >>> class Cls:
    ...     def __init__(self): self.foo = 'A'
    >>> modulelevel(Cls)
    >>> saveload(Cls)().foo
    'A'

New-style class

    >>> class Cls(object):
    ...     def __init__(self): self.foo = 'B'
    >>> modulelevel(Cls)
    >>> saveload(Cls)().foo
    'B'


Functions
---------

    >>> def func():
    ...     return 'FOO'
    >>> modulelevel(func)
    >>> saveload(func)()
    'FOO'

    >>> import sys
    >>> saveload(sys.exit)
    <built-in function exit>


Modules
-------

These shouldn't work

    >>> import sys
    >>> saveload(sys)
    Traceback (most recent call last):
      ...
    PicklingError: Can't pickle <type 'module'>: it's not found as __builtin__.module
    

Instances
---------

Picklable dict

    >>> class Cls:
    ...     def __init__(self):
    ...         self.foo = 1
    ...         self.bar = [1, 'a', u'c', (42, 43L)]
    ...         self.quux = 2+3j
    >>> modulelevel(Cls)
    >>> y = saveload(Cls())
    >>> y.foo
    1
    >>> y.bar
    [1, 'a', u'c', (42, 43L)]
    >>> y.quux
    (2+3j)

The contents of the class are naturally placed

    >>> type(loadnode('/obj/foo')) # doctest: +ELLIPSIS
    <class 'tables...Array'>
    >>> loaditem('/obj/quux')
    array((2+3j))

    >>> loaditem('/obj/__/cls').tostring()
    '__main__\nCls'

Picklable dict with init arguments

    >>> class Cls:
    ...     def __init__(self, a):
    ...         self.a = a
    >>> modulelevel(Cls)
    >>> y = saveload(Cls(42L))
    >>> y.a
    42L

Picklable dict with __getinitargs__. Note that content of __dict__ is
overridden after __init__ is called.

    >>> class Cls:
    ...     def __init__(self, a, b=None):
    ...         self.a = a
    ...         if b: print b
    ...     def __getinitargs__(self):
    ...         return ('foo', 'bar')
    >>> modulelevel(Cls)
    >>> y = saveload(Cls(42L))
    bar
    >>> y.a
    42L

    >>> type(loadnode('/obj/__/args')) # doctest: +ELLIPSIS
    <class 'tables...Group'>

New-style class using __getnewargs__.

    >>> class Cls(object):
    ...     def __init__(self, a):
    ...         self.a = a
    ...     def __new__(cls, b=None):
    ...         self = object.__new__(cls)
    ...         if b != 'boo': self.b = b
    ...         return self
    ...     def __getnewargs__(cls):
    ...         return ('baz',)
    >>> modulelevel(Cls)
    >>> x = Cls('boo')
    >>> x.a
    'boo'
    >>> y = saveload(x)
    >>> y.b
    'baz'

Old-style class with __set/getstate__

    >>> class Cls:
    ...     def __init__(self, a):
    ...         self.a = a
    ...     def __getstate__(self):
    ...         return {'baize': self.a}
    ...     def __setstate__(self, state):
    ...         self.a = state['baize']
    >>> modulelevel(Cls)
    >>> x = Cls('boo')
    >>> y = saveload(x)
    >>> y.a
    'boo'

    >>> loaditem('/obj/baize').tostring()
    'boo'

New-style class with __set/getstate___

    >>> class Cls(object):
    ...     def __init__(self, a):
    ...         self.a = a
    ...     def __getstate__(self):
    ...         return {'baize': self.a}
    ...     def __setstate__(self, state):
    ...         self.a = state['baize']
    >>> modulelevel(Cls)
    >>> x = Cls('boo')
    >>> y = saveload(x)
    >>> y.a
    'boo'

    >>> loaditem('/obj/baize').tostring()
    'boo'

Saved structure
---------------

The saved data structures should be sensible (some of this was tested above).

First basic container types:

    >>> saveload([1, 2, 3, 4, 5, 6, 7])
    [1, 2, 3, 4, 5, 6, 7]
    >>> loaditem('/obj')
    array([1, 2, 3, 4, 5, 6, 7])

    >>> saveload((1, 2, 3, 4, 5, 6, 7))
    (1, 2, 3, 4, 5, 6, 7)
    >>> loaditem('/obj')
    array([1, 2, 3, 4, 5, 6, 7])

    >>> saveload((0.25, 2., 3., 4., 5., 66., 73.))
    (0.25, 2.0, 3.0, 4.0, 5.0, 66.0, 73.0)
    >>> tuple(loaditem('/obj'))
    (0.25, 2.0, 3.0, 4.0, 5.0, 66.0, 73.0)

    >>> x = {'abba': 1, 'caca': 'zhar'}
    >>> saveload(x) == x
    True
    >>> loaditem('/obj/abba')
    array(1)
    >>> loaditem('/obj/caca').tostring()
    'zhar'

Instances should also be saved sensibly:

    >>> class Cls:
    ...     def __init__(self, abc_132, lll_123):
    ...         self.abc_132 = abc_132
    ...         self.lll_123 = lll_123
    >>> modulelevel(Cls)
    >>> x = saveload(Cls('abb', 2**80))
    >>> loaditem('/obj/abc_132').tostring()
    'abb'
    >>> loaditem('/obj/lll_123').tostring() # saved long bytecode!
    '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01'

Ditto for nested instances

    >>> class Cls2:
    ...     def __init__(self, baz):
    ...         self.baz = baz
    ...         self.asc = Cls(111, 222)
    >>> modulelevel(Cls2)
    >>> x = saveload(Cls2('baz'))
    >>> loaditem('/obj/baz').tostring()
    'baz'
    >>> loaditem('/obj/asc/abc_132')
    array(111)
    >>> loaditem('/obj/asc/lll_123')
    array(222)


Array types
-----------


    >>> for pkg in ['numarray', 'Numeric', 'numpy']:
    ...     for ary in [ [1.,2.,3.],
    ...                  [[1+0j,2+3.2j],[2,4]],
    ...                ]:
    ...         try: m = __import__(pkg)
    ...         except ImportError: continue
    ...         a = m.array(ary)
    ...         a2 = saveload(a)
    ...         assert (m.alltrue(m.ravel(a == a2)) and type(a) == type(a2))
    ...         if pkg == 'numpy':
    ...             assert a.dtype == a2.dtype
    ...         else:
    ...             assert a.typecode() == a2.typecode()


Cleanup
-------
>>> try: os.unlink('hdf5test.h5')
... except IOError: pass
... except OSError: pass
"""

import sys, os
import tables
import doctest, unittest
import copy_reg

import test_support
import pickletester
import hdf5pickle as p

class PickleTests(pickletester.AbstractPickleTests,
                  #test.pickletester.AbstractPersistentPicklerTests
                  ):

    error = IOError

    def dumps(self, arg, proto=0, fast=0):
        try: os.unlink('hdf5test.h5')
        except IOError: pass
        except OSError: pass
        f = tables.openFile('hdf5test.h5', 'w')
        try:
            p.dump(arg, f, '/obj')
        finally:
            f.close()
        f = open('hdf5test.h5', 'r')
        try:
            return f.read()
        finally:
            f.close()
    def loads(self, buf):
        f = open('hdf5test.h5', 'w')
        try:
            f.write(buf)
        finally:
            f.close()
        f = tables.openFile('hdf5test.h5', 'r')
        try:
            return p.load(f, '/obj')
        finally:
            f.close()

    def tearDown(self):
        try: os.unlink('hdf5test.h5')
        except IOError: pass
        except OSError: pass

    # Prune out pickle tests that only mess with the bytestream
    
    def test_dict_chunking(self): pass
    def test_insecure_strings(self): pass
    def test_list_chunking(self): pass
    def test_load_from_canned_string(self): pass
    def test_maxint64(self): pass
    def test_proto(self): pass
    def test_short_tuples(self): pass

    # These needed to be reimplemented: just cut stuff that compares raw output
    
    def produce_global_ext(self, extcode, opcode):
        e = pickletester.ExtensionSaver(extcode)
        try:
            copy_reg.add_extension("pickletester", "MyList", extcode)
            x = pickletester.MyList([1, 2, 3])
            x.foo = 42
            x.bar = "hello"

            # Just test, don't examine output
            s2 = self.dumps(x, 2)
            y = self.loads(s2)
            self.assertEqual(list(x), list(y))
            self.assertEqual(x.__dict__, y.__dict__)
        finally:
            e.restore()

    def test_long1(self):
        x = 12345678910111213141516178920L
        for proto in pickletester.protocols:
            s = self.dumps(x, proto)
            y = self.loads(s)
            self.assertEqual(x, y)

    def test_long4(self):
        x = 12345678910111213141516178920L << (256*8)
        for proto in pickletester.protocols:
            s = self.dumps(x, proto)
            y = self.loads(s)
            self.assertEqual(x, y)

    def test_simple_newobj(self):
        x = object.__new__(pickletester.SimpleNewObj)  # avoid __init__
        x.abc = 666
        for proto in pickletester.protocols:
            s = self.dumps(x, proto)
            #self.assertEqual(opcode_in_pickle(pickle.NEWOBJ, s), proto >= 2)
            y = self.loads(s)   # will raise TypeError if __init__ called
            self.assertEqual(y.abc, 666)
            self.assertEqual(x.__dict__, y.__dict__)

    def test_singletons(self):
        for proto in pickletester.protocols:
            for x in None, False, True:
                s = self.dumps(x, proto)
                y = self.loads(s)
                self.assert_(x is y, (proto, x, s, y))

def additional_tests():
    return doctest.DocTestSuite()

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(PickleTests))
    suite.addTest(doctest.DocTestSuite())
    try: suite.addTest(doctest.DocTestSuite(p))
    except ValueError: pass
    return suite
