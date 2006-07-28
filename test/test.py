#!/usr/bin/python
# -*- encoding: utf-8 -*-
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
    ...     m = __import__(item.__module__)
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

Homogenous list should be saved as arrays:

    >>> saveload([1, 2, 3, 4, 5, 6, 7])
    [1, 2, 3, 4, 5, 6, 7]
    >>> map(int, loadnode().shape)
    [7]
    >>> type(loadnode())
    <class 'tables.Array.Array'>

    >>> saveload(['a', 'b', 'c', 'd', 'e', 'f'])
    ['a', 'b', 'c', 'd', 'e', 'f']
    >>> map(int, loadnode().shape)
    [6]

Mixed list not so:

    >>> saveload([1, 2, 'c', 'a', 'b'])
    [1, 2, 'c', 'a', 'b']
    >>> type(loadnode())
    <class 'tables.Group.Group'>

Simple tuples, like lists

    >>> saveload((1, 2, 3, 4, 5, 6, 7))
    (1, 2, 3, 4, 5, 6, 7)
    >>> map(int, loadnode().shape)
    [7]

    >>> saveload(('a', 'b', 'c', 'd', 'e', 'f'))
    ('a', 'b', 'c', 'd', 'e', 'f')
    >>> map(int, loadnode().shape)
    [6]

    >>> saveload((1, 2, 'c', 'a', 'b'))
    (1, 2, 'c', 'a', 'b')
    >>> type(loadnode())
    <class 'tables.Group.Group'>

Empty arrays and tuples should work:

    >>> saveload(())
    ()
    >>> saveload([])
    []


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

    >>> type(loadnode('/obj/foo'))
    <class 'tables.Array.Array'>
    >>> loaditem('/obj/quux')
    array((2+3j))

    >>> loaditem('/obj/__/cls')
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

    >>> loaditem('/obj/__/args')
    ('foo', 'bar')

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

    >>> loaditem('/obj/baize')
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

    >>> loaditem('/obj/baize')
    'boo'



"""
import sys, os
import tables
import doctest, unittest

import test_support, pickletester

ppath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(ppath)
import hdf5pickle as p
sys.path.pop()

class PickleTests(pickletester.AbstractPickleTests,
                  #test.pickletester.AbstractPersistentPicklerTests
                  ):

    error = IOError

    def dumps(self, arg, proto=0, fast=0):
        try: os.unlink('hdf5test.h5')
        except IOError: pass
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

    # Prune out pickle tests that mess with the bytestream
    def test_dict_chunking(self): pass
    def test_global_ext1(self): pass
    def test_global_ext2(self): pass
    def test_global_ext4(self): pass
    def test_insecure_strings(self): pass
    def test_list_chunking(self): pass
    def test_load_from_canned_string(self): pass
    def test_long1(self): pass
    def test_long4(self): pass
    def test_maxint64(self): pass
    def test_proto(self): pass
    def test_short_tuples(self): pass
    def test_simple_newobj(self): pass
    def test_singletons(self): pass


def _test():
    try:
        test_support.run_unittest(
            PickleTests
            )
    except:
        pass
    test_support.run_doctest(__import__('__main__'), verbosity=0)

if __name__ == "__main__": _test()
