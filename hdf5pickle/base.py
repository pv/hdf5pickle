# This file is heavily adapted from 'pickle.py' in Python 2.4:
# Copyright (c) 2001, 2002, 2003, 2004 Python Software Foundation
# All Rights Reserved.
#
# Modifications to use Pytables:
# Copyright (c) 2006 Pauli Virtanen <pav@iki.fi>
#
# See LICENSE.txt for some legalese.

__all__ = ['dump', 'load', 'Pickler', 'Unpickler',
           'dump_many', 'load_many']

__docformat__ = "restructuredtext en"

from copy_reg import dispatch_table
from copy_reg import _extension_registry, _inverted_registry, _extension_cache
from types import *
import keyword, marshal
import tables, numpy, cPickle as pickle, re, struct, sys

from pickle import whichmodule, PicklingError, FLOAT, INT, LONG, NONE, \
     REDUCE, STRING, UNICODE, GLOBAL, DICT, INST, LIST, TUPLE, EXT4, \
     encode_long, decode_long

BOOL    = 'BB'
REF     = 'RR'
COMPLEX = 'CC'

NUMARRAY = 'NA'
NUMPY    = 'NP'
NUMERIC  = 'NU'

HIGHEST_PROTOCOL = 2
"""The pickling (programming) protocol supported by this module"""

def _DEBUG(*args):
    sys.stderr.write(' '.join(map(str, args)) + '\n')

try:
    from org.python.core import PyStringMap
except ImportError:
    PyStringMap = None

try:
    UnicodeType
except NameError:
    UnicodeType = None

try:
    from tables import NoSuchNodeError
except ImportError:
    NoSuchNodeError = LookupError

try:
    from tables import checkflavor
except ImportError:
    import tables.flavor
    def checkflavor(flavor, x=None, y=None):
        return flavor.lower() in tables.flavor.all_flavors

### Check what PyTables supports on this system

NumericArrayType = None
NumericArrayType_native = False
try:
    try:
        try: checkflavor('Numeric', 'f')
        except TypeError: checkflavor('Numeric', 'f', '')
        NumpyArrayType_native = True
    except ValueError:
        pass
    import Numeric
    from Numeric import ArrayType as NumericArrayType
except ImportError:
    pass

NumarrayArrayType = None
NumarrayArrayType_native = False
try:
    try:
        try: checkflavor('NumArray', 'f')
        except TypeError: checkflavor('NumArray', 'f', '')
        NumarrayArrayType_native = True
    except ValueError:
        pass
    import numarray
    from numarray import ArrayType as NumarrayArrayType
except ImportError:
    pass

NumpyArrayType = None
NumpyArrayType_native = False
try:
    try:
        try: checkflavor('numpy', 'f')
        except TypeError: checkflavor('numpy', 'f', '')
        NumpyArrayType_native = True
    except ValueError:
        pass
    import numpy
    from numpy.oldnumeric import ArrayType as NumpyArrayType
except ImportError:
    pass


HDF5PICKLE_PROTOCOL = 1
"""Identifier for the current HDF5 pickling protocol"""

#############################################################################


class _FileInterface(object):
    """
    Internal interface to a `tables.File` object.

    Includes convenience functions, including type conversion.
    """
    def __init__(self, file, type_map=None):
        self.file = file
        if type_map == None:
            self.type_map = {}
        else:
            self.type_map = type_map
    
    def  _splitpath(s):
        i = s.rindex('/')
        where, name = s[:i], s[(i+1):]
        if where == '': where = '/'
        return where, name
    _splitpath = staticmethod(_splitpath)

    def set_attr(self, obj, attr, value):
        if isinstance(obj, tables.Group):
            obj._f_setAttr(attr, value)
        else:
            setattr(obj.attrs, attr, value)

    def has_attr(self, obj, attr):
        try:
            self.get_attr(obj, attr)
            return True
        except AttributeError:
            return False

    def get_attr(self, obj, attr):
        if isinstance(obj, tables.Group):
            return obj._f_getAttr(attr)
        else:
            return getattr(obj.attrs, attr)

    def get_path(self, path):
        return self.file.getNode(path)

    def has_path(self, path):
        try:
            self.file.getNode(path)
            return True
        except NoSuchNodeError:
            return False

    def save_array(self, path, data):
        where, name = self._splitpath(path)
        type_ = type(data)

        if type_ in (tuple, list, str):
            if len(data) == 0:
                array = self.file.createArray(
                    where, name, numpy.array([0], dtype=numpy.int8))
                self.set_attr(array, 'empty', 1)
                return array
            elif type_ in (tuple, list):
                btype = type(data[0])
                if not btype in (int, float, complex):
                    raise TypeError
                for item in data:
                    if type(item) != btype:
                        raise TypeError
            if type_ is str:
                # FIXME: pytables chops off NULs from strings!
                #        protect via encoding in 8-bytes
                return self.file.createArray(where, name, numpy.fromstring(
                    data, dtype=self.type_map.get(str, numpy.uint8)))
            return self.file.createArray(where, name, numpy.array(
                data, dtype=self.type_map.get(btype)))
        elif type_ in (int, float, complex):
            return self.file.createArray(where, name, numpy.array(
                data, dtype=self.type_map.get(type_)))
        elif type_ in (long,):
            return self.file.createArray(where, name, numpy.array(
                data, dtype=self.type_map.get(type_, numpy.object_)))
        else:
            raise TypeError

    def save_numeric_array(self, path, data):
        where, name = self._splitpath(path)
        return self.file.createArray(where, name, data)

    def load_array(self, node, type_):
        if type_ in (tuple, list, str):
            if self.has_attr(node, 'empty'):
                return type_()
            else:
                if type_ is str:
                    # FIXME: pytables chops off NULs from strings!
                    #        protect via encoding in 8-bytes
                    return numpy.asarray(node.read()).tostring()
                return type_(node.read())
        elif type_ in (int, float):
            return type_(node.read())
        elif type_ is bool:
            return type_(numpy.alltrue(node.read()))
        elif type_ is complex:
            data = node.read()
            return complex(data[()])
        else:
            raise TypeError()

    def new_group(self, path):
        where, name = self._splitpath(path)
        return self.file.createGroup(where, name)


#############################################################################


class Pickler(object):
    """
    Pickles Python objects to a HDF5 file.

    Usage:
      1. Instantaniate
      2. Call `dump` or `clear_memo` as necessary

    You may wish to use a single instance of this class for multiple
    objects to preserve references. It should be safe to call the `dump`
    method multiple times, for different paths.
    """
    def __init__(self, file, type_map=None):
        self.file = _FileInterface(file, type_map)
        
        self.paths = {}
        self.memo = {}

        self.proto = HDF5PICKLE_PROTOCOL # hard-coded

        self.file.set_attr(self.file.get_path('/'),
                           'hdf5pickle_protocol',
                           HDF5PICKLE_PROTOCOL)

    def _keep_alive(self, obj):
        self.memo[id(obj)] = obj

    def clear_memo(self):
        self.paths = {}
        self.memo = {}

    def dump(self, path, obj):
        self._save(path, obj)

    def _save(self, path, obj):
        x = self.paths.get(id(obj))
        if x:
            self._save_ref(path, x)
            return
        else:
            self.paths[id(obj)] = path

        self._keep_alive(obj)

        # Check if we have a dispatch for it
        t = type(obj)
        f = self._dispatch.get(t)
        if f:
            x = f(self, path, obj)
            return

        # Check for a class with a custom metaclass; treat as regular class
        try:
            issc = issubclass(t, TypeType)
        except TypeError: # t is not a class (old Boost; see SF #502085)
            issc = 0
        if issc:
            self._save_global(path, obj)
            return

        # Check copy_reg.dispatch_table
        reduce = dispatch_table.get(t)
        if reduce:
            rv = reduce(obj)
        else:
            # Check for a __reduce_ex__ method, fall back to __reduce__
            reduce = getattr(obj, "__reduce_ex__", None)
            if reduce:
                rv = reduce(2) # "protocol 2"
            else:
                reduce = getattr(obj, "__reduce__", None)
                if reduce:
                    rv = reduce()
                else:
                    raise PicklingError("Can't pickle %r object: %r" %
                                        (t.__name__, obj))

        # Check for string returned by reduce(), meaning "save as global"
        if type(rv) is StringType:
            self._save_global(path, obj, rv)
            return

        # Assert that reduce() returned a tuple
        if type(rv) is not TupleType:
            raise PicklingError("%s must return string or tuple" % reduce)

        # Assert that it returned an appropriately sized tuple
        l = len(rv)
        if not (2 <= l <= 5):
            raise PicklingError("Tuple returned by %s must have "
                                "two to five elements" % reduce)

        # Save the reduce() output and finally memoize the object
        self._save_reduce(path, obj=obj, *rv)

    _dispatch = {}

    def _save_ref(self, path, objpath):
        group = self.file.new_group(path)
        self.file.set_attr(group, 'target', objpath)
        self.file.set_attr(group, 'pickletype', REF)

    def _save_reduce(self, path, func, args, state=None,
                    listitems=None, dictitems=None, obj=None):
        # This API is called by some subclasses

        # Assert that args is a tuple or None
        if not isinstance(args, TupleType):
            if args is None:
                # A hack for Jim Fulton's ExtensionClass, now deprecated.
                # See load_reduce()
                warnings.warn("__basicnew__ special case is deprecated",
                              DeprecationWarning)
            else:
                raise PicklingError(
                    "args from reduce() should be a tuple")

        # Assert that func is callable
        if not callable(func):
            raise PicklingError("func from reduce should be callable")

        group = self.file.new_group(path)
        self.file.new_group(path + '/__')

        self.file.set_attr(group, 'pickletype', REDUCE)

        # Protocol 2 special case: if func's name is __newobj__, use NEWOBJ
        if getattr(func, "__name__", "") == "__newobj__":
            # A __reduce__ implementation can direct protocol 2 to
            # use the more efficient NEWOBJ opcode, while still
            # allowing protocol 0 and 1 to work normally.  For this to
            # work, the function returned by __reduce__ should be
            # called __newobj__, and its first argument should be a
            # new-style class.  The implementation for __newobj__
            # should be as follows, although pickle has no way to
            # verify this:
            #
            # def __newobj__(cls, *args):
            #     return cls.__new__(cls, *args)
            #
            # Protocols 0 and 1 will pickle a reference to __newobj__,
            # while protocol 2 (and above) will pickle a reference to
            # cls, the remaining args tuple, and the NEWOBJ code,
            # which calls cls.__new__(cls, *args) at unpickling time
            # (see load_newobj below).  If __reduce__ returns a
            # three-tuple, the state from the third tuple item will be
            # pickled regardless of the protocol, calling __setstate__
            # at unpickling time (see load_build below).
            #
            # Note that no standard __newobj__ implementation exists;
            # you have to provide your own.  This is to enforce
            # compatibility with Python 2.2 (pickles written using
            # protocol 0 or 1 in Python 2.3 should be unpicklable by
            # Python 2.2).
            cls = args[0]
            if not hasattr(cls, "__new__"):
                raise PicklingError(
                    "args[0] from __newobj__ args has no __new__")
            if obj is not None and cls is not obj.__class__:
                raise PicklingError(
                    "args[0] from __newobj__ args has the wrong class")
            args = args[1:]

            self._save('%s/__/cls' % path, cls)
            self._save('%s/__/args' % path, args)
        else:
            self._save('%s/__/func' % path, func)
            self._save('%s/__/args' % path, args)

        if obj is not None:
            self._keep_alive(obj)

        if listitems is not None:
            self._save('%s/__/listitems' % path, list(listitems))

        if dictitems is not None:
            self._save('%s/__/dictitems' % path, dict(dictitems))

        if state is not None:
            self.file.set_attr(group, 'has_reduce_content', 1)
            if isinstance(state, dict):
                self._save_dict_content(path, state)
                self._keep_alive(state)
            else:
                self._save('%s/__/content' % path, state)

    def _save_none(self, path, obj):
        array = self.file.save_array(path, 0)
        self.file.set_attr(array, 'pickletype', NONE)
    _dispatch[NoneType] = _save_none

    def _save_bool(self, path, obj):
        array = self.file.save_array(path, int(obj))
        self.file.set_attr(array, 'pickletype', BOOL)
    _dispatch[bool] = _save_bool

    def _save_int(self, path, obj):
        array = self.file.save_array(path, obj)
        self.file.set_attr(array, 'pickletype', INT)
    _dispatch[IntType] = _save_int

    def _save_long(self, path, obj):
        array = self.file.save_array(path, str(encode_long(obj)))
        self.file.set_attr(array, 'pickletype', LONG)
    _dispatch[LongType] = _save_long

    def _save_float(self, path, obj):
        array = self.file.save_array(path, obj)
        self.file.set_attr(array, 'pickletype', FLOAT)
    _dispatch[FloatType] = _save_float

    def _save_complex(self, path, obj):
        array = self.file.save_array(path, obj)
        self.file.set_attr(array, 'pickletype', COMPLEX)
    _dispatch[ComplexType] = _save_complex

    def _save_string(self, path, obj):
        node = self.file.save_array(path, obj)
        self.file.set_attr(node, 'pickletype', STRING)
    _dispatch[StringType] = _save_string

    def _save_unicode(self, path, obj):
        node = self.file.save_array(path, obj.encode('utf-8'))
        self.file.set_attr(node, 'pickletype', UNICODE)
    _dispatch[UnicodeType] = _save_unicode

    def _save_tuple(self, path, obj):
        try:
            array = self.file.save_array(path, obj)
            self.file.set_attr(array, 'pickletype', TUPLE)
            return array
        except TypeError:
            pass

        group = self.file.new_group(path)
        self.file.set_attr(group, 'pickletype', TUPLE)
        for i, item in enumerate(obj):
            self._save('%s/_%d' % (path, i), item)
        return group
    _dispatch[TupleType] = _save_tuple

    def _save_list(self, path, obj):
        item = self._save_tuple(path, obj)
        self.file.set_attr(item, 'pickletype', LIST)
    _dispatch[ListType] = _save_list

    def _save_dict(self, path, obj):
        group = self.file.new_group(path)
        self.file.set_attr(group, 'pickletype', DICT)
        self._save_dict_content(path, obj)

    def _save_dict_content(self, path, obj):
        strkeys = {}
        seen = {}
        keyi = 0
        for key in obj.iterkeys():
            if (isinstance(key, str) and _check_pytables_name(key)
                    and key != "__"):
                strkeys[key] = key
                seen[key] = True
        for key in obj.iterkeys():
            if not key in strkeys:
                while ("_%d" % keyi) in seen: keyi += 1
                strkeys[key] = "_%d" % keyi
                seen[strkeys[key]] = True

        hassub = self.file.has_path('%s/__' % path)

        for key, value in obj.iteritems():
            self._save('/'.join([path, strkeys[key]]), value)
            if not strkeys[key] is key:
                if not hassub:
                    self.file.new_group('%s/__' % path)
                    hassub = True
                self._save('%s/__/%s' % (path, strkeys[key]), key)

    _dispatch[DictionaryType] = _save_dict
    if not PyStringMap is None:
        _dispatch[PyStringMap] = _save_dict
    
    def _save_inst(self, path, obj):
        cls = obj.__class__

        if hasattr(obj, '__getinitargs__'):
            args = obj.__getinitargs__()
            len(args) # XXX Assert it's a sequence
        else:
            args = ()

        try:
            getstate = obj.__getstate__
        except AttributeError:
            stuff = obj.__dict__
        else:
            stuff = getstate()

        group = self.file.new_group(path)
        self.file.set_attr(group, 'pickletype', INST)

        self.file.new_group("%s/__" % path)
        self._save('%s/__/cls' % path, cls)
        self._save('%s/__/args' % path, args)

        if isinstance(stuff, dict):
            self._save_dict_content(path, stuff)
            self._keep_alive(stuff)
        else:
            self._save('%s/__/content' % path, stuff)
    _dispatch[InstanceType] = _save_inst

    def _save_global(self, path, obj, name=None, pack=struct.pack):
        if name is None:
            name = obj.__name__

        module = getattr(obj, "__module__", None)
        if module is None:
            module = whichmodule(obj, name)

        try:
            __import__(module)
            mod = sys.modules[module]
            klass = getattr(mod, name)
        except (ImportError, KeyError, AttributeError):
            raise PicklingError(
                "Can't pickle %r: it's not found as %s.%s" %
                (obj, module, name))
        else:
            if klass is not obj:
                raise PicklingError(
                    "Can't pickle %r: it's not the same object as %s.%s" %
                    (obj, module, name))

        pickletype = None

        code = _extension_registry.get((module, name))
        if code:
            assert code > 0
            pickletype = EXT4
            stuff = pack("<i", code)
        
        if not pickletype:
            stuff = module + '\n' + name
            pickletype = GLOBAL

        array = self.file.save_array(path, str(stuff))
        self.file.set_attr(array, 'pickletype', pickletype)
    
    _dispatch[ClassType] = _save_global
    _dispatch[FunctionType] = _save_global
    _dispatch[BuiltinFunctionType] = _save_global
    _dispatch[TypeType] = _save_global

    def _save_numeric_array(self, path, obj):
        if not NumericArrayType_native:
            obj = numpy.asarray(obj)
        array = self.file.save_numeric_array(path, obj)
        self.file.set_attr(array, 'pickletype', NUMERIC)
        return array
    _dispatch[NumericArrayType] = _save_numeric_array

    def _save_numpy_array(self, path, obj):
        if not NumpyArrayType_native:
            obj = numpy.asarray(obj)
        array = self.file.save_numeric_array(path, obj)
        self.file.set_attr(array, 'pickletype', NUMPY)
        return array
    _dispatch[NumpyArrayType] = _save_numpy_array

    def _save_numarray_array(self, path, obj):
        if not NumarrayArrayType_native:
            obj = numpy.asarray(obj)
        array = self.file.save_numeric_array(path, obj)
        self.file.set_attr(array, 'pickletype', NUMARRAY)
        return array
    _dispatch[NumarrayArrayType] = _save_numarray_array


#############################################################################


class Unpickler(object):
    """
    Unpickles Python objects from a HDF5 file.

    Usage:
      1. Instantaniate
      2. Call `load` or `clear_memo` as needed

    You may wish to use a single instance of this class for multiple
    objects to preserve references. It should be safe to call the `load`
    method multiple times, for different paths.
    """
    def __init__(self, file, type_map=None):
        self.file = _FileInterface(file, type_map=None)
        self.memo = {}

    def clear_memo(self):
        self.memo = {}

    def load(self, path):
        if not path in self.memo:
            node = self.file.get_path(path)
            key = self.file.get_attr(node, 'pickletype')
            if key:
                f = self._dispatch[key]
                obj = f(self, node)
            else:
                obj = node.read()
            self.memo[path] = obj
        return self.memo[path]

    _dispatch = {}

    def _load_ref(self, node):
        path = self.file.get_attr(node, 'target')
        return self.load(path)
    _dispatch[REF] = _load_ref

    def _load_reduce(self, node):
        path = node._v_pathname
        args = self.load('%s/__/args' % path)

        if self.file.has_path('%s/__/func' % path):
            func = self.load('%s/__/func' % path)
            
            if args is None:
                warnings.warn("__basicnew__ special case is deprecated",
                              DeprecationWarning)
                obj = func.__basicnew__()
            else:
                obj = func(*args)
        else:
            cls = self.load('%s/__/cls' % path)
            obj = cls.__new__(cls, *args)

        self.memo[path] = obj

        if self.file.has_path('%s/__/listitems' % path):
            data = self.load('%s/__/listitems' % path)
            obj.extend(data)

        if self.file.has_path('%s/__/dictitems' % path):
            data = self.load('%s/__/dictitems' % path)
            for key, value in data.iteritems():
                obj[key] = value

        if self.file.has_path('%s/__/content' % path):
            state = self.load('%s/__/content' % path)
            if state is not None:
                self._setstate(obj, state)
        elif self.file.has_attr(node, 'has_reduce_content'):
            state = {}
            state = self._load_dict_content(node, state)
            self._setstate(obj, state)
        return obj
    _dispatch[REDUCE] = _load_reduce

    def _load_none(self, node):
        return None
    _dispatch[NONE] = _load_none

    def _load_bool(self, node):
        return self.file.load_array(node, bool)
    _dispatch[BOOL] = _load_bool

    def _load_int(self, node):
        return self.file.load_array(node, int)
    _dispatch[INT] = _load_int

    def _load_long(self, node):
        data = self.file.load_array(node, str)
        return decode_long(data)
    _dispatch[LONG] = _load_long

    def _load_float(self, node):
        return self.file.load_array(node, float)
    _dispatch[FLOAT] = _load_float

    def _load_complex(self, node):
        return self.file.load_array(node, complex)
    _dispatch[COMPLEX] = _load_complex

    def _load_string(self, node):
        return self.file.load_array(node, str)
    _dispatch[STRING] = _load_string

    def _load_unicode(self, node):
        data = self.file.load_array(node, str)
        return data.decode('utf-8')
    _dispatch[UNICODE] = _load_unicode

    def _load_list_content(self, node):
        if isinstance(node, tables.Array):
            return self.file.load_array(node, list)

        items = []
        self.memo[node._v_pathname] = items # avoid infinite loop

        def cmpfunc(a, b):
            c = len(a) - len(b)
            if c == 0:
                c = cmp(a, b)
            return c

        names = list(node._v_children)
        names.sort(cmpfunc)

        for name in names:
            items.append(self.load('%s/%s' % (node._v_pathname, name)))
        
        return items
    
    def _load_tuple(self, node):
        return tuple(self._load_list_content(node))
    _dispatch[TUPLE] = _load_tuple

    def _load_list(self, node):
        return self._load_list_content(node)
    _dispatch[LIST] = _load_list

    def _load_dict(self, node):
        path = node._v_pathname
        data = {}
        self.memo[path] = data
        return self._load_dict_content(node, data)

    def _load_dict_content(self, node, data):
        path = node._v_pathname
        strkeys = {}

        if '__' in node._v_children:
            n2 = node._v_children['__']
            for name in n2._v_children:
                if name.startswith('_'):
                    strkeys[name] = self.load('%s/__/%s' % (path, name))

        for key in node._v_children:
            if key == '__': continue

            if key in strkeys:
                realkey = strkeys[key]
            else:
                realkey = key

            data[realkey] = self.load('%s/%s' % (path, key))

        return data
    _dispatch[DICT] = _load_dict

    # INST and OBJ differ only in how they get a class object.  It's not
    # only sensible to do the rest in a common routine, the two routines
    # previously diverged and grew different bugs.
    # klass is the class to instantiate, and k points to the topmost mark
    # object, following which are the arguments for klass.__init__.
    def _instantiate(self, klass, args):
        instantiated = 0
        if (not args and type(klass) is ClassType and
                not hasattr(klass, "__getinitargs__")):
            try:
                value = _EmptyClass()
                value.__class__ = klass
                instantiated = 1
            except RuntimeError:
                # In restricted execution, assignment to inst.__class__ is
                # prohibited
                pass
        if not instantiated:
            try:
                value = klass(*args)
            except TypeError, err:
                raise TypeError, "in constructor for %s: %s" % (
                    klass.__name__, str(err)), sys.exc_info()[2]
        return value

    def _load_inst(self, node):
        path = node._v_pathname

        cls = self.load('%s/__/cls' % path)
        args = self.load('%s/__/args' % path)

        inst = self._instantiate(cls, args)

        self.memo[path] = inst

        if self.file.has_path('%s/__/content' % path):
            state = self.load('%s/__/content' % path)
        else:
            state = {}
            state = self._load_dict_content(node, state)
        self._setstate(inst, state)

        return inst
    _dispatch[INST] = _load_inst

    def _setstate(self, inst, state):
        setstate = getattr(inst, "__setstate__", None)
        if setstate:
            setstate(state)
            return
        
        slotstate = None
        if isinstance(state, tuple) and len(state) == 2:
            state, slotstate = state
        
        if state:
            try:
                inst.__dict__.update(state)
            except RuntimeError:
                # XXX In restricted execution, the instance's __dict__
                # is not accessible.  Use the old way of unpickling
                # the instance variables.  This is a semantic
                # difference when unpickling in restricted
                # vs. unrestricted modes.
                # Note, however, that cPickle has never tried to do the
                # .update() business, and always uses
                #     PyObject_SetItem(inst.__dict__, key, value) in a
                # loop over state.items().
                for k, v in state.items():
                    setattr(inst, k, v)
        if slotstate:
            for k, v in slotstate.items():
                setattr(inst, k, v)

    def _load_global(self, node):
        data = self.file.load_array(node, str)
        module, name = data.split('\n')
        return self._find_class(module, name)
    _dispatch[GLOBAL] = _load_global

    def _load_ext(self, node):
        data = self.file.load_array(node, str)
        code = marshal.loads('i' + data)
        return self._get_extension(code)
    _dispatch[EXT4] = _load_ext

    def _load_numeric_array(self, node):
        import Numeric
        return Numeric.asarray(node.read())
    _dispatch[NUMERIC] = _load_numeric_array

    def _load_numpy_array(self, node):
        import numpy
        return numpy.asarray(node.read())
    _dispatch[NUMPY] = _load_numpy_array

    def _load_numarray_array(self, node):
        import numarray
        return numarray.asarray(node.read())
    _dispatch[NUMARRAY] = _load_numarray_array

    def _get_extension(self, code):
        nil = []
        obj = _extension_cache.get(code, nil)
        if obj is not nil:
            self.append(obj)
            return
        key = _inverted_registry.get(code)
        if not key:
            raise ValueError("unregistered extension code %d" % code)
        obj = self._find_class(*key)
        _extension_cache[code] = obj
        return obj

    def _find_class(self, module, name):
        # Subclasses may override this
        __import__(module)
        mod = sys.modules[module]
        klass = getattr(mod, name)
        return klass


#############################################################################


class _EmptyClass:
    pass

pythonIdRE = re.compile('^[a-zA-Z_][a-zA-Z0-9_]*$')
reservedIdRE = re.compile('^_[cfgv]_')
def _checkNameValidity(name):
    """
    Check the validity of the `name` of a PyTables object,
    so that PyTables won't spew warnings or exceptions...
    """
    if not isinstance(name, basestring):  # Python >= 2.3
        raise TypeError()
    if name == '':
        raise ValueError()
    if name == '.':
        raise ValueError()
    if '/' in name:
        raise ValueError()
    if not pythonIdRE.match(name):
        raise ValueError()
    if keyword.iskeyword(name):
        raise ValueError()
    if reservedIdRE.match(name):
        raise ValueError()

def _check_pytables_name(key):
    try:
        _checkNameValidity(key)
        return True
    except:
        return False


#############################################################################


def dump(obj, file, path, type_map=None):
    """
    Dump a Python object to an open PyTables HDF5 file.

    :param obj:  the object to dump
    :param file: where to dump
    :type  file: tables.File
    :param path: path where to dump in the file
    :param type_map:
        mapping of Python basic types (str, int, ...) to numpy types.
        If ``None``, numpy's default mapping is used.
    """
    Pickler(file, type_map=type_map).dump(path, obj)

def load(file, path):
    """
    Load a Python object from an open PyTables HDF5 file.

    :param file: where to load from
    :type  file: tables.File
    :param path: path to the object in the file

    :return: loaded object
    """
    return Unpickler(file).load(path)

def dump_many(file, desc, type_map=None):
    """
    Dump multiple Python objects to an open PyTables HDF5 file,
    preserving any references between the objects.

    Calling `dump(file, path)` many times for objects keeping references
    to each other would result in duplicated data.

    :param file: where to dump
    :type  file: tables.File
    :param desc: a list of (path, obj)
    :param type_map:
        mapping of Python basic types (str, int, ...) to numpy types.
        If ``None``, numpy's default mapping is used.
    """
    p = Pickler(file, type_map=type_map)
    for path, obj in desc:
        p.dump(path, obj)

def load_many(file, paths):
    """
    Load multiple Python objects from the file, preserving any
    references between them.

    Calling `load(file, path)` many times for objects keeping references
    to each other would result to duplicated data.

    :param file: where to dump
    :type  file: tables.File
    :param paths: a list of paths where to load from

    :return: list of (path, object)
    """    
    p = Unpickler(file)
    r = []
    for path in paths:
        obj = p.load(path)
        r.append( (path, obj) )
    return r

