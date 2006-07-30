"""
==========
hdf5pickle
==========

:Author:  Pauli Virtanen <pav@iki.fi>

Create easily interfaceable representations of Python objects in HDF5
files. The aim of this module is to provide both

    (1) convenient Python object persistence
    (2) compatibility with non-Python applications

Point 2 is important, for example, if results from numerical
calculations should be easily transferable for example to a non-Python
visualization program. Writing code for dumping data creates mostly
unnecessary hassle.

This module implements `dump` and `load` methods analogous to those in
Python's pickle module. The programming interface corresponds to
pickle protocol 2, although the data is not serialized but saved in
HDF5 files.


Data format
-----------

The structure of a python object saved to a HDF5 node is as follows:

* basic types (None, bool, int, float, complex)::

    array [(1,), int/float] = NONE/BOOL/INT/FLOAT/COMPLEX
        .pickletype         = PICKLE_TYPE

* basic stream types (long, str, unicode).
  longs and unicodes are converted to strs (pickle.encode_long and utf-8)::

    array [(n,), int8] = DATA
        .pickletype    = LONG/STR/UNICODE
        .empty     = 1 #if len(DATA) == 0
    
   These can't at present be stored as real string arrays, as PyTables
   chops of strings at '\x00' chars.

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

__all__ = ['dump', 'load', 'Pickler', 'Unpickler']

__revision__ = "$Id: hdf5serialize.py 2937 2006-07-28 18:42:24Z pauli $"
__docformat__ = "restructuredtext en"
__version__ = "0.1"

from copy_reg import dispatch_table
from copy_reg import _extension_registry, _inverted_registry, _extension_cache
from types import *
import keyword, marshal
import tables, numarray, cPickle as pickle, re, struct, sys

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
    from numarray import ArrayType as NumarrayArrayType
except ImportError:
    NumarrayArrayType = None

try:
    from Numeric import ArrayType as NumericArrayType
except ImportError:
    NumericArrayType = None

try:
    from numpy import ArrayType as NumpyArrayType
except ImportError:
    NumpyArrayType = None


class FileInterface(object):
    def __init__(self, file):
        self.file = file
    
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
        except LookupError:
            return False

    def save_array(self, path, data):
        where, name = self._splitpath(path)
        type_ = type(data)

        if type_ in (tuple, list, str):
            if len(data) == 0:
                array = self.file.createArray(where, name, [0])
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
                return self.file.createArray(where, name, numarray.array(
                    map(ord, data), type=numarray.UInt8))
            return self.file.createArray(where, name, numarray.array(data))
        elif type_ is complex:
            return self.file.createArray(where, name, numarray.array(data))
        elif type_ in (int, float):
            return self.file.createArray(where, name, data)
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
                    return ''.join(map(chr, node.read()))
                return type_(node.read())
        elif type_ in (int, float, bool):
            return type_(node.read())
        elif type_ is complex:
            data = node.read()
            data.ravel()
            return complex(data[0])
        else:
            raise TypeError()

    def new_group(self, path):
        where, name = self._splitpath(path)
        return self.file.createGroup(where, name)


class Pickler(object):
    def __init__(self, file):
        self.file = FileInterface(file)
        
        self.paths = {}
        self.memo = {}

        self.proto = 2 # hard-coded

    def _keep_alive(self, obj):
        self.memo[id(obj)] = obj

    def clear_memo(self):
        self.paths = {}
        self.memo = {}

    def dump(self, obj, path):
        self.save(path, obj)

    def save(self, path, obj):
        x = self.paths.get(id(obj))
        if x:
            self.save_ref(path, x)
            return
        else:
            self.paths[id(obj)] = path

        self._keep_alive(obj)

        # Check if we have a dispatch for it
        t = type(obj)
        f = self.dispatch.get(t)
        if f:
            x = f(self, path, obj)
            return

        # Check for a class with a custom metaclass; treat as regular class
        try:
            issc = issubclass(t, TypeType)
        except TypeError: # t is not a class (old Boost; see SF #502085)
            issc = 0
        if issc:
            self.save_global(path, obj)
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
            self.save_global(path, obj, rv)
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
        self.save_reduce(path, obj=obj, *rv)

    dispatch = {}

    def save_ref(self, path, objpath):
        group = self.file.new_group(path)
        self.file.set_attr(group, 'target', objpath)
        self.file.set_attr(group, 'pickletype', REF)

    def save_reduce(self, path, func, args, state=None,
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
        if self.proto >= 2 and getattr(func, "__name__", "") == "__newobj__":
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

            self.save('%s/__/cls' % path, cls)
            self.save('%s/__/args' % path, args)
        else:
            self.save('%s/__/func' % path, func)
            self.save('%s/__/args' % path, args)

        if obj is not None:
            self._keep_alive(obj)

        if listitems is not None:
            self.save('%s/__/listitems' % path, list(listitems))

        if dictitems is not None:
            self.save('%s/__/dictitems' % path, dict(dictitems))

        if state is not None:
            self.file.set_attr(group, 'has_reduce_content', 1)
            if isinstance(state, dict):
                self._save_dict(path, state)
                self._keep_alive(state)
            else:
                self.save('%s/__/content' % path, state)

    def save_none(self, path, obj):
        array = self.file.save_array(path, 0)
        self.file.set_attr(array, 'pickletype', NONE)
    dispatch[NoneType] = save_none

    def save_bool(self, path, obj):
        array = self.file.save_array(path, int(obj))
        self.file.set_attr(array, 'pickletype', BOOL)
    dispatch[bool] = save_bool

    def save_int(self, path, obj):
        array = self.file.save_array(path, obj)
        self.file.set_attr(array, 'pickletype', INT)
    dispatch[IntType] = save_int

    def save_long(self, path, obj):
        array = self.file.save_array(path, str(encode_long(obj)))
        self.file.set_attr(array, 'pickletype', LONG)
    dispatch[LongType] = save_long

    def save_float(self, path, obj):
        array = self.file.save_array(path, obj)
        self.file.set_attr(array, 'pickletype', FLOAT)
    dispatch[FloatType] = save_float

    def save_complex(self, path, obj):
        array = self.file.save_array(path, obj)
        self.file.set_attr(array, 'pickletype', COMPLEX)
    dispatch[ComplexType] = save_complex

    def save_string(self, path, obj):
        node = self.file.save_array(path, obj)
        self.file.set_attr(node, 'pickletype', STRING)
    dispatch[StringType] = save_string

    def save_unicode(self, path, obj):
        node = self.file.save_array(path, obj.encode('utf-8'))
        self.file.set_attr(node, 'pickletype', UNICODE)
    dispatch[UnicodeType] = save_unicode

    def save_tuple(self, path, obj):
        try:
            array = self.file.save_array(path, obj)
            self.file.set_attr(array, 'pickletype', TUPLE)
            return array
        except TypeError:
            pass

        group = self.file.new_group(path)
        self.file.set_attr(group, 'pickletype', TUPLE)
        for i, item in enumerate(obj):
            self.save('%s/_%d' % (path, i), item)
        return group
    dispatch[TupleType] = save_tuple

    def save_list(self, path, obj):
        item = self.save_tuple(path, obj)
        self.file.set_attr(item, 'pickletype', LIST)
    dispatch[ListType] = save_list

    ok_dict_key_re = re.compile('^[a-zA-Z_][a-zA-Z0-9_]*$')
    def save_dict(self, path, obj):
        group = self.file.new_group(path)
        self.file.set_attr(group, 'pickletype', DICT)
        self._save_dict(path, obj)

    def _save_dict(self, path, obj):
        strkeys = {}
        seen = {}
        keyi = 0
        for key in obj.iterkeys():
            if (isinstance(key, str) and check_pytables_name(key)
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
            self.save('/'.join([path, strkeys[key]]), value)
            if not strkeys[key] is key:
                if not hassub:
                    self.file.new_group('%s/__' % path)
                    hassub = True
                self.save('%s/__/%s' % (path, strkeys[key]), key)

    dispatch[DictionaryType] = save_dict
    if not PyStringMap is None:
        dispatch[PyStringMap] = save_dict
    
    def save_inst(self, path, obj):
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
        self.save('%s/__/cls' % path, cls)
        self.save('%s/__/args' % path, args)

        if isinstance(stuff, dict):
            self._save_dict(path, stuff)
            self._keep_alive(stuff)
        else:
            self.save('%s/__/content' % path, stuff)
    dispatch[InstanceType] = save_inst

    def save_global(self, path, obj, name=None, pack=struct.pack):
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
        if self.proto >= 2:
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
    
    dispatch[ClassType] = save_global
    dispatch[FunctionType] = save_global
    dispatch[BuiltinFunctionType] = save_global
    dispatch[TypeType] = save_global

    def save_numeric_array(self, path, obj):
        array = self.file.save_numeric_array(path, obj)
        self.file.set_attr(array, 'pickletype', NUMERIC)
        return array
    dispatch[NumericArrayType] = save_numeric_array

    def save_numpy_array(self, path, obj):
        array = self.file.save_numeric_array(path, obj)
        self.file.set_attr(array, 'pickletype', NUMPY)
        return array
    dispatch[NumpyArrayType] = save_numpy_array

    def save_numarray_array(self, path, obj):
        array = self.file.save_numeric_array(path, obj)
        self.file.set_attr(array, 'pickletype', NUMARRAY)
        return array
    dispatch[NumarrayArrayType] = save_numarray_array


class Unpickler(object):
    def __init__(self, file, protocol=None):
        self.file = FileInterface(file)
        self.memo = {}
        self.paths = {}

    def load(self, path):
        if not path in self.memo:
            node = self.file.get_path(path)
            key = self.file.get_attr(node, 'pickletype')
            if key:
                f = self.dispatch[key]
                obj = f(self, node)
            else:
                obj = node.read()
            self.memo[path] = obj
        return self.memo[path]

    dispatch = {}

    def load_ref(self, node):
        path = self.file.get_attr(node, 'target')
        return self.load(path)
    dispatch[REF] = load_ref

    def load_reduce(self, node):
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
            state = self._load_dict(node, state)
            self._setstate(obj, state)
        return obj
    dispatch[REDUCE] = load_reduce

    def load_none(self, node):
        return None
    dispatch[NONE] = load_none

    def load_bool(self, node):
        return self.file.load_array(node, bool)
    dispatch[BOOL] = load_bool

    def load_int(self, node):
        return self.file.load_array(node, int)
    dispatch[INT] = load_int

    def load_long(self, node):
        data = self.file.load_array(node, str)
        return decode_long(data)
    dispatch[LONG] = load_long

    def load_float(self, node):
        return self.file.load_array(node, float)
    dispatch[FLOAT] = load_float

    def load_complex(self, node):
        return self.file.load_array(node, complex)
    dispatch[COMPLEX] = load_complex

    def load_string(self, node):
        return self.file.load_array(node, str)
    dispatch[STRING] = load_string

    def load_unicode(self, node):
        data = self.file.load_array(node, str)
        return data.decode('utf-8')
    dispatch[UNICODE] = load_unicode

    def _load_list(self, node):
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
    
    def load_tuple(self, node):
        return tuple(self._load_list(node))
    dispatch[TUPLE] = load_tuple

    def load_list(self, node):
        return self._load_list(node)
    dispatch[LIST] = load_list

    def load_dict(self, node):
        path = node._v_pathname
        data = {}
        self.memo[path] = data
        return self._load_dict(node, data)

    def _load_dict(self, node, data):
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
    dispatch[DICT] = load_dict

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

    def load_inst(self, node):
        path = node._v_pathname

        cls = self.load('%s/__/cls' % path)
        args = self.load('%s/__/args' % path)

        inst = self._instantiate(cls, args)

        self.memo[path] = inst

        if self.file.has_path('%s/__/content' % path):
            state = self.load('%s/__/content' % path)
        else:
            state = {}
            state = self._load_dict(node, state)
        self._setstate(inst, state)

        return inst
    dispatch[INST] = load_inst

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

    def load_global(self, node):
        data = self.file.load_array(node, str)
        module, name = data.split('\n')
        return self.find_class(module, name)
    dispatch[GLOBAL] = load_global

    def load_ext(self, node):
        data = self.file.load_array(node, str)
        code = marshal.loads('i' + data)
        return self.get_extension(code)
    dispatch[EXT4] = load_ext

    def load_numeric_array(self, node):
        import Numeric
        return Numeric.asarray(node.read())
    dispatch[NUMERIC] = load_numeric_array

    def load_numpy_array(self, node):
        import numpy
        return numpy.asarray(node.read())
    dispatch[NUMPY] = load_numpy_array

    def load_numarray_array(self, node):
        import numarray
        return numarray.asarray(node.read())
    dispatch[NUMARRAY] = load_numarray_array

    def get_extension(self, code):
        nil = []
        obj = _extension_cache.get(code, nil)
        if obj is not nil:
            self.append(obj)
            return
        key = _inverted_registry.get(code)
        if not key:
            raise ValueError("unregistered extension code %d" % code)
        obj = self.find_class(*key)
        _extension_cache[code] = obj
        return obj

    def find_class(self, module, name):
        # Subclasses may override this
        __import__(module)
        mod = sys.modules[module]
        klass = getattr(mod, name)
        return klass

class _EmptyClass:
    pass

pythonIdRE = re.compile('^[a-zA-Z_][a-zA-Z0-9_]*$')
reservedIdRE = re.compile('^_[cfgv]_')
def checkNameValidity(name):
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

def check_pytables_name(key):
    try:
        checkNameValidity(key)
        return True
    except:
        return False

def dump(obj, file, path):
    """
    Dump a Python object to an open PyTables HDF5 file.
    
    :Parameters:
      - `obj`: the object to save
      - `file`: `tables.File` handle (`tables.File`)
      - `path`: path to the object in the file (string)
    """
    Pickler(file).dump(obj, path)

def load(file, path):
    """
    Load a Python object from an open PyTables HDF5 file.
    
    :Parameters:
      - `file`: file handle (`tables.File`)
      - `path`: path to the object in the file (string)
    """
    return Unpickler(file).load(path)
