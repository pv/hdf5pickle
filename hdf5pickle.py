from copy_reg import dispatch_table
from copy_reg import _extension_registry, _inverted_registry, _extension_cache
from types import *
import keyword, marshal
import tables, numarray, cPickle as pickle, re, struct, sys

from pickle import whichmodule, PicklingError, FLOAT, INT, LONG, NONE, \
     REDUCE, STRING, UNICODE, GLOBAL, DICT, INST, LIST, TUPLE, EXT4, \
     encode_long, decode_long

__revision__ = "$Id: hdf5serialize.py 2937 2006-07-28 18:42:24Z pauli $"
__docformat__ = "restructuredtext en"

BOOL    = 'BB'
REF     = 'RR'
COMPLEX = 'CC'

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

def _splitpath(s):
    i = s.rindex('/')
    where, name = s[:i], s[(i+1):]
    if where == '': where = '/'
    return where, name

def _setattr(obj, attr, value):
    if isinstance(obj, tables.Group):
        obj._f_setAttr(attr, value)
    else:
        setattr(obj.attrs, attr, value)

def _hasattr(obj, attr):
    try:
        _getattr(obj, attr)
        return True
    except AttributeError:
        return False

def _getattr(obj, attr):
    if isinstance(obj, tables.Group):
        return obj._f_getAttr(attr)
    else:
        return getattr(obj.attrs, attr)

class Pickler(object):

    def __init__(self, file):
        self.file = file
        
        self.paths = {}
        self.memo = {}

        self.proto = 2 # hard-coded

    def _get_path(self, path):
        try:
            return self.file.getNode(path)
        except LookupError:
            where, name = _splitpath(path)
            return self.file.createGroup(where, name)

    def _has_path(self, path):
        try:
            self.file.getNode(path)
            return True
        except LookupError:
            return False

    def _save_array(self, path, data):
        where, name = _splitpath(path)
        if ((not hasattr(data, 'shape') or data.shape != ()) and
                hasattr(data, '__len__') and len(data) == 0):
            array = self.file.createArray(where, name, 0)
            _setattr(array, 'empty', 1)
            return array
        else:
            return self.file.createArray(where, name, data)

    def _new_group(self, path):
        where, name = _splitpath(path)
        return self.file.createGroup(where, name)

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
        group = self._new_group(path)
        _setattr(group, 'target', objpath)
        _setattr(group, 'pickletype', REF)

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

        group = self._new_group(path)
        self._new_group(path + '/__')

        _setattr(group, 'pickletype', REDUCE)

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

        if obj:
            self._keep_alive(obj)

        if listitems is not None:
            self.save('%s/__/listitems' % path, list(listitems))

        if dictitems is not None:
            self.save('%s/__/dictitems' % path, dict(dictitems))

        if state is not None:
            if isinstance(state, dict):
                self._save_dict(path, state)
                self._keep_alive(state)
            else:
                self.save('%s/__/content' % path, state)

    def save_none(self, path, obj):
        array = self._save_array(path, 0)
        _setattr(array, 'pickletype', NONE)
    dispatch[NoneType] = save_none

    def save_bool(self, path, obj):
        array = self._save_array(path, obj)
        _setattr(array, 'pickletype', BOOL)
    dispatch[bool] = save_bool

    def save_int(self, path, obj):
        array = self._save_array(path, obj)
        _setattr(array, 'pickletype', INT)
    dispatch[IntType] = save_int

    def save_long(self, path, obj):
        array = self._save_array(path, str(encode_long(obj)))
        _setattr(array, 'pickletype', LONG)
    dispatch[LongType] = save_long

    def save_float(self, path, obj):
        array = self._save_array(path, obj)
        _setattr(array, 'pickletype', FLOAT)
    dispatch[FloatType] = save_float

    def save_complex(self, path, obj):
        array = self._save_array(path, numarray.array(obj))
        _setattr(array, 'pickletype', COMPLEX)
    dispatch[ComplexType] = save_complex

    def save_string(self, path, obj):
        node = self._save_array(path, obj)
        _setattr(node, 'pickletype', STRING)
    dispatch[StringType] = save_string

    def save_unicode(self, path, obj):
        node = self._save_array(path, unicode(obj).encode('utf-8'))
        _setattr(node, 'pickletype', UNICODE)
    dispatch[UnicodeType] = save_unicode

    def save_tuple(self, path, obj):
        try:
            if len(obj) == 0: raise TypeError()

            t = type(obj[0])
            if not t in (str, int, float, complex):
                raise TypeError()
            for i in obj:
                if type(i) != t: raise TypeError()
            
            array = self._save_array(path, obj)
            _setattr(array, 'pickletype', TUPLE)
            return array
        except TypeError:
            pass

        group = self._new_group(path)
        _setattr(group, 'pickletype', TUPLE)
        for i, item in enumerate(obj):
            self.save('%s/_%d' % (path, i), item)
        return group
    dispatch[TupleType] = save_tuple

    def save_list(self, path, obj):
        item = self.save_tuple(path, obj)
        _setattr(item, 'pickletype', LIST)
    dispatch[ListType] = save_list

    ok_dict_key_re = re.compile('^[a-zA-Z_][a-zA-Z0-9_]*$')
    def save_dict(self, path, obj):
        group = self._new_group(path)
        _setattr(group, 'pickletype', DICT)
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

        hassub = self._has_path('%s/__' % path)

        for key, value in obj.iteritems():
            self.save('/'.join([path, strkeys[key]]), value)
            if not strkeys[key] is key:
                if not hassub:
                    self._new_group('%s/__' % path)
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

        group = self._new_group(path)
        _setattr(group, 'pickletype', INST)

        self._new_group("%s/__" % path)
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

        array = self._save_array(path, str(stuff))
        _setattr(array, 'pickletype', pickletype)
    
    dispatch[ClassType] = save_global
    dispatch[FunctionType] = save_global
    dispatch[BuiltinFunctionType] = save_global
    dispatch[TypeType] = save_global



class Unpickler:
    def __init__(self, file, protocol=None):
        self.file = file
        self.memo = {}
        self.paths = {}

    def _get_path(self, path):
        return self.file.getNode(path)

    def _has_path(self, path):
        try:
            self.file.getNode(path)
            return True
        except LookupError:
            return False

    def _load_array(self, node, type_):
        if _hasattr(node, 'empty'):
            return type_()
        else:
            return type_(node.read())

    def load(self, path):
        if not path in self.memo:
            node = self._get_path(path)
            key = _getattr(node, 'pickletype')
            if key:
                f = self.dispatch[key]
                obj = f(self, node)
            else:
                obj = node.read()
            self.memo[path] = obj
        return self.memo[path]

    dispatch = {}

    def load_ref(self, node):
        path = _getattr(node, 'target')
        return self.load(path)
    dispatch[REF] = load_ref

    def load_reduce(self, node):
        path = node._v_pathname
        args = self.load('%s/__/args' % path)

        if self._has_path('%s/__/func' % path):
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

        if self._has_path('%s/__/listitems' % path):
            data = self.load('%s/__/listitems' % path)
            obj.extend(data)

        if self._has_path('%s/__/dictitems' % path):
            data = self.load('%s/__/dictitems' % path)
            for key, value in data.iteritems():
                obj[key] = value

        if self._has_path('%s/__/content' % path):
            state = self.load('%s/__/content' % path)
            if state is not None:
                self._setstate(obj, state)
        else:
            state = {}
            state = self._load_dict(node, state)
            self._setstate(obj, state)
        return obj
    dispatch[REDUCE] = load_reduce

    def load_none(self, node):
        return None
    dispatch[NONE] = load_none

    def load_bool(self, node):
        return self._load_array(node, bool)
    dispatch[BOOL] = load_bool

    def load_int(self, node):
        return self._load_array(node, int)
    dispatch[INT] = load_int

    def load_long(self, node):
        data = self._load_array(node, str)
        return decode_long(data)
    dispatch[LONG] = load_long

    def load_float(self, node):
        return self._load_array(node, float)
    dispatch[FLOAT] = load_float

    def load_complex(self, node):
        data = node.read()
        data.ravel()
        return complex(data[0])
    dispatch[COMPLEX] = load_complex

    def load_string(self, node):
        return self._load_array(node, str)
    dispatch[STRING] = load_string

    def load_unicode(self, node):
        data = self._load_array(node, str)
        return data.decode('utf-8')
    dispatch[UNICODE] = load_unicode

    def _load_list(self, node):
        if isinstance(node, tables.Array):
            return node.read()

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

        if self._has_path('%s/__/content' % path):
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
        data = self._load_array(node, str)
        module, name = data.split('\n')
        return self.find_class(module, name)
    dispatch[GLOBAL] = load_global

    def load_ext(self, node):
        data = self._load_array(node, str)
        code = marshal.loads('i' + data)
        return self.get_extension(code)
    dispatch[EXT4] = load_ext

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
    Pickler(file).dump(obj, path)

def load(file, path):
    return Unpickler(file).load(path)
