#!/usr/bin/env python
import types
from multiprocessing import RLock

class SynchronizeMethodWrapper:
    """
    Wrapper object for a method to be called.
    """
    def __init__( self, obj, func, name, rlock ):
        self.obj, self.func, self.name = obj, func, name
        self.rlock = rlock
        assert obj is not None
        assert func is not None
        assert name is not None

    def __call__( self, *args, **kwds ):
        """
        This method gets called before a method is called to sync access to the core object.
        """
        with self.rlock:
            rval = self.func(*args, **kwds)
            return rval


class MPSynchronizeProxy(object):
    """
    Proxy object that synchronizes access to a core object methods and attributes that don't start with _ for multiple processes.
    """
    def __init__( self, core, private_methods_to_synchronize=[] ):
        self._obj = core
        self.rlock = RLock()
        self.methods_to_synchronize = private_methods_to_synchronize or [] # python quirks for mutable default parameter

    def __getattribute__( self, name ):
        """
        Return a proxy wrapper object if this is a method call.
        """
        if name == 'methods_to_synchronize' or (name.startswith('_') and not name in self.methods_to_synchronize):
            return object.__getattribute__(self, name)
        else:
            att = getattr(self._obj, name)
            if type(att) is types.MethodType:
                return SynchronizeMethodWrapper(self, att, name, object.__getattribute__(self, "rlock"))
            else:
                return att

    def __setitem__( self, key, value ):
        """
        Delegate [] syntax.
        """
        name = '__setitem__'
        with self.rlock:
            att = getattr(self._obj, name)
            pmeth = SynchronizeMethodWrapper(self, att, name, self.rlock)
            pmeth(key, value)

