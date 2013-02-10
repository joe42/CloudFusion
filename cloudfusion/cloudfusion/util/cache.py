'''
Created on 03.06.2011

@author: joe
'''
import time

class Entry(object):
    '''
    Cache entry.
    '''
    def __init__(self, value, dirty, modified=None, updated=None):
        self.value = value
        self.dirty = dirty
        self.updated = updated if updated != None else time.time()
        self.modified = modified if modified != None else time.time()

class Cache(object):
    def __init__(self, expiration_time):
        """Return a Cache instance with entries expiring after :param:`expiration_time` seconds."""
        self.entries = {}
        self.expire = expiration_time
        
    def refresh(self, key, disk_value, modified):
        """ Refreshes an entry with :param:`disk_value`, if :param:`modified` is bigger than the entry's modified date. """
        if key in self.entries:
            disk_entry_is_newer = modified > self.entries[key].modified
            if not disk_entry_is_newer:
                return
        self.entries[key] = Entry(value=disk_value, dirty=False, modified=modified)
        
    def write(self, key, value):
        self.entries[key] = Entry(value=value, dirty=True)
        
    def get_keys(self):
        return self.entries.keys()
    
    def get_modified(self, key):
        return self.entries[key].modified
    
    def _get_size_of_entry(self, entry):
        try:
            return entry.value.get_size()
        except:
            try:
                return entry.value.size
            except:
                return len(str(entry.value))
    
    def get_size_of_dirty_data(self):
        ret = 0
        for key in self.entries:
            if self.is_dirty(key):
                ret+= self._get_size_of_entry(self.entries[key])
        return ret
    
    def get_size_of_cached_data(self):
        ret = 0
        for key in self.entries:
            ret+= self._get_size_of_entry(self.entries[key])
        return ret
    
    def exists(self, key):
        if key in self.entries:
            return True
        return False

    def is_expired(self, key):
        return time.time() > self.entries[key].updated + self.expire
    
    def update(self, key):
        entry = self.entries[key]
        entry.updated = time.time() 
        self.entries[key] = entry
        
    def flush(self, key):
        self.update(key) 
        self.set_dirty(key, False) 
    
    def get_value(self, key):
        return self.entries[key].value
    
    def is_dirty(self, key):
        return self.entries[key].dirty
    
    def set_dirty(self, key, is_dirty):
        entry = self.entries[key]
        entry.dirty = is_dirty
        self.entries[key] = entry
    
    def delete(self, key):
        try:
            del self.entries[key]
        except KeyError:
            pass
