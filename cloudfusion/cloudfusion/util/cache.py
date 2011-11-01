'''
Created on 03.06.2011

@author: joe
'''
import time

class Cache(object):
    def __init__(self, expiration_time):
        self.entries = {}
        self.expire = expiration_time
        
    def refresh(self, key, disk_value, modified):
        """ Refreshes a entries entry with :param:`disk_value`, if :param:`modified` is bigger than the entries entry's modified date. """
        if key in self.entries:
            disk_entry_is_newer = modified > self.entries[key]['modified']
            if not disk_entry_is_newer:
                return
        entry = {}
        entry['value'] = disk_value
        entry['updated'] = time.time()
        entry['modified'] = modified
        entry['dirty'] = False
        self.entries[key] = entry
        
    def write(self, key, value):
        entry = {}
        entry['value'] = value
        entry['updated'] = time.time()
        entry['modified'] = time.time()
        entry['dirty'] = True
        self.entries[key] = entry
        
    def get_keys(self):
        return self.entries
    
    def get_modified(self, key):
        return self.entries[key]['modified']
    
    def get_size_of_dirty_data(self):
        ret = 0
        for entry in self.entries:
            if self.is_dirty(entry):
                try:
                    ret+= self.get_value(entry).get_size()
                except:
                    try:
                        ret+= self.get_value(entry).size
                    except:
                        ret += len(str(self.get_value(entry)))
        return ret
    
    def get_size_of_cached_data(self):
        ret = 0
        for entry in self.entries:
                try:
                    ret+= self.get_value(entry).get_size()
                except:
                    try:
                        ret+= self.get_value(entry).size
                    except:
                        ret += len(str(self.get_value(entry)))
        return ret
    
    def exists(self, key):
        if key in self.entries:
            return True
        return False

    def is_expired(self, key):
        return time.time() > self.entries[key]['updated'] + self.expire
    
    def update(self, key):
        self.entries[key]['updated'] = time.time() 
        
    def flush(self, key):
        self.update(key) 
        self.set_dirty(key, False) 
    
    def get_value(self, key):
        return self.entries[key]['value']
    
    def is_dirty(self, key):
        return self.entries[key]['dirty']
    
    def set_dirty(self, key, is_dirty):
        self.entries[key]['dirty'] = is_dirty
    
    def delete(self, key):
        try:
            del self.entries[key]
        except KeyError:
            pass
