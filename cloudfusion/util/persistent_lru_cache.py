'''
Created on Feb 5, 2013
'''
from cloudfusion.util.lru_cache import *
import shelve
import os
import atexit
import tempfile

LASTFILEID = "############################## #### file_id ###### #############################'"

class PersistentLRUCache(LRUCache):
    '''
    A persistent LRU cache for restricting memory usage to a minimum and 
    carrying the cache information over several sessions even after application crashes.
    This subclass of LRUCache restricts values to byte strings, so for instance,
    you cannot store integers as values.
    '''


    def __init__(self, directory, expiration_time=60, maxsize_in_MB=2000):
        """Return an LRUCache instance.
        
        :param expiration_time: Time in seconds until entries are expired.
        :param maxsize_in_MB: Approximate limit of the cache in MB.
        :param directory: Directory to store persistent data, also serves as identifier for a persistent cache instance. 
        """
        super( PersistentLRUCache, self ).__init__(expiration_time, maxsize_in_MB)
        self.filename = "Database"
        self.directory = directory
        try:
            os.makedirs(directory)
        except Exception, e:
            pass
        self.entries = shelve.open(directory+"/"+self.filename)
        atexit.register( lambda : self._close() )
        if not LISTHEAD in self.entries: #first time use 
            self.entries[LISTHEAD] = None
            self.entries[LISTTAIL] = None
            self.entries[CACHESIZE] = 0
            self.entries[LASTFILEID] = 0
            self.entries.sync()
    
    def _close(self):
        try:
            self.entries.close()
        except Exception, e:
            pass
    
    def get_keys(self):
        keys = super( PersistentLRUCache, self ).get_keys()
        keys.remove(LASTFILEID)
        return keys
        
    def refresh(self, key, disk_value, modified):
        """ Refreshes an entry with *disk_value*, if *modified* is bigger than the entry's modified date.
        :param maxsize_in_MB: Approximate limit of the cache in MB. """
        if key in self.entries:
            disk_entry_is_newer = modified > self.entries[key].modified
            if not disk_entry_is_newer:
                return
            filename = self.entries[key].value
        else:
            self.entries[LASTFILEID] += 1
            filename = self.directory+"/"+str(self.entries[LASTFILEID])
        super( PersistentLRUCache, self ).refresh(key, filename, modified)
        self._write_to_file(filename, key, disk_value)
        self._resize()
        self.entries.sync()
        
    def _write_to_file(self, filename, key, value):
        if key in self.entries: # check if file exists
            self.entries[CACHESIZE] -= self._get_persistent_size(filename)
        fh = open(filename,"w")
        fh.write(value)
        fh.close()
        self.entries[CACHESIZE] += self._get_persistent_size(filename)
        self._resize()
    
    def _get_persistent_size(self, filepath):
        try:
            return os.path.getsize(filepath)
        except Exception, e: # log error
            return 0
    
    def _get_file_content(self, filepath):
        with open(filepath) as fh:
            return fh.read()
    
    def write(self, key, value): 
        if key in self.entries:
            filename = self.entries[key].value
        else:
            self.entries[LASTFILEID] += 1
            filename = self.directory+"/"+str(self.entries[LASTFILEID])
        super( PersistentLRUCache, self ).write(key, filename)
        self._write_to_file(filename, key, value)
        self.entries.sync()
    
    def set_modified(self, key, modified):
        super( PersistentLRUCache, self ).set_modified( key, modified)
        self.entries.sync()

    def get_size_of_dirty_data(self):
        ret = 0
        for key in self.entries:
            if key not in [LISTHEAD, LISTTAIL, CACHESIZE, LASTFILEID] and self.is_dirty(key):
                ret += self._get_persistent_size(self.entries[key].value)
        return ret
    
    def get_value(self, key):
        filename = super( PersistentLRUCache, self ).get_value(key)
        self.entries.sync()
        return self._get_file_content(filename)
    
    def peek_file(self, key):
        """Like peek, but memory efficient
        The temporary file needs to be closed if it is not used anymore
        :returns: temporary file object with the value """
        ret = tempfile.NamedTemporaryFile(delete=False, suffix="_persistent_lru")
        filepath = super( PersistentLRUCache, self ).get_value(key)
        with open(filepath) as fh:
            while True:
                data = fh.read(1000000)
                if data == '':
                    break
                ret.write(data)
            ret.seek(0)
        return ret
    
    def peek(self, key):
        return self._get_file_content(super( PersistentLRUCache, self ).peek(key))
    
    def _get_size_of_entry(self, entry):
        return self._get_persistent_size(entry.value)
            
    
    def delete(self, key):
        """Remove current entry associated with key from the LRU queue and delete its persistent representation."""
        file_to_delete = None
        if key in self.entries:
            file_to_delete = self.entries[key].value
        super( PersistentLRUCache, self ).delete(key)
        if file_to_delete:
            os.remove(file_to_delete)
        self.entries.sync()
