from cloudfusion.util.cache import *
import time
from multiprocessing import Manager

LISTHEAD = '########################## ######## list_head ###### #############################'
LISTTAIL = '########################## ######## list_tail ###### #############################'
CACHESIZE = '############################## #### cache_size ###### #############################'

class LinkedEntry(Entry):
    '''
    Cache entry with next and previous links for chaining into a doubly linked list and a key for direct reference.
    '''
    def __init__(self, value, dirty, modified=None, updated=None, key=None, next=None, prev=None):
        super( LinkedEntry, self ).__init__(value, dirty, modified, updated)
        self.next = next
        self.prev = prev
        self.key = key

class MPLRUCache(Cache):
    '''
    Multiprocessing save Least Recently Used cache. When adding an entry to the cache exceeds its size limit,
    the cache discards dirty entries starting with the least recently used.
    However, dirty entries or the last entry in the cache are not deleted. 
    '''


    def __init__(self, expiration_time=60, maxsize_in_MB=200):
        """Return an LRUCache instance.
        
        :param expiration_time: Time in seconds until entries are expired.
        :param maxsize_in_MB: Approximate limit of the cache in MB.
        """
        super( MPLRUCache, self ).__init__(expiration_time)
        #self.entries = shelve.open(filename)
        #self.list_head = self.entries[LISTHEAD] if LISTHEAD in self.entries else None
        #self.entries[LISTTAIL] = self.entries[LISTTAIL] if LISTTAIL in self.entries else None
        self.maxsize = maxsize_in_MB
        self.resize_interval = 1
        manager = Manager()
        self.entries = manager.dict()
        self.entries[LISTHEAD] = None
        self.entries[LISTTAIL] = None
        self.entries[CACHESIZE] = 0
        self._last_resize = 0
    
    def set_resize_intervall(self, seconds):
        '''Do not try to reduce the cache size again before *seconds* have passed.
        Resize operations are slow, especially when the cache has many elements.
        It can be set to *0*, if you want to make sure that clean entries are removed as fast as possible.
        :param seconds: float stating how much time in seconds must pass before a cache size reduction is tried again'''
        self.resize_interval = seconds
        
    def get_resize_intervall(self):
        ''':returns: The minimal interval between consecutive calls to reduce cache size as a float in seconds.
        The default is one second.'''
        return self.resize_interval
        
    
    def get_keys(self):
        keys = super( MPLRUCache, self ).get_keys()
        keys.remove(LISTHEAD)
        keys.remove(LISTTAIL)
        keys.remove(CACHESIZE)
        return keys
        
    def _store_to_dict(self, linkedEntry):
        """This is only important for subclasses using shelve with writeback=False as a dictionary."""
        self.entries[linkedEntry.key] = linkedEntry

    def _move_used_entry_to_head(self, key):
        """Put existing entry associated with *key* in front of the LRU queue."""
        used_entry = self.entries[key]
        if not used_entry.next: #entry is list head 
            return
        self._unlink(key)
        previous_listhead = self._get_listhead_entry()
        used_entry.next = None
        used_entry.prev = previous_listhead.key
        self.entries[LISTHEAD] = used_entry.key
        previous_listhead.next = used_entry.key
        self._store_to_dict(previous_listhead)
        self._store_to_dict(used_entry)
        
    def _get_listtail_entry(self):
        tail = self.entries[LISTTAIL]
        return self.entries[tail] if tail else None
    
    def _get_listhead_entry(self):
        head = self.entries[LISTHEAD]
        return self.entries[head] if head else None

    def refresh(self, key, disk_value, modified):
        """ Refreshes an entry with *disk_value*, if *modified* is bigger than the entry's modified date. """
        if key in self.entries:
            disk_entry_is_newer = modified > self.entries[key].modified
            if not disk_entry_is_newer:
                return
            else:
                self.delete(key)
        entry = LinkedEntry(value=disk_value, dirty=False, modified=modified, key=key)
        previous_listhead = self._get_listhead_entry()
        entry.prev = previous_listhead.key if previous_listhead else None
        self.entries[LISTHEAD] = entry.key
        if previous_listhead:
            previous_listhead.next = entry.key
            self._store_to_dict(previous_listhead)
        else: #if list_head is empty, this is the first element in the list -> set tail to first element
            self.entries[LISTTAIL] = entry.key
        self.entries[key] = entry
        self.entries[CACHESIZE] += self._get_size_of_entry(entry)
        self._resize()
        
    def write(self, key, value): 
        self.delete(key)
        entry = LinkedEntry(value=value, dirty=True, key=key)
        previous_listhead = self._get_listhead_entry()
        entry.prev = previous_listhead.key if previous_listhead else None
        self.entries[LISTHEAD] = entry.key
        if previous_listhead:
            previous_listhead.next = entry.key
            self._store_to_dict(previous_listhead)
        else: #if list_head is empty, this is the first element in the list -> set tail to first element
            self.entries[LISTTAIL] = entry.key
        self.entries[key] = entry
        self.entries[CACHESIZE] += self._get_size_of_entry(entry)
        self._resize() 
        
    def bulk_write(self, dict): 
        ''':param dict: dictionary with key value mapping'''
        first_iteration = True
        for k,v in dict.iteritems():
            self.delete(k)
            entry = LinkedEntry(value=v, dirty=True, key=k)
            if first_iteration:
                first_iteration = False
                previous_listhead = self._get_listhead_entry()
                entry.prev = previous_listhead.key if previous_listhead else None
                if previous_listhead:
                    previous_listhead.next = entry.key
                    self._store_to_dict(previous_listhead)
                else: #if list_head is empty, this is the first element in the list -> set tail to first element
                    self.entries[LISTTAIL] = entry.key
                self.entries[k] = entry
                self.entries[CACHESIZE] += self._get_size_of_entry(entry)
                previous_listhead = entry
                continue
            entry.prev = previous_listhead.key
            previous_listhead.next = entry.key
            self._store_to_dict(previous_listhead)
            self.entries[k] = entry
            previous_listhead = entry
        self.entries[LISTHEAD] = entry.key
        self.entries[CACHESIZE] += sys.getsizeof(dict.values())
        self._resize() 

    def get_size_of_dirty_data(self):
        ret = 0
        for key in self.entries.keys():
            if key not in [LISTHEAD, LISTTAIL, CACHESIZE] and self.is_dirty(key):
                ret += self._get_size_of_entry(self.entries[key])
        return ret
    
    def get_size_of_cached_data(self):
        return self.entries[CACHESIZE]

    def get_value(self, key):
        self._move_used_entry_to_head(key)
        return self.entries[key].value
    
    def _resize(self): 
        """Resize cache to maxsize."""
        if self.entries[CACHESIZE]/1000000 < self.maxsize or self._last_resize+self.resize_interval>time.time():
            return
        entry = self._get_listtail_entry()
        while self.entries[CACHESIZE]/1000000 >= self.maxsize and entry.next:
            if entry.dirty == False and self.is_expired(entry.key):
                self.delete(entry.key)
            entry = self.entries[entry.next]
        self._last_resize = time.time()

            
    def peek(self, key):
        """Get value associated with *key*. 
        This does not count as the entry being used."""
        return self.entries[key].value
    
    def get_dirty_lru_entries(self, num):
        """Get list of *num* dirty least recently used entries.
        returns: list of keys of the *num* least recently used entries"""
        ret = []
        entry = self._get_listtail_entry()
        while entry:
            if num <= len(ret):
                break
            if entry.dirty:
                ret.append(entry.key)
            if not entry.next:
                break
            entry = self.entries[entry.next]
        return ret
    
    def delete(self, key):
        """Remove current entry associated with key from the LRU queue."""
        try:
            entry = self.entries[key]
            self.entries[CACHESIZE] -= self._get_size_of_entry(entry)
            self._unlink(key)
            del self.entries[key]
        except KeyError:
            pass

    def _unlink(self, key):
        try:
            entry = self.entries[key]
            if not entry.prev: #entry is list tail 
                self.entries[LISTTAIL] = entry.next
            else:
                previous_entry = self.entries[entry.prev]
                previous_entry.next = entry.next
                self._store_to_dict(previous_entry)
            if not entry.next: #entry is list head 
                self.entries[LISTHEAD] = entry.prev
            else:
                next_entry =  self.entries[entry.next]
                next_entry.prev = entry.prev
                self._store_to_dict(next_entry)
        except KeyError:
            pass
                
    def __repr__(self):
        ret = 'LRU key chain: '
        entry = self._get_listtail_entry()
        if not entry:
            return ret+"empty"
        while entry.next:
            ret += str(entry.key)+" -> "
            entry = self.entries[entry.next]
        ret += str(entry.key)
        return ret
    
    def flush(self, key):
        self.update(key) 
        self.set_dirty(key, False) 
        self._resize()
    
    def set_dirty(self, key, is_dirty):
        entry = self.entries[key]
        entry.dirty = is_dirty
        self.entries[key] = entry
        self._resize()

