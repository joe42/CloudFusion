'''
Created on Feb 5, 2013
'''
from cloudfusion.util.cache import *
import time

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

class LRUCache(Cache):
    '''
    Least Recently Used cache. When adding an entry to the cache exceeds its size limit,
    the cache discards dirty entries starting with the least recently used.
    However, dirty entries or the last entry in the cache are not deleted. 
    '''


    def __init__(self, expiration_time=60, maxsize_in_MB=200):
        """Return an LRUCache instance.
        
        :param expiration_time: Time in seconds until entries are expired.
        :param maxsize_in_MB: Approximate limit of the cache in MB.
        """
        super( LRUCache, self ).__init__(expiration_time)
        #self.entries = shelve.open(filename)
        #self.list_head = self.entries[LISTHEAD] if LISTHEAD in self.entries else None
        #self.entries[LISTTAIL] = self.entries[LISTTAIL] if LISTTAIL in self.entries else None
        self.maxsize = maxsize_in_MB
        self.entries = {}
        self.entries[LISTHEAD] = None
        self.entries[LISTTAIL] = None
        self.entries[CACHESIZE] = 0
        
    
    def get_keys(self):
        keys = super( LRUCache, self ).get_keys()
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
        
    def _get_listtail_entry(self):
        return self.entries[self.entries[LISTTAIL]] if self.entries[LISTTAIL] else None
    
    def _get_listhead_entry(self):
        return self.entries[self.entries[LISTHEAD]] if self.entries[LISTHEAD] else None

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

    def get_size_of_dirty_data(self):
        ret = 0
        for key in self.entries:
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
        entry = self._get_listtail_entry()
        while self.entries[CACHESIZE]/1000000 >= self.maxsize and entry.next:
            if entry.dirty == False:
        	       self.delete(entry.key)
            entry = self.entries[entry.next]
    
    def delete(self, key):
        """Remove current entry associated with key from the LRU queue."""
        if key in self.entries:
            entry = self.entries[key]
            self.entries[CACHESIZE] -= self._get_size_of_entry(entry)
            self._unlink(key)
            del self.entries[key]

    def _unlink(self, key):
        if key in self.entries:
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
