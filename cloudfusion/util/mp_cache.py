from cloudfusion.util.cache import *
import time
from multiprocessing import Manager

class MPCache(Cache):
    '''
    Multiprocessing cache that can synchronize entries over multiple processes. 
    '''

    def __init__(self, expiration_time=60):
        """Return an MPCache instance.
        
        :param expiration_time: Time in seconds until entries are expired.
        """
        super( MPCache, self ).__init__(expiration_time)
        manager = Manager()
        self.entries = manager.dict()
        
    def get_size_of_dirty_data(self):
        ret = 0
        for key in self.entries.keys():
            if self.is_dirty(key):
                ret+= self._get_size_of_entry(self.entries[key])
        return ret
    
    def get_size_of_cached_data(self):
        ret = 0
        for key in self.entries.keys():
            ret+= self._get_size_of_entry(self.entries[key])
        return ret