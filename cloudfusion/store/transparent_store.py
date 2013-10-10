import time

class ExceptionStats(object):
    def __init__(self, name='', exception_list=None, desc='', count=1, lasttime=time.time()):
        self.name = name
        self.exception_list = exception_list if exception_list else []
        self.count = count
        self.lasttime = lasttime
        self.desc = desc
    
    def __repr__(self):
        return"%s occured %s time(s). Last occurrence: %d seconds ago\nDescription: %s\n" % (self.name, self.count, time.time()-self.lasttime, self.desc)

class TransparentStore(object):
    '''
    Interface to get statistics about a store.
    The unit MB is 1000000 Bytes. 
    '''
    def get_dirty_files(self):
        """Get a list of file paths to files that are not already synchronized with the store"""
        return []
    
    def get_downloaded(self):
        """Get amount of data downloaded from a store in MB"""
        return 0.0
    
    def get_uploaded(self):
        """Get amount of data uploaded to a store in MB"""
        return 0.0
    
    def get_download_rate(self):
        """Get download rate in MB/s"""
        return 0.0
    
    def get_upload_rate(self):
        """Get upload rate in MB/s"""
        return 0.0
    
    def get_cache_hits(self):
        """Get number of files that were accessed while they were cached"""
        return 0
    
    def get_cache_misses(self):
        """Get number of files that were accessed while they were not in cache"""
        return 0
    
    def get_exception_stats(self):
        """Get dict of exception statistics with exception names mapping to :class:`ExceptionStats`"""
        return {}