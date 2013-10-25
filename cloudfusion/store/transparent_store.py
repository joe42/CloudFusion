import time

class ExceptionStats(object):
    def __init__(self, name='', exception_list=None, desc='', count=1, lasttime=time.time()):
        '''Statistics of a certain exception.
        :param name: the identifier of the exception 
        :param exception_list: the list of the actual exception instances
        :param desc: the description of the exception (defaults to str(exception) )
        :param count: number of occurences of the exception (defaults to 1)
        :param lasttime: the last time the exception occured in seconds from the epoche (defaults to current time)'''
        self.name = name
        self.exception_list = exception_list if exception_list else []
        self.count = count
        self.lasttime = lasttime
        self.desc = desc
    
    @staticmethod
    def add_exception(exception, exceptions_log, name=None, desc=None, count=-1, lasttime=None):
        '''Add an ExceptionStats object to the list *exception_log* or update time of occurence, exception_list, 
        and count if there is a similar exception in the log.
        :param exception: the exception to add to the log
        :param exceptions_log: an existing dictionary of exceptions mapping their name to an ExceptionStats instance (might be empty)
        :param name: the identifier of the exception (defaults to type(exception) or repr(exception) )
        :param desc: the description of the exception (defaults to str(exception) )
        :param count: number of occurences of the exception (defaults to last count plus 1 or to 1 if if no exception with the same identifier exists)
        :param lasttime: the last time the exception occured in seconds from the epoche (defaults to current time)
        :returns: the updated exception log'''
        if not name:
            name = type(exception)
            if name == 'Exception':
                name = repr(exception)
        if exceptions_log.has_key(name):
            e_stat = exceptions_log[name]
            e_stat.exception_list.append(exception)
            e_stat.lasttime = time.time()
            if count == -1:
                e_stat.count += 1
            else:
                e_stat.count = count
        else:
            desc = desc if desc else str(exception)
            if count == -1:
                count = 1
            e_stat = ExceptionStats(name, [exception], desc, count)
            exceptions_log[name] = e_stat
        return exceptions_log
    
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
    
    def get_status_information(self):
        '''Get arbitrary string describing status of the store'''
        return ''