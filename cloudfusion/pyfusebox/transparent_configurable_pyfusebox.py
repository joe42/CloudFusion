'''
Created on Oct 7, 2013

@author: joe
'''
from cloudfusion.pyfusebox.configurable_pyfusebox import *
from cloudfusion.pyfusebox.virtualfile import VirtualFile
from cloudfusion.store.transparent_store import TransparentStore, ExceptionStats
from cloudfusion.store.transparent_caching_store import TransparentMultiprocessingCachingStore
import os.path

class TransparentConfigurablePyFuseBox(ConfigurablePyFuseBox):
    '''
    '''
    STATS_DIR = '/stats'
    VIRTUAL_DIRTY_FILELIST_FILE = STATS_DIR+'/notuploaded'
    VIRTUAL_ERRORS_FILE = STATS_DIR+'/errors'
    VIRTUAL_STATISTICS_FILE = STATS_DIR+'/stats'
    DATA_FOLDER_PATH = "/data"
    XATTR_IS_DIRTY = 'XATTR_IS_DIRTY'
    
    def __init__(self, root):
        super( TransparentConfigurablePyFuseBox, self ).__init__(root)
        self.virtual_files = {}
        self.virtual_files[self.VIRTUAL_DIRTY_FILELIST_FILE] = VirtualFile(self.VIRTUAL_DIRTY_FILELIST_FILE)
        self.virtual_files[self.VIRTUAL_ERRORS_FILE] = VirtualFile(self.VIRTUAL_ERRORS_FILE)
        self.virtual_files[self.VIRTUAL_STATISTICS_FILE] = VirtualFile(self.VIRTUAL_STATISTICS_FILE)
        self.store_initialized = False
        self.exceptions_log = {}
    
    def getxattr(self, path, name, position=0):
        if name == self.XATTR_IS_DIRTY:
            if isinstance(self.store, TransparentStore):
                return repr(self.remove_data_folder_prefix(path) in self.store.get_dirty_files())
        return '' # Should return ENOATTR

    def listxattr(self, path):
        if isinstance(self.store, TransparentStore):
            if self.remove_data_folder_prefix(path) in self.store.get_dirty_files():
                return [self.XATTR_IS_DIRTY]
        return []
        
    def getattr(self, path, fh=None):
        if self.STATS_DIR == path:
            return self._getattr_for_folder_with_full_access()
        if path in self.virtual_files.keys():
            return self.virtual_files[path].getattr()
        return super( TransparentConfigurablePyFuseBox, self ).getattr(path, fh)
    
    def truncate(self, path, length, fh=None):
        if path in self.virtual_files.keys():
            return self.virtual_files[path].truncate()
        return super( TransparentConfigurablePyFuseBox, self ).truncate(path, length, fh)

    def read(self, path, size, offset, fh):
        if path in self.virtual_files.keys():
            self._update_vtf(path)
            return self.virtual_files[path].read(size, offset)
        return super( TransparentConfigurablePyFuseBox, self ).read(path, size, offset, fh)

    def _update_vtf(self, path):
        if path == self.VIRTUAL_DIRTY_FILELIST_FILE:
            if isinstance(self.store, TransparentStore):
                self.virtual_files[path].truncate()
                for dirty_file in self.store.get_dirty_files():
                    self.virtual_files[path].text += dirty_file + "\n"
        if path == self.VIRTUAL_ERRORS_FILE:
            if isinstance(self.store, TransparentStore) or self.store == None: #None to show errors on inizialization 
                self.virtual_files[path].truncate()
                self.virtual_files[path].text = self._get_exception_stats()
        if path == self.VIRTUAL_STATISTICS_FILE:
            if isinstance(self.store, TransparentStore):
                self.virtual_files[path].truncate()
                self.virtual_files[path].text += "Aborted downloads are not considered. \n"
                self.virtual_files[path].text += "If the store is updated from outside, reading this file if it is in the cache, is still counted as hit.\n"
                self.virtual_files[path].text += "%s MBs download rate\n" % self.store.get_download_rate()
                self.virtual_files[path].text += "%s MBs upload rate\n" % self.store.get_upload_rate()
                self.virtual_files[path].text += "%s MB uploaded\n" % self.store.get_uploaded()
                self.virtual_files[path].text += "%s MB downloaded\n\n" % self.store.get_downloaded()
                self.virtual_files[path].text += "%s cache misses\n" % self.store.get_cache_misses()
                self.virtual_files[path].text += "%s cache hits\n\n" % self.store.get_cache_hits()
                self.virtual_files[path].text += "%s MB of cached data\n\n" % self.store.get_cachesize()
                if self.store.get_status_information() != '':
                    self.virtual_files[path].text += "Status:\n%s" % self.store.get_status_information()
                
    def _initialize_store(self):
        try:
            super( TransparentConfigurablePyFuseBox, self )._initialize_store()
        except Exception, e:
            self._log_exception(e)
            raise e
    
    def _get_exception_stats(self):
        ret = ''
        for e_stat in self.exceptions_log.values():
            ret += str(e_stat)
        if self.store == None:
            return ret
        for e_stat in self.store.get_exception_stats().values():
            ret += str(e_stat)
        return ret
    
    def _log_exception(self, exception):
        name = repr(exception)
        if self.exceptions_log.has_key(name):
            e_stat = self.exceptions_log[name]
            e_stat.exception_list.append(exception)
            e_stat.lasttime = time.time()
            e_stat.count += 1
        else:
            e_stat = ExceptionStats(name, [exception], str(exception))
            self.exceptions_log[name] = e_stat
    
    def write(self, path, buf, offset, fh):
        if path in self.virtual_files.keys():
            raise FuseOSError(EACCES) 
        return super( TransparentConfigurablePyFuseBox, self ).write(path, buf, offset, fh)
    
    def release(self, path, fh):
        if path in self.virtual_files.keys():
            return 0
        return super( TransparentConfigurablePyFuseBox, self ).release(path, fh) 
       
    def readdir(self, path, fh):
        ret = super( TransparentConfigurablePyFuseBox, self ).readdir(path, fh) 
        if path == self.STATS_DIR: #add virtual files
            for path in self.virtual_files.keys():
                ret.append(self.virtual_files[path].get_name())
        if path == "/":# add stats folder
            ret.append(os.path.basename(self.STATS_DIR))
        return ret
    