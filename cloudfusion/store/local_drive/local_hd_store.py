'''
Created on 08.04.2011

@author: joe
'''

from cloudfusion.store.store import *
import logging
from cloudfusion.util.exponential_retry import retry
from cloudfusion.mylogging import db_logging_thread
import shutil
from cloudfusion.util import file_util
import errno

class LocalHDStore(Store):
    '''Subclass of Store implementing an interface to the local file system.'''
    
    def __init__(self, config):
        super(LocalHDStore, self).__init__()
        self.name = 'harddrive'
        self._logging_handler = self.name
        self.logger = logging.getLogger(self._logging_handler)
        self.logger = db_logging_thread.make_logger_multiprocessingsave(self.logger)
        self.logger.info("creating %s store", self.name)
        if not 'root' in config or config['root']=='/':
            #prevent writing to actual file system root
            self.logger.error("Error: specify a root directory with root=/my_root_folder in the configuration ini file; root must be a subdirectory -using /tmp/harddriveroot instead")
            self.root = '/tmp/harddriveroot'
        else:    
            self.root = config['root']
        if not os.path.exists(self.root):
            os.makedirs(self.root)
        print "root:"+self.root
        self.logger.info("api initialized")
     
        
    def __deepcopy__(self, memo):
        from copy import deepcopy
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k == 'logger':
                setattr(result, k, self.logger)
            elif k == '_logging_handler':
                setattr(result, k, self._logging_handler)
            else:
                setattr(result, k, deepcopy(v, memo))
        return result
        
    def get_name(self):
        self.logger.debug("getting name")
        return self.name
    
    def get_file(self, path_to_file): 
        self.logger.info("getting file: %s", path_to_file)
        self._raise_error_if_invalid_path(path_to_file)
        with open(self.root+path_to_file) as f:
            return f.read()

    @retry((Exception), tries=1, delay=0) 
    def store_fileobject(self, fileobject, path, interrupt_event=None):
        size = file_util.get_file_size_in_bytes(fileobject)
        self.logger.info("Storing file object of size %s to %s", size, path)
        with open(self.root+path, 'w') as f:
            f.write(fileobject.read())
        return os.path.getmtime(self.root+path)
    
    # worst case: object still exists and takes up space or is appended to, by mistake
    # with caching_store, the entry in cache is deleted anyways 
    @retry((Exception), tries=5, delay=0) 
    def delete(self, path, is_dir=False): 
        self.logger.info("deleting %s", path)
        self._raise_error_if_invalid_path(path)
        try:
            if is_dir:
                shutil.rmtree(self.root+path)
            else:
                os.remove(self.root+path)
        except Exception, e:
            pass
        
    @retry((Exception))
    def account_info(self):
        self.logger.debug("retrieving account info")
        return "Local hard drive store in directory "+self.root

    @retry((Exception))
    def create_directory(self, directory):
        self.logger.info("creating directory %s", directory)
        try:
            os.mkdir(self.root+directory)
        except OSError, e:
            if e.errno == errno.EEXIST:
                raise AlreadyExistsError(directory+" already exists.")
            else:
                raise e
               
    def duplicate(self, path_to_src, path_to_dest):
        self.logger.info("duplicating %s to %s", path_to_src, path_to_dest)
        if not os.path.isdir(self.root+path_to_src):
            shutil.copyfile(self.root+path_to_src, self.root+path_to_dest)
        else:
            shutil.copytree(self.root+path_to_src, self.root+path_to_dest)
    
    def move(self, path_to_src, path_to_dest):
        self.logger.info("moving %s to %s", path_to_src, path_to_dest)
        if not os.path.isdir(self.root+path_to_src):
            shutil.copyfile(self.root+path_to_src, self.root+path_to_dest)
            self.delete(path_to_src)
        else:
            shutil.copytree(self.root+path_to_src, self.root+path_to_dest)
            self.delete(path_to_src, is_dir=True)
    
    def get_overall_space(self):
        self.logger.debug("retrieving all space")
        st = os.statvfs(self.root)
        ret = (st.f_blocks * st.f_frsize)
        return ret

    def get_used_space(self):
        self.logger.debug("retrieving used space")
        st = os.statvfs(self.root)
        ret = (st.f_blocks - st.f_bfree) * st.f_frsize
        return ret
        
    @retry((Exception))
    def get_directory_listing(self, directory):
        self.logger.debug("getting directory listing for %s", directory)
        self._raise_error_if_invalid_path(directory)
        ret = []
        for filename in os.listdir(self.root+directory):
            if directory != '/':
                ret.append(directory+'/'+filename)
            else:
                ret.append('/'+filename)
        return ret

    def _handle_error(self, error, stacktrace, method_name, remaining_tries, *args, **kwargs):
        """Used by retry decorator to react to errors."""
        if isinstance(error, OSError):
            self.logger.debug("OSError")
            self.logger.exception(error)
            self.logger.debug(str(error.errno == errno.ENOENT))
            self.logger.debug(str(error.errno))
            
            if error.errno == errno.ENOENT:
                raise NoSuchFilesytemObjectError("Error: No such file or directory occured in method %s with arguments %s %s"%(method_name, args, kwargs))
        elif isinstance(error, AlreadyExistsError):
            self.logger.error("Error could not be handled: \n%s", stacktrace)
            raise error
        if remaining_tries == 0: # throw error after last try
            raise StoreAccessError(str(error), 0) 
        return False
        
    @retry((Exception))
    def get_metadata(self, path):
        self.logger.debug("getting metadata for %s", path)
        self._raise_error_if_invalid_path(path)
        ret = {}
        ret["bytes"] = os.path.getsize(self.root+path)
        ret["modified"] = os.path.getmtime(self.root+path)
        ret["path"] = path
        ret["is_dir"] = os.path.isdir(self.root+path) 
        return ret
        
    def _get_time_difference(self):
        self.logger.debug("getting time difference")
        return 0
    
    def get_logging_handler(self):
        return self._logging_handler
    
    def get_max_filesize(self):
        """Return maximum number of bytes per file"""
        return 1000*1000*1000*1000