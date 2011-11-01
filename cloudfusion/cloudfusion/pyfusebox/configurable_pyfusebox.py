'''
Created on 23.08.2011

@author: joe

This FUSE module initializes the store at runtime when the user accesses the virtual file /.config/.###config### and writes the appropriate parameters to the file.
'''

from cloudfusion.pyfusebox.pyfusebox import *
from cloudfusion.pyfusebox.flushing_pyfusebox import FlushingPyFuseBox
from cloudfusion.pyfusebox.virtualconfigfile import VirtualConfigFile
from cloudfusion.store.dropbox.dropbox_store import DropboxStore
from cloudfusion.store.sugarsync.sugarsync_store import SugarsyncStore
from cloudfusion.store.caching_store import CachingStore
from cloudfusion.store.metadata_caching_store import MetadataCachingStore


class ConfigurablePyFuseBox(FlushingPyFuseBox):
    VIRTUAL_CONFIG_FILE = '/config/config'
    DATA_FOLDER_PATH = "/data"
    def __init__(self, root):
        self.virtual_file = VirtualConfigFile(self.VIRTUAL_CONFIG_FILE)
        self.store_initialized = False
        self.logger = logging.getLogger('pyfusebox')
        self.logger.debug("initialized configurable pyfusebox")
        super( ConfigurablePyFuseBox, self ).__init__(root, None)
    
    def _getattr_for_folder_with_full_access(self):
        st = zstat()
        st['st_mode'] = 0777 | stat.S_IFDIR
        st['st_nlink']=2
        st['st_size'] = 4096
        st['st_blocks'] = (int) ((st['st_size'] + 4095L) / 4096L);
        return st;
    
    def getattr(self, path, fh=None):
        self.logger.debug("getattr "+path+"")
        if path == "/": 
            return self._getattr_for_folder_with_full_access();
        if path == self.virtual_file.get_path():
            return self.virtual_file.getattr()
        if self.virtual_file.get_subdir(path):
            return self._getattr_for_folder_with_full_access()
        if self.store_initialized and path.startswith(self.DATA_FOLDER_PATH):
            path = self.remove_data_folder_prefix(path)
            return super( ConfigurablePyFuseBox, self ).getattr(path, fh)
        if path == self.DATA_FOLDER_PATH: 
            return self._getattr_for_folder_with_full_access();
        raise FuseOSError(ENOENT)
    
    def truncate(self, path, length, fh=None):
        self.logger.debug("truncate %s to %s" % (path, length))
        if path == self.virtual_file.get_path():
            self.virtual_file.truncate()
            return 0
        if self.store_initialized and path.startswith(self.DATA_FOLDER_PATH):
            path = self.remove_data_folder_prefix(path)
            return super( ConfigurablePyFuseBox, self ).truncate(path, length, fh)
        raise FuseOSError(ENOENT)
    
    def rmdir(self, path):
        self.logger.debug("rmdir %s" % (path))
        if  path == self.DATA_FOLDER_PATH or path == self.virtual_file.get_dir():
            raise FuseOSError(EACCES)             
        if self.store_initialized and path.startswith(self.DATA_FOLDER_PATH):
            path = self.remove_data_folder_prefix(path)
            return super( ConfigurablePyFuseBox, self ).rmdir(path)
        raise FuseOSError(ENOENT)
        
    def mkdir(self, path, mode):
        self.logger.debug("mkdir %s with mode: %s" % (path, str(mode)))
        if path == self.DATA_FOLDER_PATH: 
            raise FuseOSError(EEXIST) 
        if self.store_initialized and path.startswith(self.DATA_FOLDER_PATH):
            path = self.remove_data_folder_prefix(path)
            return super( ConfigurablePyFuseBox, self ).mkdir(path, mode)
        raise FuseOSError(EACCES) 
            
    def remove_data_folder_prefix(self, path):
        path = path[len(self.DATA_FOLDER_PATH):]
        if not path.startswith("/"):
            path = "/"
        return path
    
    def statfs(self, path):#add size of vtf
        self.logger.debug("statfs %s" % (path))
        if self.store_initialized:
            path = self.remove_data_folder_prefix(path)
            return super( ConfigurablePyFuseBox, self ).statfs(path)
    
    def rename(self, old, new):
        self.logger.debug("rename %s to %s" % (old, new))
        """if new == self.virtual_file.get_path():
            buf = 
            written_bytes =  self.virtual_file.write(buf, offset)
            if written_bytes >0: # configuration changed
                self._initialize_store()
            return written_bytes"""
        if old == self.DATA_FOLDER_PATH: 
            raise FuseOSError(EACCES) 
        if new == self.DATA_FOLDER_PATH: 
            raise FuseOSError(EACCES) 
        if self.store_initialized and old.startswith(self.DATA_FOLDER_PATH)  and new.startswith(self.DATA_FOLDER_PATH):
            old = self.remove_data_folder_prefix(old)
            new = self.remove_data_folder_prefix(new)
            return super( ConfigurablePyFuseBox, self ).rename(old, new)
        raise FuseOSError(EACCES) 

    def create(self, path, mode):
        self.logger.debug("create %s with mode %s" % (path, str(mode)))
        if path == self.DATA_FOLDER_PATH : 
            raise FuseOSError(EACCES) 
        if self.store_initialized and path.startswith(self.DATA_FOLDER_PATH):
            path = self.remove_data_folder_prefix(path)
            return super( ConfigurablePyFuseBox, self ).create(path, mode)
        raise FuseOSError(EACCES) 
    
    def unlink(self, path):
        if self.store_initialized and path.startswith(self.DATA_FOLDER_PATH):
            path = self.remove_data_folder_prefix(path)
            super( ConfigurablePyFuseBox, self ).unlink(path)

    def read(self, path, size, offset, fh):
        self.logger.debug("read %s" % path)
        if path == self.virtual_file.get_path():
            return self.virtual_file.read(size, offset)
        if self.store_initialized and path.startswith(self.DATA_FOLDER_PATH):
            path = self.remove_data_folder_prefix(path)
            return super( ConfigurablePyFuseBox, self ).read(path, size, offset, fh)

    def _initialize_store(self):
        self.logger.debug("_initialize_store:")
        conf = self.virtual_file.get_store_config_data()
        service = conf['name']
        self.logger.debug("got service name")
        cache_time = 0
        metadata_cache_time = 0
        if 'cache' in conf:
            cache_time = int(conf['cache']) 
        if 'metadata_cache' in conf:
            metadata_cache_time = int(conf['metadata_cache']) 
        self.logger.debug("got cache parameter")
        auth = self.virtual_file.get_service_auth_data()
        self.logger.debug("got auth data: "+str(auth))
        store = self.__get_new_store(service, auth) #catch error?
        self.logger.debug("initialized store")
        if cache_time > 0 and metadata_cache_time > 0:
            store = MetadataCachingStore( CachingStore( MetadataCachingStore( store, metadata_cache_time ), cache_time ), metadata_cache_time )
            self.set_cache_expiration_time(cache_time)
        elif cache_time > 0:
            store = CachingStore(store, cache_time)
            self.set_cache_expiration_time(cache_time)
        elif metadata_cache_time > 0:
            store = MetadataCachingStore( store, metadata_cache_time )
        self.store = store
        super( ConfigurablePyFuseBox, self ).start_cyclic_flushing()
        self.logger.debug("initialized service")
        self.store_initialized = True
        
    def __get_new_store(self, service, auth):
        self.logger.debug("__get_new_store:")
        if service.lower() == "sugarsync":
            store = SugarsyncStore(auth)
        else: # default
            self.logger.debug("get dropbox store")
            try:
                store = DropboxStore(auth)
            except Exception as (e,msg):
                self.logger.debug(str(e)+" -- "+msg)
            self.logger.debug("got dropbox store")
        return store
    
    def write(self, path, buf, offset, fh):
        self.logger.debug("write %s ... starting with %s at %s - fh: %s" % (path, buf[0:10], offset, fh))
        if path == self.virtual_file.get_path():
            self.logger.debug("writing to virtual file "+path)
            written_bytes = self.virtual_file.write(buf, offset)
            self.logger.debug("written bytes:"+str(written_bytes))
            if written_bytes >0: # configuration changed
                self._initialize_store()
            return written_bytes
        if self.store_initialized and path.startswith(self.DATA_FOLDER_PATH):
            path = self.remove_data_folder_prefix(path)
            return super( ConfigurablePyFuseBox, self ).write(path, buf, offset, fh)
        return 0
    
    def flush(self, path, fh):
        self.logger.debug("flush %s - fh: %s" % (path, fh))
        if path == self.virtual_file.get_path():
            return 0
        if self.store_initialized and path.startswith(self.DATA_FOLDER_PATH):
            path = self.remove_data_folder_prefix(path)
            return super( ConfigurablePyFuseBox, self ).flush(path, fh)
    
    def release(self, path, fh):
        self.logger.debug("release %s - fh: %s" % (path, fh))
        if path == self.virtual_file.get_path():
            return 0
        if self.store_initialized and path.startswith(self.DATA_FOLDER_PATH):
            path = self.remove_data_folder_prefix(path)
            return super( ConfigurablePyFuseBox, self ).release(path, fh) 
       
    def readdir(self, path, fh):
        self.logger.debug("readdir "+path+"")
        directories = []
        if path == self.virtual_file.get_dir(): #add virtual file
            directories.append(self.virtual_file.get_name())
        if path == "/":# add virtual folders
            directories.append(self.virtual_file.get_dir())
            directories.append(os.path.basename(self.DATA_FOLDER_PATH))
        elif self.store_initialized and path.startswith(self.DATA_FOLDER_PATH):
            path = self.remove_data_folder_prefix(path)
            directories += self.store.get_directory_listing(path)
        #self.logger.debug("readdir -> "+str(directories)+"")
        file_objects = [".", ".."]
        for file_object in directories:
            if file_object != "/":
                file_object = os.path.basename(file_object.encode('utf8'))
                file_objects.append( file_object )
        return file_objects;

        
#TODO:
"""
    def chmod(self, path, mode):
        raise FuseOSError(EROFS)
    
    def chown(self, path, uid, gid):
        raise FuseOSError(EROFS)
        """
        