'''
Created on 08.04.2011
'''
import os.path

class StoreAccessError(Exception):
    def __init__(self, msg, status=0):
        if status != 0:
            msg =msg+"\nStatus: %s" %status
        super(StoreAccessError, self).__init__(msg)
        self.status = status
class DateParseError(Exception):
    def __init__(self, msg):
        super(DateParseError, self).__init__(msg)
class StoreSpaceLimitError(StoreAccessError):
    def __init__(self, msg="", status=0):
        super(StoreSpaceLimitError, self).__init__(msg, status)
class NoSuchFilesytemObjectError(StoreAccessError):
    def __init__(self, path, status=0):
        super(NoSuchFilesytemObjectError, self).__init__("%s does not exist." % path, status)
class StoreAutorizationError(StoreAccessError):
    def __init__(self, msg, status=0):
        super(StoreAutorizationError, self).__init__(msg, status)
class AlreadyExistsError(StoreAccessError):
    def __init__(self, msg, status=0):
        super(AlreadyExistsError, self).__init__(msg, status)
class InvalidPathValueError(ValueError):
    def __init__(self, path):
        super(InvalidPathValueError, self).__init__(path+" "+"is no valid path!!") 
class InterruptedException(Exception):
    def __init__(self, msg):
        super(InterruptedException, self).__init__(msg)

    
class Store(object):
    '''Central interface for any cloud storage provider. 
    Any cloud storage provider that is used by CloudFusion needs to implement this interface.
    After implementing the interface for a new provider, you can add file system access to it by
    introducing a new branch to the if statement in :meth:`cloudfusion.pyfusebox.configurable_pyfusebox.ConfigurablePyFuseBox.__get_new_store`.
    Advanced functionality such as caching and concurrency are supplied by wrappers, which are already implemented.
    Path parameters are always absolute paths of a file system, starting with a '/'
    '''
    def _is_valid_path(self, path):
        return path[0] == "/";
    
    def _raise_error_if_invalid_path(self, path):
        if not self._is_valid_path(path):
            raise InvalidPathValueError(path)
        
    def get_name(self):
        ''':returns: the name of the service; i.e. Amazon S3, or Dropbox'''
        raise NotImplementedError()
    
    def get_file(self, path_to_file):
        ''':returns: the data of the remote file at *path_to_file* as a string
        :raises: NoSuchFilesytemObjectError if the object does not exist'''
        raise NotImplementedError()
    
    def store_file(self, path_to_file, dest_dir="/", remote_file_name = None, interrupt_event=None):
        """Store the local file *path_to_file* to directory *dest_dir* on the store.
        :param path_to_file: local file path
        :param dest_dir: remote destination directory to store the contents of the local file to
        :param remote_file_name: the file name on the store; by default this is the original file name if this parameter is None.
        :param interrupt_event: (optional) If the value is not None, listen for an interrupt event with with interrupt_event.wait() \
        until the file has been stored. Abort the upload if interrupt_event.wait() returns.
        :returns: (optional) the date in seconds, when the file was updated"""
        if dest_dir == "/":
            dest_dir = ""
        fileobject = open(path_to_file)
        if not remote_file_name:
            remote_file_name = os.path.basename(path_to_file)
        return self.store_fileobject(fileobject, dest_dir + "/" + remote_file_name)
        
    def store_fileobject(self, fileobject, path, interrupt_event=None):
        """Store the contents of *fileobject* to *path* on the store.
        :param fileobject: A file like object. The position of the fileobject needs to be at 0 (use fileobject.seek(0) before calling this method)
        :param path: The remote file path to store the contents of fileobject to
        :param interrupt_event: (optional) If the value is not None, listen for an interrupt event with with interrupt_event.wait() \
        until the file has been stored. Abort the upload if interrupt_event.wait() returns.
        :returns: (optional) the date in seconds, when the file was updated"""
        raise NotImplementedError()
            
    def delete(self, path, is_dir):
        '''Delete file or directory tree at path.
        Delete does not raise a NoSuchFilesytemObjectError exception.
        :param path: path to the file or directory to delete
        :param is_dir: True iff path points to a directory'''
        raise NotImplementedError()
          
    def account_info(self):
        """:returns: a human readable string describing account info like provider, name, statistics"""
        raise NotImplementedError()
    
    def get_free_space(self):
        """:returns: free space in bytes"""
        return self.get_overall_space()-self.get_used_space()
    
    def get_overall_space(self):
        """:returns: overall space in bytes"""
        raise NotImplementedError()
    
    def get_used_space(self):
        """:returns: space used by files in bytes"""
        raise NotImplementedError()

    def create_directory(self, directory):
        '''Create the remote directory *directory*
        :param directory: the absolute path name of the directory to create
        :raises: AlreadyExistsError if the directory does already exist:
        '''
        raise NotImplementedError()
        
    def duplicate(self, path_to_src, path_to_dest):
        """Duplicate file or directory from *path_to_src* to *path_to_dest*.
        If *path_to_dest* exists before, it is deleted or overwritten.
        If *path_to_src* is a directory, the directory is duplicated with all its files and directories.
        Either this method or :meth:`~.move` needs to be implemented in a subclass.
        :param path_to_src: must never be the same as *path_to_dest*
        :param path_to_dest: must end in the name of the child directory or the file specified by *path_to_src*"""
        raise NotImplementedError()
        
    def move(self, path_to_src, path_to_dest):
        """Rename a remote file or directory *path_to_src* to *path_to_dest*. 
        If *path_to_dest* exists before, it is deleted or overwritten.
        If *path_to_src* is a directory, the directory is renamed to *path_to_dest*.
        Default implementation relies on an implementation of :meth:`~.duplicate` in a subclass, but it should be overwritten.
        Either this method or :meth:`~.duplicate` needs to be implemented in a subclass.
        :param path_to_src: path to a remote file or directory
        :param path_to_dest: path of the new remote file or directory"""
        self.duplicate(path_to_src, path_to_dest)
        try:
            self.delete(path_to_src, is_dir=True)
        except Exception, e:
            try:
                self.delete(path_to_src, is_dir=False)
            except Exception, e:
                pass
 
    def get_modified(self, path):
        ''':returns: the time *path* was modified in seconds from the epoche'''
        resp = self.get_metadata(path)
        return resp['modified']  
    
    def get_directory_listing(self, directory):
        ''':returns: list of absolute file paths of files in *directory*'''
        raise NotImplementedError()
    
    def get_bytes(self, path):
        ''':returns: the number of bytes of the file at *path*, or 0 if *path* is a directory'''
        resp = self.get_metadata(path)
        return resp['bytes']
    
    def exists(self, path):
        ''':returns: True if a remote file or directory exists at *path*, and False otherwise'''
        try:
            self.get_metadata(path)
            return True
        except NoSuchFilesytemObjectError:
            return False;
    
    def __deepcopy__(self, memo):
        '''This method is needed for copying the Store instance to other processes.
        Overwrite this method to add a property that cannot be serialized or should be shared by multiple processes.
        The property can be added by creating a new branch in the if statement, comparing k with the property name.'''
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
    
    def get_metadata(self, path):
        """ This method is a hook that must be implemented by subclasses. 
        If it is implemented, the methods :meth:`~.exists`, :meth:`~.get_bytes`, :meth:`~.is_dir` work out of the box.
        
        :returns: A dictionary with the keys 'modified', 'bytes' and 'is_dir' containing the corresponding metadata for *path*
          
        The value for 'modified' is a date in seconds, stating when the object corresponding to *path* was last modified.  
        The value for 'bytes' is the number of bytes of the object corresponding to *path*. It is 0 if the object is a directory.
        The value for 'is_dir' is True if the object is a directory and False otherwise.
        
        :raises: NoSuchFilesytemObjectError if the object does not exist
        """
        raise NotImplementedError()

    def is_dir(self, path):
        ''':returns: True if *path* is a remote file, and False if it is a remote directory
        :raises: NoSuchFilesytemObjectError if the remote object does not exist'''
        resp = self.get_metadata(path)
        return resp['is_dir']
    
    def get_logging_handler(self):
        '''Get the name of the logging handler used by a subclass, so that the wrappers may use the same logger.
        Wrappers are responsible for extended functionality like caching data or concurrency i.e. :class:`cloudfusion.store.transparent_caching_store.TransparentMultiprocessingCachingStore`.
        This method might simply return :meth:`~.get_name`, even if the subclass does not use a logger.
        :return: the name of the logging handler used by a subclass and its wrappers.'''
        raise NotImplementedError()
    
    def reconnect(self):
        '''Try to reconnect to the service.'''
        pass
    
    def set_configuration(self, config):
        '''Set configuration options during runtime.
        The method is normally called by :class:`cloudfusion.pyfusebox.configurable_pyfusebox.ConfigurablePyFuseBox`,
        when the user changes CloudFusion's configuration file in /config/config. 
        :param config: a dictionary with configuration options'''
        pass
    
    def get_configuration(self, config):
        '''Get configuration options during runtime.
        The method is normally called by :class:`cloudfusion.pyfusebox.configurable_pyfusebox.ConfigurablePyFuseBox`,
        when the user reads CloudFusion's configuration file in /config/config.
        It can return a dictionary with variables to display in /config/config. 
        :returns: a dictionary with variable names and corresponsing values'''
        return {}
    
    def get_max_filesize(self):
        """Return maximum number of bytes per file; Some cloud storages limit the size of files to be uploaded."""
        return 1000*1000*1000
