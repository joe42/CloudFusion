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
class RetrieveMetadataError(StoreAccessError): 
    def __init__(self, path, msg="", status=0):
        super(RetrieveMetadataError, self).__init__("Could not retrieve metadata for "+path+"\nDescription: "+msg, status)
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
    def _is_valid_path(self, path):
        return path[0] == "/";
    
    def _raise_error_if_invalid_path(self, path):
        if not self._is_valid_path(path):
            raise InvalidPathValueError(path)
        
    def get_name(self):
        raise NotImplementedError()
    
    def get_file(self, path_to_file):
        raise NotImplementedError()
    
    def store_file(self, path_to_file, dest_dir="/", remote_file_name = None, interrupt_event=None):
        """Store the local file *path_to_file* to directory *dest_dir* on the store.
        :param remote_file_name: the file name on the store or the original file name if this parameter is None.
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
        :param remote_file_name: the file name on the store or the original file name if this parameter is None.
        :param interrupt_event: (optional) If the value is not None, listen for an interrupt event with with interrupt_event.wait() \
        until the file has been stored. Abort the upload if interrupt_event.wait() returns.
        :returns: (optional) the date in seconds, when the file was updated"""
        raise NotImplementedError()
            
    def delete(self, path):
        raise NotImplementedError()
          
    def account_info(self):
        """:returns: a human readable string describing account info like provider, name, statistics"""
        raise NotImplementedError()
    
    def get_free_space(self):
        """:returns: free space in bytes"""
        return self.get_overall_space()-self.get_used_space()
    
    def get_overall_space(self):
        """:returns: overal space in bytes"""
        raise NotImplementedError()
    
    def get_used_space(self):
        """:returns: space used by files in bytes"""
        raise NotImplementedError()

    def create_directory(self, directory):
        raise NotImplementedError()
        
    def duplicate(self, path_to_src, path_to_dest):
        """Duplicate file or directory from *path_to_src* to *directory path_to_dest*.
        If *path_to_dest* exists, it is overwritten by the file or directory specified by *path_to_src*
        If *path_to_src* is a directory, the directory is duplicated with all its files and directories.
        Either this method or :meth:`~.move` needs to be implemented in a subclass to work with :class:`cloudfusion.pyfusebox.pyfusebox.PyFuseBox`.
        :param path_to_src: must never be the same as *path_to_dest*
        :param path_to_dest: must end in the name of the child directory or the file specified by *path_to_src*"""
        raise NotImplementedError()
        
    def move(self, path_to_src, path_to_dest):
        """Move file or directory from *path_to_src* to *directory path_to_dest*.
        If *path_to_dest* exists, it is overwritten by the file or directory specified by *path_to_src*
        If *path_to_src* is a directory, the directory is moved with all its files and directories.
        Default implementation relies on an implementation of :meth:`~.duplicate` in a subclass, but it should be overwritten.
        Either this method or :meth:`~.duplicate` needs to be implemented in a subclass to work with :class:`cloudfusion.pyfusebox.pyfusebox.PyFuseBox`.
        :param path_to_src: must never be the same as *path_to_dest*
        :param path_to_dest: must end in the name of the child directory or the file specified by *path_to_src*"""
        self.duplicate(path_to_src, path_to_dest)
        self.delete(path_to_src)
 
    def get_modified(self, path):
        resp = self._get_metadata(path)
        return resp['modified']  
    
    def get_directory_listing(self, directory):
        raise NotImplementedError()
    
    def get_bytes(self, path):
        resp = self._get_metadata(path)
        return resp['bytes']
    
    def exists(self, path):
        try:
            self._get_metadata(path)
            return True
        except NoSuchFilesytemObjectError:
            return False;
    
    def _get_metadata(self, path):
        """ This method is a hook that can be implemented by subclasses. 
        If it is implemented, the methods :meth:`~.exists`, :meth:`~.get_bytes`, :meth:`~.is_dir` work out of the box.
        
        :returns: A dictionary with the keys 'modified', 'bytes' and 'is_dir' containing the corresponding metadata for *path*  
        The value for 'modified' is a date in seconds, stating when the object corresponding to *path* was last modified.  
        The value for 'bytes' is the number of bytes of the object corresponding to *path*. It is 0 if the object is a directory.
        The value for 'is_dir' is True if the object is a directory and False otherwise.
        
        :raises: NoSuchFilesytemObjectError if the object does not exist
        :raises: NotImplementedError if the method is not implemented
        """
        raise NotImplementedError()

    def is_dir(self, path):
        resp = self._get_metadata(path)
        return resp['is_dir']
    
    def get_logging_handler(self):
        raise NotImplementedError()
    
    def flush(self):
        pass
    
    def reconnect(self):
        pass
    
    def get_max_filesize(self):
        """Return maximum number of bytes per file"""
        return 1000*1000*1000
