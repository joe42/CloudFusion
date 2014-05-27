from cloudfusion.store.bulk_get_metadata import BulkGetMetadata
from cloudfusion.store.webdav.webdav_store import WebdavStore
from cloudfusion.util.exponential_retry import retry


class BulkGetMetadataWebdavStore(WebdavStore, BulkGetMetadata):
    '''Subclass of GoogleStore, extending it by implementing the BulkGetMetadata interface for quick directory lising.'''
    
    def __init__(self, config):
        super(BulkGetMetadataWebdavStore, self).__init__(config)
    
    @retry(Exception)
    def get_bulk_metadata(self, directory):
        """:returns: A dictionary mapping the path of every file object in *directory* to a dictionary with the keys\
        'modified', 'bytes' and 'is_dir' containing the corresponding metadata for the file object.
                
        The value for 'modified' is a date in seconds, stating when the file object was last modified.  
        The value for 'bytes' is the number of bytes of the file object. It is 0 if the object is a directory.
        The value for 'is_dir' is True if the file object is a directory and False otherwise.
        
        :raises: NoSuchFilesytemObjectError if the directory does not exist"""
        
        return self.tinyclient.get_bulk_metadata(directory)