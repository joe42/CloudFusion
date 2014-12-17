from cloudfusion.store.bulk_get_metadata import BulkGetMetadata
import time
import cloudfusion.third_party.parsedatetime.parsedatetime as pdt
from cloudfusion.store.gs.google_store import GoogleStore
from cloudfusion.util.exponential_retry import retry
from cloudfusion.util.string import to_str


class BulkGetMetadataGoogleStore(GoogleStore, BulkGetMetadata):
    '''Subclass of GoogleStore, extending it by implementing the BulkGetMetadata interface for quick directory lising.'''
    
    def __init__(self, config):
        super(BulkGetMetadataGoogleStore, self).__init__(config)
    
    @retry(Exception)
    def get_bulk_metadata(self, directory):
        """:returns: A dictionary mapping the path of every file object in *directory* to a dictionary with the keys\
        'modified', 'bytes' and 'is_dir' containing the corresponding metadata for the file object.
        
        The value for 'modified' is a date in seconds, stating when the file object was last modified.  
        The value for 'bytes' is the number of bytes of the file object. It is 0 if the object is a directory.
        The value for 'is_dir' is True if the file object is a directory and False otherwise.
        
        :raises: NoSuchFilesytemObjectError if the directory does not exist
        """
        ret = {}
        directory += '/' if directory != '/' else ''
        listing = self.bucket.list(directory[1:], "/")
        for obj in listing:
            path = '/'+obj.name if obj.name[-1] != '/' else '/'+obj.name[:-1]
            path = to_str(path)
            if path == directory[:-1]:
                continue
            metadata = {}
            cal = pdt.Calendar()
            if self._is_dir(obj):
                metadata["is_dir"] = self._is_dir(obj)
                metadata["modified"] = time.time()
                metadata["bytes"] = 0
            else:
                mod_date = int(time.mktime(cal.parse(obj.last_modified)[0]))
                metadata["modified"] = mod_date
                metadata["is_dir"] = self._is_dir(obj)
                metadata["bytes"] = obj.size
            ret[path] = metadata
        return ret