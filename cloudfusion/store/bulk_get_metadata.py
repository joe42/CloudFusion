'''
Created on 08.04.2011
'''
import os.path

    
class BulkGetMetadata(object):
    def get_bulk_metadata(self, directory):
        """:returns: A dictionary mapping the path of every file object in *directory* to a dictionary with the keys\
        'modified', 'bytes' and 'is_dir' containing the corresponding metadata for the file object.
        
        The value for 'modified' is a date in seconds, stating when the file object was last modified.  
        The value for 'bytes' is the number of bytes of the file object. It is 0 if the object is a directory.
        The value for 'is_dir' is True if the file object is a directory and False otherwise.
        
        :raises: NoSuchFilesytemObjectError if the directory does not exist
        """
        raise NotImplementedError()
