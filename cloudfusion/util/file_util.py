'''
Created on Sep 26, 2014

@author: joe
'''
import os

def get_file_size_in_mb(file):
    '''*file* may be the path to a file or a fileobject'''
    return get_file_size_in_bytes(file) / 1000.0 / 1000  


def get_file_size_in_bytes(file):
    '''*file* may be the path to a file or a fileobject'''
    if isinstance(file, basestring):
        return os.path.getsize(file)
    #assume it is a fileobject
    previous_pos = file.tell() 
    file.seek(0, os.SEEK_END)
    size_in_mb = file.tell()
    file.seek(previous_pos)
    return size_in_mb