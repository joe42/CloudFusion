'''
Created on 23.04.2011

@author: joe
'''
import os
import tempfile

class NameableFile(object):
    def __init__(self, file_object, name):
        self.file_object = file_object
        if hasattr(self.file_object, 'closed'):            
            self.closed = file_object.closed
        if hasattr(self.file_object, 'encoding'):            
            self.closed = file_object.encoding
        if hasattr(self.file_object, 'mode'):            
            self.closed = file_object.mode
        if hasattr(self.file_object, 'newlines'):            
            self.closed = file_object.newlines
        self.name = name
        self.softspace = file_object.softspace
    def close(self):
        self.file_object.close()
    def flush(self):
        self.file_object.flush()
    def fileno(self):
        return self.file_object.fileno()
    def isatty(self):
        return self.file_object.isatty()
    def next(self):
        return self.file_object.next()
    def read(self, size=None):
        return self.file_object.read(size)
    def readline(self, size=None):
        return self.file_object.readline(size)
    def readlines(self, sizehint=None):
        return self.file_object.readlines(sizehint)
    def xreadlines(self):
        return self.file_object.xreadlines()
    def seek(self, offset, whence=os.SEEK_SET):
        self.file_object.seek(offset, whence)
    def tell(self):
        return self.file_object.tell()
    def truncate(self, size):
        self.file_object.truncate(size)
    def write(self, str):
        return self.file_object.write(str)
    def writelines(self, sequence):
        return self.file_object.writelines(sequence)
    def __repr__(self):
        return repr(self.file_object)
    def __str__(self):
        return str(self.file_object)
    
class NonclosingFile(object):
    def __init__(self, file_object):
        self.file_object = file_object
        if hasattr(self.file_object, 'closed'):            
            self.closed = file_object.closed
        if hasattr(self.file_object, 'encoding'):            
            self.closed = file_object.encoding
        if hasattr(self.file_object, 'mode'):            
            self.closed = file_object.mode
        if hasattr(self.file_object, 'name'):            
            self.closed = file_object.name
        if hasattr(self.file_object, 'newlines'):            
            self.closed = file_object.newlines
        self.softspace = file_object.softspace
    def close(self):
        pass;
    def flush(self):
        self.file_object.flush()
    def fileno(self):
        return self.file_object.fileno()
    def isatty(self):
        return self.file_object.isatty()
    def next(self):
        return self.file_object.next()
    def read(self, size=None):
        return self.file_object.read(size)
    def readline(self, size=None):
        return self.file_object.readline(size)
    def readlines(self, sizehint=None):
        return self.file_object.readlines(sizehint)
    def xreadlines(self):
        return self.file_object.xreadlines()
    def seek(self, offset, whence=os.SEEK_SET):
        self.file_object.seek(offset, whence)
    def tell(self):
        return self.file_object.tell()
    def truncate(self, size):
        self.file_object.truncate(size)
    def write(self, str):
        return self.file_object.write(str)
    def writelines(self, sequence):
        return self.file_object.writelines(sequence)
    def __repr__(self):
        return repr(self.file_object)
    def __str__(self):
        return str(self.file_object)
    
        
class DataFileWrapper(file):
    def __init__(self, data):
        self.data = data
        self.len = len(self.data)
        self.offset = 0
        self.file_object = tempfile.SpooledTemporaryFile()
        self.file_object.write(data)
        self.file_object.seek(0)
    def close(self):
        self.file_object.close()
    def flush(self):
        self.file_object.flush()
    def fileno(self):
        return self.file_object.fileno()
    def isatty(self):
        return self.file_object.isatty()
    def next(self):
        return self.file_object.next()
    def read(self, size=None):
        if not size:
            return self.file_object.read()
        return self.file_object.read(size)
    def readline(self, size=None):
        if not size:
            return self.file_object.readline()
        return self.file_object.readline(size)
    def readlines(self, sizehint=None):
        return self.file_object.readlines(sizehint)
    def xreadlines(self):
        return self.file_object.xreadlines()
    def seek(self, offset, whence=os.SEEK_SET):
        self.file_object.seek(offset, whence)
    def tell(self):
        return self.file_object.tell()
    def truncate(self, size):
        self.file_object.truncate(size)
    def write(self, str):
        return self.file_object.write(str)
    def writelines(self, sequence):
        return self.file_object.writelines(sequence)
    def __repr__(self):
        return repr(self.file_object)
    def __str__(self):
        return str(self.file_object)


