# -*- coding: utf-8 -*- 
'''
Created on Jan 13, 2015

@author: joe
'''
import os.path

class PathName(object):
    '''Builder for path names for store tests.
    Path names are ASCII characters by default, but can be set to test
    a usual non ASCII character, or a very rare UTF-8, that is supported by fewer encodings.
    '''
    LOCAL_TESTFILE_PATH = 'cloudfusion/tests/testfile'
    LOCAL_BIGTESTFILE_PATH = 'cloudfusion/tests/bigtestfile'
    LOCAL_TESTFILE_NAME = 'testfile'
    LOCAL_BIGTESTFILE_NAME = 'bigtestfile'
    MAINDIR = '/My SugarSync'
    
    def __init__(self, maindir='My SugarSync', testdir='testdir'):
        self.UTF8_CHAR = ''
        self.path = ''
        self.is_file_path = False
        self.maindir = '/'+maindir
        self._testdir = self.maindir + '/' + testdir
    
    def copy(self):
        copy = PathName(os.path.basename(self.maindir), os.path.basename(self._testdir))
        copy.UTF8_CHAR = self.UTF8_CHAR
        copy.path = self.path
        copy.is_file_path = self.is_file_path
        return copy
    
    def get_maindir(self):
        return self.maindir
    
    def set_restricted_utf8(self):
        '''Make use of the Japanese character き in pathnames.'''
        self.UTF8_CHAR = 'き'
        return self
    
    def set_utf8(self):
        '''Make use of the unusual UTF-8 character 𠀋 in pathnames.'''
        self.UTF8_CHAR = '𠀋'
        return self
    
    def get_path(self):
        '''Gets current path.'''
        return self.path
    
    def get_filename(self):
        '''Gets current filename.'''
        if self.is_file_path:
            return os.path.basename(self.path)
        else:
            raise Exception(self.path+' is not a path to a file.')
        
    def get_parent(self):
        '''Gets current parent directory.'''
        return os.path.dirname(self.path)
    
    def get_local_testfile(self):
        return self.LOCAL_TESTFILE_PATH
    
    def get_local_testfile_name(self):
        return self.LOCAL_TESTFILE_NAME
    
    def get_local_bigtestfile(self):
        return self.LOCAL_BIGTESTFILE_PATH
    
    def get_local_bigtestfile_name(self):
        return self.LOCAL_BIGTESTFILE_NAME
    
    def testdir(self):
        '''Resets the current path and creates a new one starting with the main test directory.
        All test should take place within this directory.'''
        self.is_file_path = False
        self.path = self._testdir
        return self
    
    def dir(self, _dir='nested directory'):
        if self.is_file_path:
            raise Exception(self.path+' is not a path to a directory.')
        self.path += '/' + _dir + self.UTF8_CHAR
        return self
    
    def file(self, _file='testfile_remote', nr=None):
        if self.is_file_path:
            raise Exception(self.path+' is not a path to a directory.')
        self.path += '/' + _file + self.UTF8_CHAR
        if nr:
            self.path += str(nr)
        self.is_file_path = True
        return self
