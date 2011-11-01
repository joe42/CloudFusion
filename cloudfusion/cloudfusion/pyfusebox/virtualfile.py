'''
Created on 24.08.2011

@author: joe
'''
import os
from cloudfusion.pyfusebox.pyfusebox import zstat
import stat
import logging.config
import cloudfusion
logging.config.fileConfig(os.path.dirname(cloudfusion.__file__)+'/config/logging.conf')
        
class VirtualFile(object):
    INITIAL_TEXT="""
Some virtual Text.
"""
    def __init__(self, path):
        self.logger = logging.getLogger('pyfusebox')
        self.path = path
        self.stats = zstat()
        self.text = self.INITIAL_TEXT
        self.stats['st_mode'] = 0777 | stat.S_IFREG
    def getattr(self):
        self.stats['st_size'] = len(self.text)
        self.stats['st_blocks'] = (int) ((self.stats['st_size'] + 4095L) / 4096L);
        return self.stats
    def truncate(self):
        self.text = ''
    def get_text(self):
        return self.text
    def get_path(self):
        return self.path
    def get_subdir(self, path):
        """:returns: the parent directory of this virtualfile's path which is a subdirectory of path"""
        dir = self.get_path()
        while dir != "/":
            parent_dir = os.path.dirname(dir)
            if parent_dir == path:
                return  os.path.basename(dir)
            dir = parent_dir
        return None
    def __str__(self):
        return self.path
    def read(self, size, offset):
        return self.text[offset: offset+size]
    def write(self, buf, offset):
        self.logger.debug("write %s to %s" % (buf,offset))
        self.text = self.text[:offset]+buf+self.text[len(buf)+offset:] 
        self.logger.debug("wrote %s bytes starting with %s..." % (len(buf), self.text[0:30]))
        return len(buf)
    def get_dir(self):
        """:returns: the pyth of the directory of this virtual file"""
        return os.path.dirname(self.path)
    def get_name(self):
        return os.path.basename(self.path)