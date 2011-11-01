'''
Created on 24.08.2011

@author: joe
'''
from ConfigParser import SafeConfigParser
from cloudfusion.pyfusebox.virtualfile import VirtualFile
import StringIO
from cloudfusion.store.dropbox import file_decorator

class VirtualConfigFile(VirtualFile):
    INITIAL_TEXT="""
#explanation of config parameter 1
#explanation of config parameter 2
#explanation of config parameter 3

"""
    def __init__(self, path):
        super( VirtualConfigFile, self ).__init__(path)
        
    def get_service_auth_data(self):
        vtf = file_decorator.DataFileWrapper(self.get_text())
        config = SafeConfigParser()
        config.readfp(vtf)
        return dict(config.items('auth'))
    
    def get_store_config_data(self):
        vtf = file_decorator.DataFileWrapper(self.get_text())
        config = SafeConfigParser()
        config.readfp(vtf)
        return dict(config.items('store'))
    
    #def __check_data(self, text):
    #    """::returns: True iff text contains all necessary parameters in the right format and False otherwise."""
    #    return True

    """def write(self, buf, offset):
        text_tmp = self.text[:offset]+buf+self.text[len(buf)+offset:] 
        if self.__check_data(text_tmp):
            self.text = text_tmp
            return len(buf)
        return 0"""