'''
Created on 12.05.2011

@author: joe
'''
from cloudfusion.pyfusebox.pyfusebox import PyFuseBox
from cloudfusion.pyfusebox.configurable_pyfusebox import ConfigurablePyFuseBox
from cloudfusion.fuse import FUSE
import os, sys, stat,  time
from dropbox import auth
import getpass
from ConfigParser import SafeConfigParser

def check_arguments(args):
    if not len(args) in [2,3]:
        print 'usage: %s mountpoint [foreground]' % args[0]
        exit(1)

def main():
    check_arguments(sys.argv)
    foreground  = False 
    if len(sys.argv) == 3:
        foreground = True
    #store = get_store(service, key, secret, username, password)
        #store = MetadataCachingStore( CachingStore( MetadataCachingStore( store ) ) )
    fuse_operations = ConfigurablePyFuseBox(sys.argv[1])
    FUSE(fuse_operations, sys.argv[1], foreground=foreground, nothreads=True)
    
if __name__ == '__main__':
    main()