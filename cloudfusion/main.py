'''
Created on 12.05.2011

'''
from cloudfusion.fuse import FUSE
import os, sys
import logging.config
from mylogging import db_logging_thread
from cloudfusion.mylogging.nullhandler import NullHandler
import cloudfusion
from cloudfusion.pyfusebox.transparent_configurable_pyfusebox import TransparentConfigurablePyFuseBox
import shutil
import argparse
from threading import Thread
import time

def check_arguments(args):
    if not len(args) in [2,3,4,5,6]:
        print 'usage: %s [--config path/to/config.ini] mountpoint [foreground] [log]' % args[0]
        print '--config configfile.ini: initialization file to automatically start Cloudfusion with a storage provider like Dropbox or Sugarsync'
        print 'mountpoint: empty folder in which the virtual file system is created'
        print 'foreground: run program in foreground'
        print 'log: write logs to the directory .cloudfusion/logs'
        print 'big_write option of fuse is used automatically to optimize throughput if the system supports it (requires fuse 2.8 or higher)'
        exit(1)

def set_configuration(mountpoint, config_file):
    '''Wait until the file system is mounted, then overwrite the virtual configuration file.
    This will configure Cloudfusion so that it can be used.'''
    virtual_configuration_file = mountpoint+'/config/config'
    while not os.path.exists(virtual_configuration_file):
        time.sleep(1) 
    shutil.copyfile(config_file, virtual_configuration_file) 
        
    
def start_configuration_thread(mountpoint, config_file):
    '''Start a thread to write the configuration file 
    to config/config, after the file system has been mounted.'''
    config_thread = Thread(target=set_configuration, args=(mountpoint, config_file))
    config_thread.setDaemon(True)
    config_thread.start()
            

def main():
    check_arguments(sys.argv)
    parser = argparse.ArgumentParser()
    parser.add_argument('mountpoint')
    parser.add_argument('--config', help='Configuration file.')
    parser.add_argument('args', nargs=argparse.REMAINDER) #collect all arguments positioned after positional and optional parameters 
    args = parser.parse_args()
    foreground  = 'foreground' in args.args 
    if not "log" in args.args:
        logging.getLogger().addHandler(NullHandler())
    else:
        if not os.path.exists(".cloudfusion/logs"):
            os.makedirs(".cloudfusion/logs")
        logging.config.fileConfig(os.path.dirname(cloudfusion.__file__)+'/config/logging.conf')
        db_logging_thread.start()    
    mountpoint = args.mountpoint
    if args.config: #evaluates to false
        if not os.path.exists(args.config):
            exit(1)
        start_configuration_thread(mountpoint, args.config)
    if not os.path.exists(mountpoint):
        os.makedirs(mountpoint)
    fuse_operations = TransparentConfigurablePyFuseBox(mountpoint)
    try:
        #first try to mount file system with big_writes option (more performant)
        FUSE(fuse_operations, mountpoint, foreground=foreground, nothreads=True, big_writes=True, max_read=131072, max_write=131072) 
    except RuntimeError, e:
        FUSE(fuse_operations, mountpoint, foreground=foreground, nothreads=True)
    
if __name__ == '__main__':
    main()