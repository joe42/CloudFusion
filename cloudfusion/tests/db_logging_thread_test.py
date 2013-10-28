'''
Created on Oct 27, 2013

@author: joe
'''
from nose.tools import *
import threading
from random import random
import time
import logging
import logging.config
from cloudfusion.mylogging import db_logging_thread
import cloudfusion
import os
import multiprocessing

class TestingThread(threading.Thread):
    def __init__(self):
        super( TestingThread, self ).__init__()
        self.logger = logging.getLogger('pyfusebox')
    def run(self):
        remaining_loops = 2000
        while remaining_loops != 0:
            remaining_loops -= 1
            time.sleep(random())
            self.logger.debug("Thread %s says hello!" % threading.current_thread())
            
            
class TestingProcess(object):
    def __init__(self):
        self.process = multiprocessing.Process(target=self.run)
        self.logger = logging.getLogger('sugarsync')
        
    def start(self):
        self.process.start()
        
    def join(self):
        self.process.join()
        
        
    def run(self):
        self.logger = db_logging_thread.make_logger_multiprocessingsave(self.logger)
        remaining_loops = 2000
        while remaining_loops != 0:
            remaining_loops -= 1
            time.sleep(random())
            self.logger.debug("Process %s says hello!" % os.getpid())
            
        
            
def test():
    logging.config.fileConfig(os.path.dirname(cloudfusion.__file__)+'/config/logging.conf')
    db_logging_thread.start()
    time.sleep(2)
    threads = []
    processes = []
    for i in range(0,10):
        t = TestingThread()
        threads.append(t)
    for i in range(0,10):
        p = TestingProcess()
        processes.append(p)
    for p in processes:
        p.start()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for p in processes:
        p.join()
    
        
    
    
    
    

