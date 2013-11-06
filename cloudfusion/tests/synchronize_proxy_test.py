'''
Created on Jun 11, 2013

@author: joe
'''
from cloudfusion.util.synchronize_proxy import SynchronizeProxy
from nose.tools import *
import threading
from random import random
import time

class TestSubject(object):
    def __init__(self):
        self.value = 0 #on synchronized access, value is always 0
    def get_value(self):
        return self.value
    def temporarily_change_value(self):
        self.__change()
        self.__unchange()
    def __change(self):
        self.value = 1 #can only be observed on asynchronous access
        time.sleep(random()/2)
        time.sleep(random()/2)
    def __unchange(self):
        self.value = 0

class TestingThread(threading.Thread):
    def __init__(self, test_obj):
        super( TestingThread, self ).__init__()
        self.test_obj = test_obj
    def run(self):
        self.test_obj.temporarily_change_value()
        self.test_obj.temporarily_change_value()
        self.test_obj.temporarily_change_value()
        self.test_obj.temporarily_change_value()
        self.observed_value = self.test_obj.get_value()
    
def test():
    threads = []
    test_obj = SynchronizeProxy(TestSubject())
    for i in range(0,500):
        t = TestingThread(test_obj)
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    value_sum = 0
    for t in threads:
        value_sum += t.observed_value
    print str(value_sum)+"sum"
    assert value_sum == 0, "Value of test subject has changed to one at least once, which can only be observed with an asynchronous access."
'''
    This test does not need to pass. It is just meant as an indicator for the amount of threads that are needed to cause an error in the above test.
    The number of threads should actually be much higher in order to return a significant result. But this has been tested successfully with 2000 threads.
    threads = []
    test_obj = TestSubject()
    for i in range(0,500):
        t = TestingThread(test_obj)
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    value_sum = 0
    for t in threads:
        value_sum += t.observed_value
    print str(value_sum)+"sum"
    assert value_sum > 0, "Value of test subject has not been changed to one at least once, which should only happen with synchronized access."
''' 
        
    
    
    
    

