# -*- coding: utf-8 -*-
'''
Created on 04.06.2011

@author: joe
'''
from cloudfusion.util.persistent_lru_cache import PersistentLRUCache
import time, os
from nose.tools import *
import shutil
from cloudfusion.util.lru_cache import LISTHEAD

KEY1 = "KEY1𠀋"
KEY2 = "KEY2𠀋"
KEY3 = "KEY3𠀋"
VALUE1 = "42"
VALUE2 = "43"
VALUE3 = "52"
VALUE4 = "𠀋"
directory="/tmp/testpersistentcache"

def set_up():
    os.mkdir(directory)
        
def tear_down():
    shutil.rmtree(directory)

@with_setup(set_up, tear_down)    
def test_refresh():
    test_obj = PersistentLRUCache(directory=directory)
    test_obj.refresh(KEY1, VALUE2, time.time())
    test_obj.refresh(KEY1,VALUE1, time.time())
    assert test_obj.get_value(KEY1) == VALUE1
    test_obj.refresh(KEY1,VALUE2, time.time()-1000)
    assert test_obj.get_value(KEY1) == VALUE1, "Refresh should not have worked since the modified time of the 'disk' entry is older than the cache entry."
    assert not test_obj.is_dirty(KEY1)

@with_setup(set_up, tear_down)   
def test_is_expired():
    test_obj = PersistentLRUCache(directory,1)
    test_obj.write(KEY1, VALUE1)
    time.sleep(2)
    assert test_obj.is_expired(KEY1)
       
@with_setup(set_up, tear_down)   
def test_update():
    test_obj = PersistentLRUCache(directory,1)
    test_obj.write(KEY1, VALUE1)
    time.sleep(2)
    assert test_obj.is_expired(KEY1)
    test_obj.update(KEY1)
    assert not test_obj.is_expired(KEY1)
    
@with_setup(set_up, tear_down)   
def test_write():
    test_obj = PersistentLRUCache(directory=directory)
    test_obj.write(KEY1, VALUE1)
    test_obj.write(VALUE1, KEY1)
    assert test_obj.get_value(KEY1) == VALUE1
    assert test_obj.get_value(VALUE1) == KEY1
    assert test_obj.is_dirty(KEY1)
    
@with_setup(set_up, tear_down)   
def test_get_keys():
    test_obj = PersistentLRUCache(directory=directory)
    test_obj.refresh(KEY1, VALUE2, time.time())
    test_obj.write(KEY2, VALUE1)
    assert KEY1 in test_obj.get_keys()
    assert KEY2 in test_obj.get_keys()
    assert not VALUE4 in test_obj.get_keys()
    
@with_setup(set_up, tear_down)   
def test_get_value():
    test_obj = PersistentLRUCache(directory=directory)
    test_obj.refresh(KEY1, VALUE2, time.time())
    assert test_obj.get_value(KEY1) == VALUE2
    test_obj.write(KEY1, VALUE1)
    assert test_obj.get_value(KEY1) == VALUE1
    
@with_setup(set_up, tear_down)   
def test_get_modified():
    test_obj = PersistentLRUCache(directory=directory)
    modified_time = time.time()
    test_obj.refresh(KEY1, VALUE2, modified_time)
    assert test_obj.get_modified(KEY1) == modified_time
    test_obj.write(KEY1, VALUE1)
    time.sleep(0.01)
    assert test_obj.get_modified(KEY1) < time.time()
    
    
@with_setup(set_up, tear_down)   
def test_set_modified():
    test_obj = PersistentLRUCache(directory=directory)
    modified_time = 42
    before_modification = time.time()
    test_obj.write(KEY1, VALUE1)
    time.sleep(0.01)
    assert test_obj.get_modified(KEY1) < time.time()
    assert test_obj.get_modified(KEY1) > before_modification
    test_obj.set_modified(KEY1, modified_time)
    assert test_obj.get_modified(KEY1) == modified_time
    
@with_setup(set_up, tear_down)   
def test_get_size_of_dirty_data():
    test_obj = PersistentLRUCache(directory=directory)
    assert test_obj.get_size_of_dirty_data() == 0
    test_obj.refresh(KEY1, VALUE4,  time.time())
    assert test_obj.get_size_of_dirty_data() == 0
    test_obj.write(KEY2, VALUE1)
    assert test_obj.get_size_of_dirty_data() == 2
    test_obj.write(KEY2, VALUE3)
    assert test_obj.get_size_of_dirty_data() == 2
    test_obj.write(KEY1, VALUE4)
    assert test_obj.get_size_of_dirty_data() == 6
    test_obj.refresh(KEY2, VALUE1, time.time())
    assert test_obj.get_size_of_dirty_data() == 4
    
@with_setup(set_up, tear_down)   
def test_get_size_of_cached_data():
    test_obj = PersistentLRUCache(directory=directory)
    modified_time = time.time()
    assert test_obj.get_size_of_cached_data() == 0
    test_obj.refresh(KEY1, VALUE4, modified_time)
    assert test_obj.get_size_of_cached_data() == 4
    test_obj.write(KEY2, VALUE1)
    assert test_obj.get_size_of_cached_data() == 6
    test_obj.write(KEY2, VALUE3)
    assert test_obj.get_size_of_cached_data() == 6
    test_obj.refresh(KEY1, VALUE4, modified_time)
    assert test_obj.get_size_of_cached_data() == 6
    
@with_setup(set_up, tear_down)   
def test_is_dirty():
    test_obj = PersistentLRUCache(directory=directory)
    test_obj.refresh(KEY1, VALUE2, time.time())
    assert not test_obj.is_dirty(KEY1)
    test_obj.write(KEY1, VALUE1)
    assert test_obj.is_dirty(KEY1)

@with_setup(set_up, tear_down)   
def test_exists():
    test_obj = PersistentLRUCache(directory=directory)
    assert not test_obj.exists(KEY1)
    test_obj.write(KEY1, VALUE1)
    assert test_obj.exists(KEY1)
    assert not test_obj.exists(KEY2)
    
@with_setup(set_up, tear_down)   
def test_delete():
    test_obj = PersistentLRUCache(directory=directory)
    test_obj.write(KEY1, VALUE1)
    test_obj.write(VALUE1, KEY1)
    test_obj.delete(KEY1)
    test_obj.delete(KEY3) # non existent key
    test_obj.delete(VALUE1)
    assert_raises( KeyError, test_obj.get_value, (VALUE1) )
    assert_raises( KeyError, test_obj.get_value, (KEY1) )
    assert not test_obj.exists(VALUE1)
    assert not test_obj.exists(KEY1)

@with_setup(set_up, tear_down)   
def test_reorder():
    test_obj = PersistentLRUCache(directory=directory)
    test_obj.write(KEY1, "")
    test_obj.write(KEY2, "")
    test_obj.get_value(KEY1)
    assert test_obj.entries[LISTHEAD] == KEY1
    test_obj.peek(KEY2)
    test_obj.peek_file(KEY2)
    assert test_obj.entries[LISTHEAD] == KEY1
    test_obj.delete(KEY1)
    assert test_obj.entries[LISTHEAD] == KEY2
    test_obj.delete(KEY2)
        
@with_setup(set_up, tear_down)   
def test_persistence():
    test_obj = PersistentLRUCache(directory=directory, maxsize_in_MB=0)
    test_obj.write(KEY3, VALUE1)
    assert test_obj.get_value(KEY3) == VALUE1
    test_obj.entries.close()
    test_obj = PersistentLRUCache(directory=directory)
    assert test_obj.get_value(KEY3) == VALUE1
    
@with_setup(set_up, tear_down)   
def test_resize_zerosize():
    test_obj = PersistentLRUCache(directory=directory, expiration_time=0.00001, maxsize_in_MB=0)
    test_obj.set_resize_intervall(0)
    test_obj.refresh(KEY1, VALUE2, time.time())
    time.sleep(0.001)
    assert KEY1 in test_obj.get_keys()
    test_obj.refresh(KEY2, VALUE1, time.time())
    assert KEY2 in test_obj.get_keys()
    assert not KEY1 in test_obj.get_keys() #deleted due to internal resize
    assert test_obj.get_value(KEY2) == VALUE1
    
@with_setup(set_up, tear_down)   
def test_resize():
    test_obj = PersistentLRUCache(directory=directory, expiration_time=0.00001,maxsize_in_MB=30)
    test_obj.set_resize_intervall(0)
    for i in range(10,62):
        test_obj.refresh(str(i), "a"*2000000, time.time())
        time.sleep(0.001)
        assert test_obj.get_size_of_cached_data() < 30000003
        for j in range(10,i-14+1):
            assert not str(j) in test_obj.get_keys()
        for j in range(10,i+1)[-14:]:
            assert test_obj.get_value(str(j)) == "a"*2000000
            
@with_setup(set_up, tear_down)   
def test_resize_dirty():
    test_obj = PersistentLRUCache(directory=directory, maxsize_in_MB=0)
    test_obj.set_resize_intervall(0)
    for i in range(10,62):
        test_obj.write(str(i), "a"*2000000)
    assert test_obj.get_size_of_cached_data() > 50000000