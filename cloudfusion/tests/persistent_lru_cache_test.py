'''
Created on 04.06.2011

@author: joe
'''
from cloudfusion.util.persistent_lru_cache import PersistentLRUCache
import time, os
from nose.tools import *
import shutil
    
directory="/tmp/testpersistentcache"

def set_up():
    os.mkdir(directory)
        
def tear_down():
    shutil.rmtree(directory)

@with_setup(set_up, tear_down)    
def test_refresh():
    test_obj = PersistentLRUCache(directory=directory)
    test_obj.refresh("some_key", "43", time.time())
    test_obj.refresh("some_key","42", time.time())
    assert test_obj.get_value("some_key") == "42"
    test_obj.refresh("some_key","43", time.time()-1000)
    assert test_obj.get_value("some_key") == "42", "Refresh should not have worked since the modified time of the 'disk' entry is older than the cache entry."
    assert not test_obj.is_dirty("some_key")

@with_setup(set_up, tear_down)   
def test_is_expired():
    test_obj = PersistentLRUCache(directory,1)
    test_obj.write("some_key", "42")
    time.sleep(2)
    assert test_obj.is_expired("some_key")
       
@with_setup(set_up, tear_down)   
def test_update():
    test_obj = PersistentLRUCache(directory,1)
    test_obj.write("some_key", "42")
    time.sleep(2)
    assert test_obj.is_expired("some_key")
    test_obj.update("some_key")
    assert not test_obj.is_expired("some_key")
    
@with_setup(set_up, tear_down)   
def test_write():
    test_obj = PersistentLRUCache(directory=directory)
    test_obj.write("some_key", "42")
    test_obj.write("42", "some_key")
    assert test_obj.get_value("some_key") == "42"
    assert test_obj.get_value("42") == "some_key"
    assert test_obj.is_dirty("some_key")
    
@with_setup(set_up, tear_down)   
def test_get_keys():
    test_obj = PersistentLRUCache(directory=directory)
    test_obj.refresh("some_key", "43", time.time())
    test_obj.write("some_other_key", "42")
    assert "some_key" in test_obj.get_keys()
    assert "some_other_key" in test_obj.get_keys()
    assert not "some_keyXYZ" in test_obj.get_keys()
    
@with_setup(set_up, tear_down)   
def test_get_value():
    test_obj = PersistentLRUCache(directory=directory)
    test_obj.refresh("some_key", "43", time.time())
    assert test_obj.get_value("some_key") == "43"
    test_obj.write("some_key", "42")
    assert test_obj.get_value("some_key") == "42"
    
@with_setup(set_up, tear_down)   
def test_get_modified():
    test_obj = PersistentLRUCache(directory=directory)
    modified_time = time.time()
    test_obj.refresh("some_key", "43", modified_time)
    assert test_obj.get_modified("some_key") == modified_time
    test_obj.write("some_key", "42")
    assert test_obj.get_modified("some_key") < time.time()
    
    
@with_setup(set_up, tear_down)   
def test_set_modified():
    test_obj = PersistentLRUCache(directory=directory)
    modified_time = 42
    before_modification = time.time()
    test_obj.write("some_key", "101")
    assert test_obj.get_modified("some_key") < time.time()
    assert test_obj.get_modified("some_key") > before_modification
    test_obj.set_modified("some_key", modified_time)
    assert test_obj.get_modified("some_key") == modified_time
    
@with_setup(set_up, tear_down)   
def test_get_size_of_dirty_data():
    test_obj = PersistentLRUCache(directory=directory)
    assert test_obj.get_size_of_dirty_data() == 0
    test_obj.refresh("some_key", "abcd",  time.time())
    assert test_obj.get_size_of_dirty_data() == 0
    test_obj.write("some_other_key", "42")
    assert test_obj.get_size_of_dirty_data() == 2
    test_obj.write("some_other_key", "52")
    assert test_obj.get_size_of_dirty_data() == 2
    test_obj.write("some_key", "abcd")
    assert test_obj.get_size_of_dirty_data() == 6
    test_obj.refresh("some_other_key", "42", time.time())
    assert test_obj.get_size_of_dirty_data() == 4
    
@with_setup(set_up, tear_down)   
def test_get_size_of_cached_data():
    test_obj = PersistentLRUCache(directory=directory)
    modified_time = time.time()
    assert test_obj.get_size_of_cached_data() == 0
    test_obj.refresh("some_key", "abcd", modified_time)
    assert test_obj.get_size_of_cached_data() == 4
    test_obj.write("some_other_key", "42")
    assert test_obj.get_size_of_cached_data() == 6
    test_obj.write("some_other_key", "52")
    assert test_obj.get_size_of_cached_data() == 6
    test_obj.refresh("some_key", "abcd", modified_time)
    assert test_obj.get_size_of_cached_data() == 6
    
@with_setup(set_up, tear_down)   
def test_is_dirty():
    test_obj = PersistentLRUCache(directory=directory)
    test_obj.refresh("some_key", "43", time.time())
    assert not test_obj.is_dirty("some_key")
    test_obj.write("some_key", "42")
    assert test_obj.is_dirty("some_key")

@with_setup(set_up, tear_down)   
def test_exists():
    test_obj = PersistentLRUCache(directory=directory)
    assert not test_obj.exists("some_key")
    test_obj.write("some_key", "42")
    assert test_obj.exists("some_key")
    assert not test_obj.exists("some_other_key")
    
@with_setup(set_up, tear_down)   
def test_delete():
    test_obj = PersistentLRUCache(directory=directory)
    test_obj.write("some_key", "42")
    test_obj.write("42", "some_key")
    test_obj.delete("some_key")
    test_obj.delete("non_existant_key")
    test_obj.delete("42")
    assert_raises( KeyError, test_obj.get_value, ("42") )
    assert_raises( KeyError, test_obj.get_value, ("some_key") )
    assert not test_obj.exists("42")
    assert not test_obj.exists("some_key")

@with_setup(set_up, tear_down)   
def test_reorder():
    test_obj = PersistentLRUCache(directory=directory)
    test_obj.write("/xxx", "")
    test_obj.write("/yyy", "")
    test_obj.get_value("/xxx")
    test_obj.delete("/xxx")
    test_obj.delete("/yyy")
        
@with_setup(set_up, tear_down)   
def test_persistence():
    test_obj = PersistentLRUCache(directory=directory, maxsize_in_MB=0)
    test_obj.write("keyX", "42")
    assert test_obj.get_value("keyX") == "42"
    test_obj.entries.close()
    test_obj = PersistentLRUCache(directory=directory)
    assert test_obj.get_value("keyX") == "42"
    
@with_setup(set_up, tear_down)   
def test_resize_zerosize():
    test_obj = PersistentLRUCache(directory=directory, expiration_time=0.00001, maxsize_in_MB=0)
    test_obj.set_resize_intervall(0)
    test_obj.refresh("some_key", "43", time.time())
    time.sleep(0.001)
    assert "some_key" in test_obj.get_keys()
    test_obj.refresh("some_other_key", "42", time.time())
    assert "some_other_key" in test_obj.get_keys()
    assert not "some_key" in test_obj.get_keys() #deleted due to internal resize
    assert test_obj.get_value("some_other_key") == "42"
    
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