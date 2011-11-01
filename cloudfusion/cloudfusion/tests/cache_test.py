'''
Created on 04.06.2011

@author: joe
'''
from cloudfusion.util.cache import Cache
import time
from nose.tools import *
    
def test_refresh():
    test_obj = Cache(1)
    test_obj.refresh("some_key", 43, time.time())
    test_obj.refresh("some_key",42, time.time())
    assert test_obj.get_value("some_key") == 42
    test_obj.refresh("some_key",43, time.time()-1000)
    assert test_obj.get_value("some_key") == 42, "Refresh should not have worked since the modified time of the 'disk' entry is older than the cache entry."
    assert not test_obj.is_dirty("some_key")

def test_is_expired():
    test_obj = Cache(1)
    test_obj.write("some_key", 42)
    time.sleep(2)
    assert test_obj.is_expired("some_key")
       
def test_update():
    test_obj = Cache(1)
    test_obj.write("some_key", 42)
    time.sleep(2)
    assert test_obj.is_expired("some_key")
    test_obj.update("some_key")
    assert not test_obj.is_expired("some_key")
    
def test_write():
    test_obj = Cache(1)
    test_obj.write("some_key", 42)
    test_obj.write(42, "some_key")
    assert test_obj.get_value("some_key") == 42
    assert test_obj.get_value(42) == "some_key"
    assert test_obj.is_dirty("some_key")
    
def test_get_keys():
    test_obj = Cache(1)
    test_obj.refresh("some_key", 43, time.time())
    test_obj.write("some_other_key", 42)
    assert "some_key" in test_obj.get_keys()
    assert "some_other_key" in test_obj.get_keys()
    assert not "some_keyXYZ" in test_obj.get_keys()
    
def test_get_value():
    test_obj = Cache(1)
    test_obj.refresh("some_key", 43, time.time())
    assert test_obj.get_value("some_key") == 43
    test_obj.write("some_key", 42)
    assert test_obj.get_value("some_key") == 42
    
def test_get_modified():
    test_obj = Cache(1)
    modified_time = time.time()
    test_obj.refresh("some_key", 43, modified_time)
    assert test_obj.get_modified("some_key") == modified_time
    test_obj.write("some_key", 42)
    assert test_obj.get_modified("some_key") < time.time()
    
def test_get_size_of_dirty_data():
    test_obj = Cache(1)
    assert test_obj.get_size_of_dirty_data() == 0
    test_obj.refresh("some_key", "abcd",  time.time())
    assert test_obj.get_size_of_dirty_data() == 0
    test_obj.write("some_other_key", 42)
    assert test_obj.get_size_of_dirty_data() == 2
    test_obj.write("some_other_key", 52)
    assert test_obj.get_size_of_dirty_data() == 2
    test_obj.write("some_key", "abcd")
    assert test_obj.get_size_of_dirty_data() == 6
    test_obj.refresh("some_other_key", 42, time.time())
    assert test_obj.get_size_of_dirty_data() == 4
    
def test_get_size_of_cached_data():
    test_obj = Cache(1)
    modified_time = time.time()
    assert test_obj.get_size_of_cached_data() == 0
    test_obj.refresh("some_key", "abcd", modified_time)
    assert test_obj.get_size_of_cached_data() == 4
    test_obj.write("some_other_key", 42)
    assert test_obj.get_size_of_cached_data() == 6
    test_obj.write("some_other_key", 52)
    assert test_obj.get_size_of_cached_data() == 6
    test_obj.refresh("some_key", "abcd", modified_time)
    assert test_obj.get_size_of_cached_data() == 6
    
def test_is_dirty():
    test_obj = Cache(1)
    test_obj.refresh("some_key", 43, time.time())
    assert not test_obj.is_dirty("some_key")
    test_obj.write("some_key", 42)
    assert test_obj.is_dirty("some_key")

def test_exists():
    test_obj = Cache(1)
    assert not test_obj.exists("some_key")
    test_obj.write("some_key", 42)
    assert test_obj.exists("some_key")
    assert not test_obj.exists("some_other_key")
    
def test_delete():
    test_obj = Cache(1)
    test_obj.write("some_key", 42)
    test_obj.write(42, "some_key")
    test_obj.delete("some_key")
    test_obj.delete("non_existant_key")
    test_obj.delete(42)
    assert_raises( KeyError, test_obj.get_value, (42) )
    assert_raises( KeyError, test_obj.get_value, ("some_key") )
    assert not test_obj.exists(42)
    assert not test_obj.exists("some_key")
    

