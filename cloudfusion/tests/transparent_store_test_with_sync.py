'''
Module to test all implementations of the :class:`TransparentStore` interface.
The actual test only tests an implementation with a remote service; For instance test_chunk_cache_store
tests the TransparentStore implementation :class:`TransparentChunkMultiprocessingCachingStore` with box.com.
Though the remote service is arbitrary, as the module concentrates on testing the TransparentStore
implementation.
The tests make use of the synchronization mechanism in the TransparentStore interface for instance,
to wait until an upload is complete. 

TODO: Use small cache size, upload several files and synchronize so that the files are retrieved from the remote store. 

The tests can be executed in parallel by calling each test individually
in a separate nosetests process like this::

    nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_chunk_cache_store &
    nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_chunk_metadata_cache_store &
    nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_cache_store &
    nosetests -v -s -x cloudfusion.tests.transparent_store_test_with_sync:test_metadata_cache_store &


Created on Dec 18, 2014

@author: joe
'''
import os
from cloudfusion.store.dropbox.dropbox_store import DropboxStore
from cloudfusion.store.sugarsync.sugarsync_store import SugarsyncStore
from functools import partial
from nose.tools import *
from cloudfusion.store.store import *
from cloudfusion.store.metadata_caching_store import MetadataCachingStore
import os.path, time
import tempfile
from ConfigParser import SafeConfigParser
import cloudfusion
from cloudfusion.store.transparent_chunk_caching_store import TransparentChunkMultiprocessingCachingStore
from cloudfusion.store.transparent_caching_store import TransparentMultiprocessingCachingStore
from cloudfusion.store.webdav.webdav_store import WebdavStore
from time import sleep
from cloudfusion.tests.config import *
from cloudfusion.tests.path_name import PathName

pathname = PathName(testdir='testdir_with_sync')
store = None

def teardown_func():
    try:
        store.delete(pathname.testdir().get_path(), True)
    except Exception, e:
        pass

@with_setup(teardown=teardown_func)
def test_chunk_cache_store():
    global store 
    config = get_webdav_box_config()
    store = TransparentChunkMultiprocessingCachingStore( WebdavStore(config),
                                                         cache_expiration_time=1, 
                                                         cache_size_in_mb=0 )
    for test in _generate_store_tests(store, "TransparentChunkCachingStore WebdavStore box", 
                                      include_space_tests=False):
        yield test

@with_setup(teardown=teardown_func)
def test_chunk_metadata_cache_store():
    global store 
    config = get_webdav_yandex_config()
    store = TransparentChunkMultiprocessingCachingStore( MetadataCachingStore( WebdavStore(config) ),
                                                         cache_expiration_time=1, 
                                                         cache_size_in_mb=0 )
    for test in _generate_store_tests(store, "TransparentChunkCachingStore WebdavStore yandex", 
                                      include_space_tests=False):
        yield test

@with_setup(teardown=teardown_func)
def test_cache_store():
    global store 
    config = get_dropbox_config()
    store = DropboxStore(config)
    store = TransparentMultiprocessingCachingStore( store,
                                                    cache_expiration_time=1, 
                                                    cache_size_in_mb=0 )
    for test in _generate_store_tests(store, "TransparentCachingStore DropboxStore"):
        yield test
        

@with_setup(teardown=teardown_func)
def test_metadata_cache_store():
    global store 
    config = get_sugarsync_config()
    store = SugarsyncStore(config)
    store = TransparentMultiprocessingCachingStore(  MetadataCachingStore(store),
                                                     cache_expiration_time=1, 
                                                     cache_size_in_mb=0 )
    for test in _generate_store_tests(store, "TransparentCachingStore MetadataCachingStore SugarsyncStore"):
        yield test
        
def _create_test_directory(store):
    try:
        store.create_directory(pathname.MAINDIR)
    except AlreadyExistsError:
        pass
    try:
        store.create_directory(pathname.testdir().get_path())
    except AlreadyExistsError:
        pass
 
def _generate_store_tests(store, description_of_store, include_space_tests=True):
    '''Generate general tests for store.
    :param store: The object to test.
    :type store: :class:`Store`
    :param description_of_store: String to describe the store instance.
    :param include_space_tests: Indicates if tests about the free, used, and overall space should be executed.
    :type include_space_tests: boolean'''
    store.delete(pathname.testdir().get_path(), True)
    _create_test_directory(store)
    if include_space_tests:
        test = partial(_test_get_free_space, store)
        test.description = description_of_store+": getting free space"
        yield (test, )
        test = partial(_test_get_overall_space, store)
        test.description = description_of_store+": getting overall space"
        yield (test, )
        test = partial(_test_get_used_space, store)
        test.description = description_of_store+": getting used space"
        yield (test, )
    test = partial(_test_fail_on_is_dir, store)
    test.description = description_of_store+": fail on determining if file system object is a file or a directory"
    yield (test, ) 
    test = partial(_test_fail_on_get_bytes, store)
    test.description = description_of_store+": fail on getting number of bytes from file"
    yield (test, ) 
    test = partial(_test_fail_on_get_modified, store)
    test.description = description_of_store+": fail on getting modified time"
    yield (test, ) 
    test = partial(_test_create_delete_directory, store)
    test.description = description_of_store+": creating and deleting directory"
    yield (test, )
    test = partial(_test_store_delete_file, store)
    test.description = description_of_store+": creating and deleting file"
    yield (test, )
    test = partial(_test_get_file, store)
    test.description = description_of_store+": getting file"
    yield (test, )
    test = partial(_test_duplicate, store)
    test.description = description_of_store+": copying (duplicating) file and directory"
    yield (test, )
    test = partial(_test_move_directory, store)
    test.description = description_of_store+": moving directory"
    yield (test, )
    test = partial(_test_move_file, store)
    test.description = description_of_store+": moving file"
    yield (test, )
    test = partial(_test_get_bytes, store)
    test.description = description_of_store+": getting number of bytes from file"
    yield (test, )
    test = partial(_test_is_dir, store)
    test.description = description_of_store+": determine if file system object is a file or a directory"
    yield (test, )
    test = partial(_test_account_info, store)
    test.description = description_of_store+": getting account info"
    yield (test, )
    test = partial(_test_get_modified, store)
    test.description = description_of_store+": getting modified time"
    yield (test, )
    test = partial(_test_get_directory_listing, store)
    test.description = description_of_store+": getting directory listing"
    yield (test, )  
    test = partial(_test_exists, store)
    test.description = description_of_store+": determine if file and directory exists"
    yield (test, )  
    test = partial(_test_nested_move, store)
    test.description = description_of_store+": move nested files"
    yield (test, ) 
    test = partial(_test_nested_duplicate, store)
    test.description = description_of_store+": duplicate nested files"
    yield (test, ) 
    store.delete(pathname.testdir().get_path(), True)

def _assert_all_in(in_list, all_list):
    assert all(item in in_list for item in all_list), "expected all items in %s to be found in %s" % (all_list, in_list)
    
#def _test_with_root_filepath(store):
#    listing = store.get_directory_listing("/")
#    cached_listing1 = store.get_directory_listing("/")
#    cached_listing2 = store.get_directory_listing("/")
#    root = pathname.testdir().get_path()+"/"
#    _assert_all_in(listing, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+pathname.LOCAL_TESTFILE_NAME]) 
#    _assert_all_in(cached_listing1, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+pathname.LOCAL_TESTFILE_NAME]) 
#    _assert_all_in(cached_listing2, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+pathname.LOCAL_TESTFILE_NAME]) 
#    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path(), REMOTE_TESTFILE_NAME)
#    resp = store.get_file(pathname.testdir().get_path()+"/"+REMOTE_TESTFILE_NAME)
#    _delete_file(store, REMOTE_TESTFILE_NAME, pathname.testdir().get_path())
#    assert len(resp) == 4, "length of file from remote side should be 4 bytes, since in testfile I stored the word 'test'"
    
def finish_upload(store):
    while store.get_dirty_files() != []:
        time.sleep(2)
    
def _test_get_file(store):
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path(), pathname.file().get_filename())
    finish_upload(store)
    first_resp = store.get_file(pathname.get_path())
    second_resp = store.get_file(pathname.get_path())
    store.delete(pathname.testdir().file().get_path(), is_dir=False)
    assert first_resp == second_resp, "first response should be same as second response, but %s != %s" % (first_resp, second_resp)
    with open(pathname.LOCAL_TESTFILE_PATH) as file:
        assert file.read() == first_resp, "Remote file differs from the local file."
      
def _test_fail_on_is_dir(store): 
    assert_raises(NoSuchFilesytemObjectError, store.is_dir, pathname.testdir().file('non existent file').get_path())
    assert_raises(NoSuchFilesytemObjectError, store.is_dir, pathname.testdir().file('non existent dir').get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    finish_upload(store)
    store.delete(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME, False)
    store.create_directory(pathname.testdir().file('deleted dir').get_path())
    store.delete(pathname.testdir().file('deleted dir').get_path(), True)
    assert_raises(NoSuchFilesytemObjectError, store.is_dir, pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    assert_raises(NoSuchFilesytemObjectError, store.is_dir, pathname.testdir().file('deleted dir').get_path())
        
def _test_fail_on_get_bytes(store):
    assert_raises(NoSuchFilesytemObjectError, store.get_bytes, pathname.testdir().file('non existent file').get_path())
    assert_raises(NoSuchFilesytemObjectError, store.get_bytes, pathname.testdir().file('non existent dir').get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    finish_upload(store)
    store.delete(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME, False)
    store.create_directory(pathname.testdir().file('deleted dir').get_path())
    store.delete(pathname.testdir().file('deleted dir').get_path(), True)
    assert_raises(NoSuchFilesytemObjectError, store.get_bytes, pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    assert_raises(NoSuchFilesytemObjectError, store.get_bytes, pathname.testdir().file('deleted dir').get_path())
    
def _test_fail_on_get_modified(store):
    assert_raises(NoSuchFilesytemObjectError, store.get_modified, pathname.testdir().file('non existent file').get_path())
    assert_raises(NoSuchFilesytemObjectError, store.get_modified, pathname.testdir().file('non existent dir').get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    finish_upload(store)
    store.delete(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME, False)
    store.create_directory(pathname.testdir().file('deleted dir').get_path())
    store.delete(pathname.testdir().file('deleted dir').get_path(), True)
    assert_raises(NoSuchFilesytemObjectError, store.get_modified, pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    assert_raises(NoSuchFilesytemObjectError, store.get_modified, pathname.testdir().file('deleted dir').get_path())

def _test_get_bytes(store):
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    finish_upload(store)
    res = store.get_bytes(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    store.delete(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME, False)
    assert res > 0 and res < 10, "stored file should be between one and ten bytes big, but has a size of %s bytes" % res

def _test_is_dir(store):
    assert store.is_dir(pathname.testdir().get_path()) == True
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    finish_upload(store)
    assert store.is_dir(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME) == False 
    store.delete(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME, False)
        
def _test_account_info(store):
    assert type(store.account_info()) == str
            
def _test_get_modified(store):
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    finish_upload(store)
    file_modified_time = int(store.get_modified(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME))
    now_time = time.time()
    store.delete(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME, False)
    assert _assert_equal_with_variance( file_modified_time, now_time, 15, "modified time stamp of copied file is off by %s seconds" %  abs(file_modified_time-now_time) )
    store.create_directory(pathname.testdir().dir('modified dir').get_path())
    dir_modified_time = store.get_modified(pathname.testdir().dir('modified dir').get_path())
    now_time = time.time()
    store.delete(pathname.testdir().dir('modified dir').get_path(), True)
    assert _assert_equal_with_variance( dir_modified_time, now_time, 15, "modified time stamp of copied file is off by %s seconds" %  abs(dir_modified_time-now_time) )
    #assert not store.is_dir(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_PATH) 

def _assert_equal_with_variance(val1, val2, variance, msg =""):
    return (val1<=val2+variance) and (val1>=val2-variance), msg
        

def _test_get_free_space(store):
    free_space = store.get_free_space()
    used_space = store.get_used_space()
    overall_space = store.get_overall_space()
    assert abs(free_space - (overall_space - used_space)) < 6000, "free space should amount to overall space minus used space (%s) but is %s" % (overall_space - used_space, free_space)

def _test_get_overall_space(store):
    overall_space = store.get_overall_space()
    try: 
        int(overall_space)
    except Exception as e:
        assert False, "exception on getting overall space: "+str(e) 
        
def _test_get_used_space(store):
    used_space = store.get_used_space()
    try: 
        int(used_space)
    except Exception as e:
        assert False, "exception on getting used space"+str(e)  

def _test_get_directory_listing(store): 
    _create_directories(store, pathname.testdir().get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path(), remote_file_name=pathname.testdir().file(nr=2).get_filename())
    finish_upload(store)
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path(), remote_file_name=pathname.testdir().file(nr=3).get_filename())
    listing = store.get_directory_listing(pathname.testdir().get_path())
    cached_listing1 = store.get_directory_listing(pathname.testdir().get_path())
    cached_listing2 = store.get_directory_listing(pathname.testdir().get_path())
    _delete_directories(store, pathname.testdir().get_path())
    _delete_file(store, pathname.LOCAL_TESTFILE_NAME, pathname.testdir().get_path())
    _delete_file(store, pathname.testdir().file(nr=2).get_filename(), pathname.testdir().get_path())
    _delete_file(store, pathname.testdir().file(nr=3).get_filename(), pathname.testdir().get_path())
    root = pathname.testdir().get_path()+"/"
    _assert_all_in(listing, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+pathname.LOCAL_TESTFILE_NAME]) 
    _assert_all_in(cached_listing1, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+pathname.LOCAL_TESTFILE_NAME]) 
    _assert_all_in(cached_listing2, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+pathname.LOCAL_TESTFILE_NAME])

def _test_move_directory(store):
    source = pathname.copy().testdir().dir('source dir')
    dest = pathname.copy().testdir().dir('destination dir')
    store.create_directory(source.get_path())
    store.move(source.get_path(), dest.get_path())
    assert _dir_exists(store, dest.get_path())
    store.delete(dest.get_path(), True)
            
def _test_move_file(store):
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path(), remote_file_name=pathname.testdir().file(nr=2).get_filename())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path(), remote_file_name=pathname.testdir().file(nr=3).get_filename())
    finish_upload(store)
    assert store.exists(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME) 
    store.move(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME, pathname.testdir().file().get_path())
    assert store.exists(pathname.testdir().file().get_path()) 
    store.delete(pathname.testdir().file().get_path(), False)
    _delete_file(store, pathname.testdir().file(nr=2).get_filename(), pathname.testdir().get_path())
    _delete_file(store, pathname.testdir().file(nr=3).get_filename(), pathname.testdir().get_path())
    
def _test_nested_move(store):
    source = pathname.copy().testdir().dir('source dir')
    dest = pathname.copy().testdir().dir('destination dir')
    store.create_directory(source.get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, source.get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, source.get_path(), remote_file_name=source.copy().file(nr=2).get_filename())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, source.get_path(), remote_file_name=source.copy().file(nr=3).get_filename())
    finish_upload(store)
    store.create_directory(source.copy().dir('nested dir').get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, source.copy().dir('nested dir').get_path(), remote_file_name=source.copy().file().get_filename())
    store.move(source.get_path(), dest.get_path())
    assert _dir_exists(store, dest.copy().dir('nested dir').get_path())
    assert store.exists(dest.copy().dir('nested dir').file().get_path())
    contents = store.get_file(dest.copy().dir('nested dir').file().get_path())
    with open(pathname.LOCAL_TESTFILE_PATH) as _file:
        assert _file.read() == contents, "move file differs from the local file."
    contents = store.get_file(dest.copy().file(nr=2).get_path())
    with open(pathname.LOCAL_TESTFILE_PATH) as _file:
        assert _file.read() == contents, "move file differs from the local file."
    assert not store.exists(source.copy().dir('nested dir').file().get_path())
    assert not store.exists(source.copy().dir('nested dir').get_path())
    store.delete(dest.get_path(), True)

def _test_create_delete_directory(store):
    _create_directories(store, pathname.testdir().get_path())
    _delete_directories(store, pathname.testdir().get_path())

def _dir_exists(store, path):
    exists = store.exists(path)
    if not exists:
        return False
    is_dir = store.is_dir(path)
    return is_dir

def _create_directories(store, root_dir="/"):
    if root_dir[-1] != "/":
        root_dir+="/"
    store.create_directory(root_dir+"Test1")
    assert _dir_exists(store, root_dir+"Test1")
    store.create_directory(root_dir+"tesT2")
    assert _dir_exists(store, root_dir+"tesT2")
    store.create_directory(root_dir+"testdub")
    assert _dir_exists(store, root_dir+"testdub")
    try:
        assert store.create_directory(root_dir+"testdub") != 200
    except AlreadyExistsError:
        pass
    store.create_directory(root_dir+"testcasesensitivity")
    assert _dir_exists(store, root_dir+"testcasesensitivity")
    try:
        assert store.create_directory(root_dir+"testcasesensitivity".upper() ) != 200
    except AlreadyExistsError:
        pass
        
def _delete_directories(store, root_dir="/"):
    if root_dir[-1] != "/":
        root_dir+="/"
    store.delete(root_dir+"Test1", True)
    assert not store.exists(root_dir+"Test1")
    store.delete(root_dir+"tesT2", True)
    assert not store.exists(root_dir+"tesT2")
    store.delete(root_dir+"testdub", True)
    assert not store.exists(root_dir+"testdub")
    store.delete(root_dir+"testcasesensitivity", True)
    assert not store.exists(root_dir+"testcasesensitivity")
    
def _test_store_delete_file(store):
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path(), remote_file_name=pathname.testdir().file(nr=2).get_filename())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path(), remote_file_name=pathname.testdir().file(nr=3).get_filename())
    finish_upload(store)
    assert store.exists(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    _delete_file(store, pathname.LOCAL_TESTFILE_NAME, pathname.testdir().get_path())
    _delete_file(store, pathname.testdir().file(nr=2).get_filename(), pathname.testdir().get_path())
    _delete_file(store, pathname.testdir().file(nr=3).get_filename(), pathname.testdir().get_path())
    assert not store.exists(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    store.store_file(pathname.LOCAL_BIGTESTFILE_PATH, pathname.testdir().get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path(), remote_file_name=pathname.testdir().file(nr=2).get_filename())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path(), remote_file_name=pathname.testdir().file(nr=3).get_filename())
    finish_upload(store)
    assert store.exists(pathname.testdir().get_path()+"/"+pathname.LOCAL_BIGTESTFILE_NAME)
    _delete_file(store, pathname.LOCAL_BIGTESTFILE_NAME, pathname.testdir().get_path())
    _delete_file(store, pathname.testdir().file(nr=2).get_filename(), pathname.testdir().get_path())
    _delete_file(store, pathname.testdir().file(nr=3).get_filename(), pathname.testdir().get_path())
    assert not store.exists(pathname.testdir().get_path()+"/"+pathname.LOCAL_BIGTESTFILE_NAME)
    empty_fileobject = tempfile.SpooledTemporaryFile()
    store.store_fileobject(empty_fileobject, pathname.testdir().file('empty_file').get_path())
    finish_upload(store)
    assert store.exists(pathname.testdir().file('empty_file').get_path())
    store.delete(pathname.testdir().file('empty_file').get_path(), is_dir=False)
    assert not store.exists(pathname.testdir().file('empty_file').get_path())
    local_fileobject = open(pathname.LOCAL_TESTFILE_PATH)
    store.store_fileobject(local_fileobject, pathname.testdir().file('empty_file').get_path())
    finish_upload(store)
    assert store.exists(pathname.testdir().file('empty_file').get_path())
    store.delete(pathname.testdir().file('empty_file').get_path(), is_dir=False)
    assert not store.exists(pathname.testdir().file('empty_file').get_path())
    
def _test_exists(store):
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path(), remote_file_name=pathname.testdir().file(nr=2).get_filename())
    finish_upload(store)
    store.store_file(pathname.LOCAL_TESTFILE_PATH, pathname.testdir().get_path(), remote_file_name=pathname.testdir().file(nr=3).get_filename())
    assert store.exists(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    assert store.exists(pathname.testdir().get_path()+"/"+pathname.testdir().file(nr=2).get_filename())
    assert store.exists(pathname.testdir().get_path()+"/"+pathname.testdir().file(nr=3).get_filename())
    _delete_file(store, pathname.LOCAL_TESTFILE_NAME, pathname.testdir().get_path())
    store.delete(pathname.testdir().file(nr=2).get_path(), is_dir=False)
    store.delete(pathname.testdir().file(nr=3).get_path(), is_dir=False)
    assert not store.exists(pathname.testdir().get_path()+"/"+pathname.LOCAL_TESTFILE_NAME)
    assert not store.exists(pathname.testdir().file(nr=2).get_path())
    assert not store.exists(pathname.testdir().file(nr=3).get_path())
    assert store.exists(pathname.testdir().get_path())
    assert store.exists(pathname.testdir().get_path())
    assert not store.exists(pathname.testdir().file('non existent dir').get_path())
    assert not store.exists(pathname.testdir().file('non existent file').get_path())

def _delete_file(store, filename, root_dir="/"):
    if root_dir[-1] != "/":
        root_dir += "/"
    store.delete(root_dir+filename, False)
    
def _test_duplicate(store):
    origin = pathname.copy().testdir().dir('origin').file()
    copy = pathname.copy().testdir().dir('copy').file()
    store.create_directory(origin.get_parent())
    assert _dir_exists(store, origin.get_parent())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, origin.get_parent(), origin.get_filename()) 
    finish_upload(store)
    assert store.exists(origin.get_path())
    store.duplicate(origin.get_parent(), copy.get_parent())
    assert _dir_exists(store, copy.get_parent())
    store.duplicate(origin.get_path(), copy.get_path())
    assert store.exists(copy.get_path())
    store.delete(origin.get_parent(), True)
    store.delete(copy.get_parent(), True)
    
def _test_nested_duplicate(store):
    origin = pathname.copy().testdir().dir('origin')
    copy = pathname.copy().testdir().dir('copy')
    store.create_directory(origin.get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, origin.get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, origin.get_path(), remote_file_name=origin.copy().file(nr=2).get_filename())
    store.create_directory(origin.copy().dir('nested dir').get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, origin.copy().dir('nested dir').get_path(), remote_file_name=origin.copy().file('nested file').get_filename())
    finish_upload(store)
    store.duplicate(origin.get_path(), copy.get_path())
    store.store_file(pathname.LOCAL_TESTFILE_PATH, origin.get_path(), remote_file_name=pathname.testdir().file(nr=3).get_filename())
    assert _dir_exists(store, copy.get_path())
    assert _dir_exists(store, copy.copy().dir('nested dir').get_path())
    assert store.exists(copy.copy().dir('nested dir').file('nested file').get_path())
    contents = store.get_file(copy.copy().dir('nested dir').file('nested file').get_path())
    with open(pathname.LOCAL_TESTFILE_PATH) as file:
        assert file.read() == contents, "duplicated file differs from the local file."
    contents = store.get_file(copy.copy().file(nr=2).get_path())
    with open(pathname.LOCAL_TESTFILE_PATH) as file:
        assert file.read() == contents, "duplicated file differs from the local file."
    store.delete(origin.get_path(), True)
    store.delete(copy.get_path(), True)
    assert not store.exists(copy.get_path())
    assert not store.exists(copy.copy().dir('nested dir').get_path())
    assert not store.exists(copy.copy().file(nr=3).get_path())
    

    
        
