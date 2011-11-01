import os
from cloudfusion.store.dropbox.dropbox_store import DropboxStore
from cloudfusion.store.sugarsync.sugarsync_store import SugarsyncStore
from functools import partial
from nose.tools import *
from nose.tools import eq_ as eq
from cloudfusion.store.store import *
from cloudfusion.store.caching_store import CachingStore
from cloudfusion.store.metadata_caching_store import MetadataCachingStore
import os.path, time
import tempfile
from dropbox import auth
from ConfigParser import SafeConfigParser
from cloudfusion.store.sugarsync.error_handling_sugarsync_store import ErrorHandlingSugarsyncStore
import time
import cloudfusion
LOCAL_TESTFILE_PATH = "cloudfusion/tests/testfile"
REMOTE_TESTDIR = "/testdir"
REMOTE_MODIFIED_TESTDIR = REMOTE_TESTDIR+"/"+"testdir"
REMOTE_METADATA_TESTDIR = REMOTE_TESTDIR+"/"+"testdir"
LOCAL_TESTFILE_NAME = "testfile"
REMOTE_TESTFILE_NAME = "testfile_remote"
REMOTE_DUPLICATE_TESTDIR_ORIGIN = REMOTE_TESTDIR+"/"+"original"
REMOTE_DUPLICATE_TESTDIR_COPY = REMOTE_TESTDIR+"/"+"copy of original" 
REMOTE_DUPLICATE_TESTFILE_ORIGIN = REMOTE_TESTDIR+"/"+REMOTE_TESTFILE_NAME
REMOTE_DUPLICATE_TESTFILE_COPY = REMOTE_TESTDIR+"/"+"copy of "+REMOTE_TESTFILE_NAME 
REMOTE_MOVE_TESTDIR_ORIGIN = REMOTE_TESTDIR+"/"+"moving directory"
REMOTE_MOVE_TESTDIR_RENAMED = REMOTE_TESTDIR+"/"+"moving directory renamed"
REMOTE_MOVE_TESTFILE_RENAMED = "moving file renamed"
REMOTE_NON_EXISTANT_FILE = REMOTE_TESTDIR+"/"+"i_am_a_file_which_does_not_exist"
REMOTE_NON_EXISTANT_DIR = REMOTE_TESTDIR+"/"+"i_am_a_folder_which_does_not_exist"
REMOTE_DELETED_FILE = REMOTE_TESTDIR+"/"+"i_am_a_file_which_is_deleted"
REMOTE_DELETED_DIR = REMOTE_TESTDIR+"/"+"i_am_a_folder_which_is_deleted"

def get_dropbox_config():
    return auth.Authenticator.load_config(os.path.dirname(cloudfusion.__file__)+"/config/dropbox_testing.ini")

def get_sugarsync_config():
    config = SafeConfigParser()
    config_file = open(os.path.dirname(cloudfusion.__file__)+"/config/sugarsync_testing.ini", "r")
    config.readfp(config_file)
    return dict(config.items('auth'))

io_apis = []

def setUp():
    dropbox_config = get_dropbox_config()
    sugarsync_config = get_sugarsync_config()
    io_apis.append( DropboxStore(dropbox_config) ) 
    io_apis.append( SugarsyncStore(sugarsync_config) ) 
    io_apis.append( CachingStore( DropboxStore(dropbox_config) ) ) 
    io_apis.append( CachingStore( SugarsyncStore(sugarsync_config) ) )
    io_apis.append( MetadataCachingStore( CachingStore( SugarsyncStore(sugarsync_config) ) ) )
    io_apis.append( CachingStore( MetadataCachingStore( DropboxStore(dropbox_config) ) ) )
    #io_apis.append( ErrorHandlingSugarsyncStore( SugarsyncStore(sugarsync_config) )  ) 
    #time.sleep(10)
    for io_api in io_apis:
        try:
            io_api.create_directory(REMOTE_TESTDIR)
        except AlreadyExistsError:
            pass
        
def tearDown():
    for io_api in io_apis:
        io_api.delete(REMOTE_TESTDIR)
 
def test_io_apis():
    for io_api in io_apis:
#        test = partial(_test_with_root_filepath, io_api)
#        test.description = io_api.get_name()+":"+" "+"fail on determining if file system object is a file or a directory"
#        yield (test, ) 
        test = partial(_test_fail_on_is_dir, io_api)
        test.description = io_api.get_name()+":"+" "+"fail on determining if file system object is a file or a directory"
        yield (test, ) 
        test = partial(_test_fail_on_get_bytes, io_api)
        test.description = io_api.get_name()+":"+" "+"fail on getting number of bytes from file"
        yield (test, ) 
        test = partial(_test_fail_on_get_modified, io_api)
        test.description = io_api.get_name()+":"+" "+"fail on getting modified time"
        yield (test, ) 
        test = partial(_test_create_delete_directory, io_api)
        test.description = io_api.get_name()+":"+" "+"creating and deleting directory"
        yield (test, )
        test = partial(_test_store_delete_file, io_api)
        test.description = io_api.get_name()+":"+" "+"creating and deleting file"
        yield (test, )
        test = partial(_test_get_file, io_api)
        test.description = io_api.get_name()+":"+" "+"getting file"
        yield (test, )
        test = partial(_test_duplicate, io_api)
        test.description = io_api.get_name()+":"+" "+"copying (duplicating) file and directory"
        yield (test, )
        test = partial(_test_move_directory, io_api)
        test.description = io_api.get_name()+":"+" "+"moving directory"
        yield (test, )
        test = partial(_test_move_file, io_api)
        test.description = io_api.get_name()+":"+" "+"moving file"
        yield (test, )
        test = partial(_test_get_bytes, io_api)
        test.description = io_api.get_name()+":"+" "+"getting number of bytes from file"
        yield (test, )
        test = partial(_test_is_dir, io_api)
        test.description = io_api.get_name()+":"+" "+"determine if file system object is a file or a directory"
        yield (test, )
        test = partial(_test_account_info, io_api)
        test.description = io_api.get_name()+":"+" "+"getting account info"
        yield (test, )
        test = partial(_test_get_modified, io_api)
        test.description = io_api.get_name()+":"+" "+"getting modified time"
        yield (test, )
        test = partial(_test_get_free_space, io_api)
        test.description = io_api.get_name()+":"+" "+"getting free space"
        yield (test, )
        test = partial(_test_get_overall_space, io_api)
        test.description = io_api.get_name()+":"+" "+"getting overall space"
        yield (test, )
        test = partial(_test_get_used_space, io_api)
        test.description = io_api.get_name()+":"+" "+"getting used space"
        yield (test, )
        test = partial(_test_get_directory_listing, io_api)
        test.description = io_api.get_name()+":"+" "+"getting directory listing"
        yield (test, )  
        test = partial(_test_exists, io_api)
        test.description = io_api.get_name()+":"+" "+"determine if file and directory exists"
        yield (test, )  

def _assert_all_in(in_list, all_list):
    assert all(item in in_list for item in all_list), "expected all items in %s to be found in %s" % (all_list, in_list)
    
#def _test_with_root_filepath(io_api):
#    listing = io_api.get_directory_listing("/")
#    cached_listing1 = io_api.get_directory_listing("/")
#    cached_listing2 = io_api.get_directory_listing("/")
#    root = REMOTE_TESTDIR+"/"
#    _assert_all_in(listing, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+LOCAL_TESTFILE_NAME]) 
#    _assert_all_in(cached_listing1, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+LOCAL_TESTFILE_NAME]) 
#    _assert_all_in(cached_listing2, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+LOCAL_TESTFILE_NAME]) 
#    io_api.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR, REMOTE_TESTFILE_NAME)
#    resp = io_api.get_file(REMOTE_TESTDIR+"/"+REMOTE_TESTFILE_NAME)
#    _delete_file(io_api, REMOTE_TESTFILE_NAME, REMOTE_TESTDIR)
#    assert len(resp) == 4, "length of file from remote side should be 4 bytes, since in testfile I stored the word 'test'"
    
    
def _test_get_file(io_api):
    io_api.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR, REMOTE_TESTFILE_NAME)
    first_resp = io_api.get_file(REMOTE_TESTDIR+"/"+REMOTE_TESTFILE_NAME)
    second_resp = io_api.get_file(REMOTE_TESTDIR+"/"+REMOTE_TESTFILE_NAME)
    _delete_file(io_api, REMOTE_TESTFILE_NAME, REMOTE_TESTDIR)
    assert first_resp == second_resp, "first response should be same as second response, but %s != %s" % (first_resp, second_resp)
    assert len(first_resp) == 4, "length of file from remote side should be 4 bytes, since in testfile I stored the word 'test' but is %s bytes" % len(first_resp)
    
def _test_fail_on_is_dir(io_api): 
    assert_raises(NoSuchFilesytemObjectError, io_api.is_dir, REMOTE_NON_EXISTANT_FILE)
    assert_raises(NoSuchFilesytemObjectError, io_api.is_dir, REMOTE_NON_EXISTANT_DIR)
    io_api.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    io_api.delete(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME)
    io_api.create_directory(REMOTE_DELETED_DIR)
    io_api.delete(REMOTE_DELETED_DIR)
    assert_raises(NoSuchFilesytemObjectError, io_api.is_dir, REMOTE_DELETED_FILE)
    assert_raises(NoSuchFilesytemObjectError, io_api.is_dir, REMOTE_DELETED_DIR)
        
def _test_fail_on_get_bytes(io_api):
    assert_raises(NoSuchFilesytemObjectError, io_api.get_bytes, REMOTE_NON_EXISTANT_FILE)
    assert_raises(NoSuchFilesytemObjectError, io_api.get_bytes, REMOTE_NON_EXISTANT_DIR)
    io_api.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    io_api.delete(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME)
    io_api.create_directory(REMOTE_DELETED_DIR)
    io_api.delete(REMOTE_DELETED_DIR)
    assert_raises(NoSuchFilesytemObjectError, io_api.get_bytes, REMOTE_DELETED_FILE)
    assert_raises(NoSuchFilesytemObjectError, io_api.get_bytes, REMOTE_DELETED_DIR)
    
def _test_fail_on_get_modified(io_api):
    assert_raises(NoSuchFilesytemObjectError, io_api.get_modified, REMOTE_NON_EXISTANT_FILE)
    assert_raises(NoSuchFilesytemObjectError, io_api.get_modified, REMOTE_NON_EXISTANT_DIR)
    io_api.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    io_api.delete(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME)
    io_api.create_directory(REMOTE_DELETED_DIR)
    io_api.delete(REMOTE_DELETED_DIR)
    assert_raises(NoSuchFilesytemObjectError, io_api.get_modified, REMOTE_DELETED_FILE)
    assert_raises(NoSuchFilesytemObjectError, io_api.get_modified, REMOTE_DELETED_DIR)

def _test_get_bytes(io_api):
    io_api.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR) 
    res = io_api.get_bytes(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME)
    io_api.delete(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME)
    assert res == 4, "stored file should be 4 bytes big, but has a size of %s bytes" % res

def _test_is_dir(io_api):
    assert io_api.is_dir(REMOTE_TESTDIR) == True
    io_api.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    assert io_api.is_dir(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME) == False 
    io_api.delete(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME)
        
def _test_account_info(io_api):
    assert type(io_api.account_info()) == str
            
def _test_get_modified(io_api):
    io_api.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    file_modified_time = int(io_api.get_modified(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME))
    now_time = time.time()
    io_api.delete(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME)
    assert _assert_equal_with_variance( file_modified_time, now_time, 15, "modified time stamp of copied file is off by %s seconds" %  abs(file_modified_time-now_time) )
    io_api.create_directory(REMOTE_MODIFIED_TESTDIR)
    dir_modified_time = io_api.get_modified(REMOTE_MODIFIED_TESTDIR)
    now_time = time.time()
    io_api.delete(REMOTE_MODIFIED_TESTDIR)
    assert _assert_equal_with_variance( dir_modified_time, now_time, 15, "modified time stamp of copied file is off by %s seconds" %  abs(dir_modified_time-now_time) )
    #assert not io_api.is_dir(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_PATH) 

def _assert_equal_with_variance(val1, val2, variance, msg =""):
    return (val1<=val2+variance) and (val1>=val2-variance), msg
        

def _test_get_free_space(io_api):
    free_space = io_api.get_free_space()
    used_space = io_api.get_used_space()
    overall_space = io_api.get_overall_space()
    assert free_space == overall_space - used_space, "free space should amount to overall space minus used space (%s) but is %s" % (overall_space - used_space, free_space)

def _test_get_overall_space(io_api):
    overall_space = io_api.get_overall_space()
    try: 
        int(overall_space)
    except Exception as e:
        assert False, "exception on getting overall space: "+str(e) 
        
def _test_get_used_space(io_api):
    used_space = io_api.get_used_space()
    try: 
        int(used_space)
    except Exception as e:
        assert False, "exception on getting used space"+str(e)  

def _test_get_directory_listing(io_api): 
    _create_directories(io_api, REMOTE_TESTDIR)
    io_api.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    listing = io_api.get_directory_listing(REMOTE_TESTDIR)
    cached_listing1 = io_api.get_directory_listing(REMOTE_TESTDIR)
    cached_listing2 = io_api.get_directory_listing(REMOTE_TESTDIR)
    _delete_directories(io_api, REMOTE_TESTDIR)
    _delete_file(io_api, LOCAL_TESTFILE_NAME, REMOTE_TESTDIR)
    root = REMOTE_TESTDIR+"/"
    _assert_all_in(listing, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+LOCAL_TESTFILE_NAME]) 
    _assert_all_in(cached_listing1, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+LOCAL_TESTFILE_NAME]) 
    _assert_all_in(cached_listing2, [root+'Test1',root+"tesT2",root+"testdub",root+"testcasesensitivity",root+LOCAL_TESTFILE_NAME]) 

def _test_move_directory(io_api):
    io_api.create_directory(REMOTE_MOVE_TESTDIR_ORIGIN)
    io_api.move(REMOTE_MOVE_TESTDIR_ORIGIN, REMOTE_MOVE_TESTDIR_RENAMED)
    assert _dir_exists(io_api, REMOTE_MOVE_TESTDIR_RENAMED)
    io_api.delete(REMOTE_MOVE_TESTDIR_RENAMED)
            
def _test_move_file(io_api):
    io_api.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    assert io_api.exists(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME) 
    io_api.move(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME, REMOTE_TESTDIR+"/"+REMOTE_MOVE_TESTFILE_RENAMED)
    assert io_api.exists(REMOTE_TESTDIR+"/"+REMOTE_MOVE_TESTFILE_RENAMED) 
    io_api.delete(REMOTE_TESTDIR+"/"+REMOTE_MOVE_TESTFILE_RENAMED)

def _test_create_delete_directory(io_api):
    _create_directories(io_api, REMOTE_TESTDIR)
    _delete_directories(io_api, REMOTE_TESTDIR)

def _dir_exists(io_api, path):
    exists = io_api.exists(path)
    if not exists:
        return False
    is_dir = io_api.is_dir(path)
    return is_dir

def _create_directories(io_api, root_dir="/"):
    if root_dir[-1] != "/":
        root_dir+="/"
    io_api.create_directory(root_dir+"Test1")
    assert _dir_exists(io_api, root_dir+"Test1")
    io_api.create_directory(root_dir+"tesT2")
    assert _dir_exists(io_api, root_dir+"tesT2")
    io_api.create_directory(root_dir+"testdub")
    assert _dir_exists(io_api, root_dir+"testdub")
    try:
        assert io_api.create_directory(root_dir+"testdub") != 200
    except AlreadyExistsError:
        pass
    io_api.create_directory(root_dir+"testcasesensitivity")
    assert _dir_exists(io_api, root_dir+"testcasesensitivity")
    try:
        assert io_api.create_directory(root_dir+"testcasesensitivity".upper() ) != 200
    except AlreadyExistsError:
        pass
        
def _delete_directories(io_api, root_dir="/"):
    if root_dir[-1] != "/":
        root_dir+="/"
    io_api.delete(root_dir+"Test1")
    assert not io_api.exists(root_dir+"Test1")
    io_api.delete(root_dir+"tesT2")
    assert not io_api.exists(root_dir+"tesT2")
    io_api.delete(root_dir+"testdub")
    assert not io_api.exists(root_dir+"testdub")
    io_api.delete(root_dir+"testcasesensitivity")
    assert not io_api.exists(root_dir+"testcasesensitivity")
    
def _test_store_delete_file(io_api):
    io_api.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    assert io_api.exists(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME)
    _delete_file(io_api, LOCAL_TESTFILE_NAME, REMOTE_TESTDIR)
    assert not io_api.exists(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME)
    empty_fileobject = tempfile.SpooledTemporaryFile()
    io_api.store_fileobject(empty_fileobject, REMOTE_TESTDIR+"/"+"empty_file")
    assert io_api.exists(REMOTE_TESTDIR+"/"+"empty_file")
    _delete_file(io_api, "empty_file", REMOTE_TESTDIR)
    assert not io_api.exists(REMOTE_TESTDIR+"/"+"empty_file")
    local_fileobject = open(LOCAL_TESTFILE_PATH)
    io_api.store_fileobject(local_fileobject, REMOTE_TESTDIR+"/"+"empty_file")
    assert io_api.exists(REMOTE_TESTDIR+"/"+"empty_file")
    _delete_file(io_api, "empty_file", REMOTE_TESTDIR)
    assert not io_api.exists(REMOTE_TESTDIR+"/"+"empty_file")
    
def _test_exists(io_api):
    io_api.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR)
    assert io_api.exists(REMOTE_TESTDIR+"/"+LOCAL_TESTFILE_NAME)
    _delete_file(io_api, LOCAL_TESTFILE_NAME, REMOTE_TESTDIR)
    assert io_api.exists(REMOTE_TESTDIR)
    assert io_api.exists(REMOTE_TESTDIR)
    assert not io_api.exists(REMOTE_NON_EXISTANT_DIR)
    assert not io_api.exists(REMOTE_NON_EXISTANT_FILE)

def _delete_file(io_api, file, root_dir="/"):
    if root_dir[-1] != "/":
        root_dir += "/"
    io_api.delete(root_dir+file)
    
def _test_duplicate(io_api):
    io_api.create_directory(REMOTE_DUPLICATE_TESTDIR_ORIGIN)
    assert _dir_exists(io_api, REMOTE_DUPLICATE_TESTDIR_ORIGIN)
    io_api.store_file(LOCAL_TESTFILE_PATH, REMOTE_TESTDIR, REMOTE_TESTFILE_NAME) 
    assert io_api.exists(REMOTE_TESTDIR+"/"+REMOTE_TESTFILE_NAME)
    io_api.duplicate(REMOTE_DUPLICATE_TESTDIR_ORIGIN, REMOTE_DUPLICATE_TESTDIR_COPY)
    assert _dir_exists(io_api, REMOTE_DUPLICATE_TESTDIR_COPY)
    io_api.duplicate(REMOTE_DUPLICATE_TESTFILE_ORIGIN, REMOTE_DUPLICATE_TESTFILE_COPY)
    assert io_api.exists(REMOTE_DUPLICATE_TESTFILE_COPY)
    io_api.delete(REMOTE_DUPLICATE_TESTDIR_ORIGIN)
    io_api.delete(REMOTE_DUPLICATE_TESTDIR_COPY)
    io_api.delete(REMOTE_DUPLICATE_TESTFILE_ORIGIN)
    io_api.delete(REMOTE_DUPLICATE_TESTFILE_COPY)
    
 #assert_all_in(resp.data.keys(), [u'is_deleted', u'thumb_exists',u'bytes', u'modified', u'path', u'is_dir',u'size', u'root', u'hash', u'contents', u'icon'])
       

        