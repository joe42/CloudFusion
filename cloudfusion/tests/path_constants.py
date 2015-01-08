'''
Created on Dec 19, 2014

@author: joe
'''
LOCAL_TESTFILE_PATH = "cloudfusion/tests/testfile"
LOCAL_BIGTESTFILE_PATH = "cloudfusion/tests/bigtestfile"
REMOTE_TESTDIR_PART1 = "/My SugarSync" #Sugarsync does not allow writing into Cloud Folder through API
REMOTE_TESTDIR_PART2 = "testdir"
REMOTE_TESTDIR = REMOTE_TESTDIR_PART1+"/"+REMOTE_TESTDIR_PART2
REMOTE_MODIFIED_TESTDIR = REMOTE_TESTDIR+"/"+"testdir"
REMOTE_METADATA_TESTDIR = REMOTE_TESTDIR+"/"+"testdir"
LOCAL_TESTFILE_NAME = "testfile"
LOCAL_BIGTESTFILE_NAME = "bigtestfile"
REMOTE_TESTFILE_NAME = "testfile_remote"
REMOTE_TESTFILE_NAME2 = "testfile_remote2"
REMOTE_TESTFILE_NAME3 = "testfile_remote3"
REMOTE_NESTED_FILE_NAME = "nested file"
REMOTE_NESTED_FILE_NAME2 = "nested file2"
REMOTE_NESTED_FILE_NAME3 = "nested file3"
REMOTE_NESTED_DIRECTORY_PART1 = REMOTE_TESTDIR+"/"+"nested directory1"
REMOTE_NESTED_DIRECTORY_PART2 = "nested directory2"
REMOTE_NESTED_DIRECTORY = REMOTE_NESTED_DIRECTORY_PART1 + "/" + REMOTE_NESTED_DIRECTORY_PART1
REMOTE_NESTED_FILE = REMOTE_NESTED_DIRECTORY+"/"+REMOTE_NESTED_FILE_NAME
REMOTE_NESTED_FILE2 = REMOTE_NESTED_DIRECTORY_PART1 + "/" + REMOTE_NESTED_FILE_NAME2
REMOTE_NESTED_FILE3 = REMOTE_NESTED_DIRECTORY+"/"+REMOTE_NESTED_FILE_NAME3
REMOTE_DUPLICATE_TESTDIR_ORIGIN = REMOTE_TESTDIR+"/"+"original"
REMOTE_DUPLICATE_NESTED_TESTDIR_ORIGIN = REMOTE_DUPLICATE_TESTDIR_ORIGIN + "/" + "nested directory"
REMOTE_DUPLICATE_NESTED_TESTFILE_ORIGIN = REMOTE_DUPLICATE_NESTED_TESTDIR_ORIGIN + "/" + REMOTE_NESTED_FILE_NAME
REMOTE_DUPLICATE_TESTDIR_COPY = REMOTE_TESTDIR+"/"+"copy of original" 
REMOTE_DUPLICATE_NESTED_TESTDIR_COPY = REMOTE_DUPLICATE_TESTDIR_COPY + "/" + "nested directory"
REMOTE_DUPLICATE_NESTED_TESTFILE_COPY = REMOTE_DUPLICATE_NESTED_TESTDIR_COPY + "/" + REMOTE_NESTED_FILE_NAME
REMOTE_DUPLICATE_TESTFILE_ORIGIN = REMOTE_TESTDIR+"/"+REMOTE_TESTFILE_NAME
REMOTE_DUPLICATE_TESTFILE_COPY = REMOTE_TESTDIR+"/"+"copy of "+REMOTE_TESTFILE_NAME 
REMOTE_MOVE_TESTDIR_ORIGIN = REMOTE_TESTDIR+"/"+"moving directory"
REMOTE_MOVE_TESTDIR_RENAMED = REMOTE_TESTDIR+"/"+"moving directory renamed"
REMOTE_MOVE_TESTFILE_RENAMED = "moving file renamed"
REMOTE_MOVE_NESTED_TESTDIR_ORIGIN = REMOTE_MOVE_TESTDIR_ORIGIN + "/" + "nested directory"
REMOTE_MOVE_NESTED_TESTFILE_ORIGIN = REMOTE_MOVE_NESTED_TESTDIR_ORIGIN + "/" + REMOTE_NESTED_FILE_NAME
REMOTE_MOVE_NESTED_TESTDIR_RENAMED = REMOTE_MOVE_TESTDIR_RENAMED + "/" + "nested directory"
REMOTE_MOVE_NESTED_TESTFILE_RENAMED = REMOTE_MOVE_NESTED_TESTDIR_RENAMED + "/" + REMOTE_NESTED_FILE_NAME
REMOTE_NON_EXISTANT_FILE = REMOTE_TESTDIR+"/"+"i_am_a_file_which_does_not_exist"
REMOTE_NON_EXISTANT_DIR = REMOTE_TESTDIR+"/"+"i_am_a_folder_which_does_not_exist"
REMOTE_DELETED_FILE = REMOTE_TESTDIR+"/"+"i_am_a_file_which_is_deleted"
REMOTE_DELETED_DIR = REMOTE_TESTDIR+"/"+"i_am_a_folder_which_is_deleted"