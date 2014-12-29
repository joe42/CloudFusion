# -*- coding: utf-8 -*- 
'''
Created on Dec 17, 2014

@author: joe

Path constants for store tests that do not support using the unusual UTF-8 character 𠀋 
in file and directory names. Instead use the Japanese character き.
'''
LOCAL_TESTFILE_PATH = "cloudfusion/tests/testfile"
LOCAL_BIGTESTFILE_PATH = "cloudfusion/tests/bigtestfile"
REMOTE_TESTDIR_PART1 = "/My SugarSync" #Sugarsync does not allow writing into Cloud Folder through API
REMOTE_TESTDIR_PART2 = "testdirき"
REMOTE_TESTDIR = REMOTE_TESTDIR_PART1+"/"+REMOTE_TESTDIR_PART2
REMOTE_MODIFIED_TESTDIR = REMOTE_TESTDIR+"/"+"testdir"
REMOTE_METADATA_TESTDIR = REMOTE_TESTDIR+"/"+"testdir"
LOCAL_TESTFILE_NAME = "testfile"
LOCAL_BIGTESTFILE_NAME = "bigtestfile"
REMOTE_TESTFILE_NAME = "testfile_remoteき"
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