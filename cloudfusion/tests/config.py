'''
Functions to provide configuration data for each store during a test run. 
Created on Dec 19, 2014

@author: joe
'''
from ConfigParser import SafeConfigParser
import cloudfusion
import os

def get_dropbox_config():
    config = SafeConfigParser()
    config_file = open(os.path.dirname(cloudfusion.__file__)+"/config/Dropbox.ini", "r")
    config.readfp(config_file)
    return dict(config.items('auth'))

def get_sugarsync_config():
    config = SafeConfigParser()
    config_file = open(os.path.dirname(cloudfusion.__file__)+"/config/sugarsync_testing.ini", "r")
    config.readfp(config_file)
    return dict(config.items('auth'))

def get_webdav_gmx_config():
    config = SafeConfigParser()
    config_file = open(os.path.dirname(cloudfusion.__file__)+"/config/Webdav_gmx_testing.ini", "r")
    config.readfp(config_file)
    auth = dict(config.items('auth'))
    return auth

def get_webdav_tonline_config():
    config = SafeConfigParser()
    config_file = open(os.path.dirname(cloudfusion.__file__)+"/config/Webdav_tonline_testing.ini", "r")
    config.readfp(config_file)
    auth = dict(config.items('auth'))
    return auth

def get_webdav_fourshared_config():
    config = SafeConfigParser()
    config_file = open(os.path.dirname(cloudfusion.__file__)+"/config/Webdav_fourshared_testing.ini", "r")
    config.readfp(config_file)
    auth = dict(config.items('auth'))
    return auth

def get_webdav_box_config():
    config = SafeConfigParser()
    config_file = open(os.path.dirname(cloudfusion.__file__)+"/config/Webdav_box_testing.ini", "r")
    config.readfp(config_file)
    auth = dict(config.items('auth'))
    return auth

def get_webdav_yandex_config():
    config = SafeConfigParser()
    config_file = open(os.path.dirname(cloudfusion.__file__)+"/config/Webdav_yandex_testing.ini", "r")
    config.readfp(config_file)
    auth = dict(config.items('auth'))
    return auth

def get_local_config():
    return {'root': '/tmp/cloudfusion_localhd'}

def get_google_config():
    config = SafeConfigParser()
    config_file = open(os.path.dirname(cloudfusion.__file__)+"/config/Google_testing.ini", "r")
    config.readfp(config_file)
    auth = dict(config.items('auth'))
    if 'access_key_id' in auth:
            auth['consumer_key'] = auth['access_key_id']
            auth['consumer_secret'] = auth['secret_access_key']
    return auth

def get_amazon_config():
    config = SafeConfigParser()
    config_file = open(os.path.dirname(cloudfusion.__file__)+"/config/AmazonS3_testing.ini", "r")
    config.readfp(config_file)
    auth = dict(config.items('auth'))
    if 'access_key_id' in auth:
            auth['consumer_key'] = auth['access_key_id']
            auth['consumer_secret'] = auth['secret_access_key']
    return auth

def get_gdrive_config():
    config = SafeConfigParser()
    config_file = open(os.path.dirname(cloudfusion.__file__)+"/config/GDrive.ini", "r")
    config.readfp(config_file)
    return dict(config.items('auth'))