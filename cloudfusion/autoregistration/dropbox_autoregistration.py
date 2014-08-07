# -*- coding: UTF-8 -*-
'''
Auto registration script for dropbox.
Takes username and password from stdin. The parameters must each end in newlines ("\n").
Opens firefox, navigates to dropbox.com, enters registration details, and selects the Free Plan Option.

Created on Aug 5, 2014

@author: joe
'''
from sikuli.Sikuli import Screen, App, Key, addImagePath, Settings, popup
import time
import sys
import os

Settings.MoveMouseDelay = 0
SUPPORTED_LANGUAGES = ['english','german']
dbRegistrationBtn = "Registration_Btn.png"
firstNameInput = "First_Name_Input.png"
continueBtnFreeAccount = "Continue_Btn_Free_Account.png"

def configure_language():
    '''Set the correct images according to the language of the website in the current browser window.
    :return: True iff successful'''
    global dbRegistrationBtn, firstNameInput, continueBtnFreeAccount
    for lang in SUPPORTED_LANGUAGES:
        for tries in range(3):
            if screen.exists(lang+'/'+dbRegistrationBtn,2):
                dbRegistrationBtn = lang + '/' + dbRegistrationBtn
                firstNameInput = lang +'/' + firstNameInput
                continueBtnFreeAccount = lang + '/' + continueBtnFreeAccount
                return True
    return False

if __name__ == '__main__':
    #read username and password from stdin
    time.sleep(1) #wait 
    user = sys.stdin.readline()
    pwd = sys.stdin.readline()
    #Add path to where images shall be searched at
    ABS_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    addImagePath(ABS_PATH+"/images/dropbox")
    # Start browser
    App.open("firefox www.dropbox.com")
    screen = Screen()
    successful = configure_language()
    if not successful:
        popup("Automatic registration at Dropbox failed for one of the following reasons.\n"+
              "Otherwise you can report an issue at https://github.com/joe42/CloudFusion/issues.\n"+
              "1. Firefox is not installed\n"+
              "2. You were already logged in to Dropbox\n"+
              "3. Your browser language settings are neither German nor English\n"+
              "4. Your browser is open in a different workspace\n", 
              "Error")
    screen.click(dbRegistrationBtn)
    if screen.exists(firstNameInput,2):
        screen.click(firstNameInput)
    screen.paste("John")
    screen.type(Key.TAB)
    screen.paste("Smith")
    screen.type(Key.TAB)
    screen.paste(user)
    screen.type(Key.TAB)
    screen.paste(pwd)
    screen.type(Key.TAB)
    screen.type(Key.SPACE)
    screen.click(dbRegistrationBtn)
    if not screen.exists(continueBtnFreeAccount,4):
        popup("Automatic registration at Dropbox failed for one of the following reasons.\n"+
              "You can report an issue at https://github.com/joe42/CloudFusion/issues.\n"+
              "1. The Dropbox registration user interface has changed\n"+
              "2. You or another application interfered with the registration process\n", 
              "Error")
    screen.wait()
    screen.click(continueBtnFreeAccount)
    App.open("firefox www.dropbox.com")
