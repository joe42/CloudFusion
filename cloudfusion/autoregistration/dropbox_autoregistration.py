# -*- coding: UTF-8 -*-
'''
Created on Aug 5, 2014

@author: joe
'''
from sikuli.Sikuli import Screen, App, Key, addImagePath, Settings
import time
Settings.MoveMouseDelay = 0
SUPPORTED_LANGUAGES = ['english','german']
dbRegistrationBtn = "Registration_Btn.png"
firstNameInput = "First_Name_Input.png"
continueBtnFreeAccount = "Continue_Btn_Free_Account.png"

def configure_language():
    '''Set the correct images according to the language of the website in the current browser window.'''
    global dbRegistrationBtn, firstNameInput, continueBtnFreeAccount
    for lang in SUPPORTED_LANGUAGES:
        print "lang:"+lang
        if screen.exists(lang+'/'+dbRegistrationBtn):
            print "select "+lang
            dbRegistrationBtn = lang + '/' + dbRegistrationBtn
            firstNameInput = lang +'/' + firstNameInput
            continueBtnFreeAccount = lang + '/' + continueBtnFreeAccount
            return

if __name__ == '__main__':
    
    
    addImagePath("../images/dropbox")
    addImagePath("images/dropbox")
    # Start browser
    App.open("firefox www.dropbox.com")
    screen = Screen()
    time.sleep(5)
    configure_language()
    screen.click(dbRegistrationBtn,0)
    time.sleep(2)
    if screen.exists(firstNameInput):
        screen.click(firstNameInput,0)
    #//screen.click(OPEN_MOUNT_DIRECTORY_BTN,0)
    #//Thread.sleep(5000)
    #//screen.rightClick(LOGO)
    #//screen.click(MENU_EXIT)
    screen.paste("Johannes")
    screen.type(Key.TAB)
    screen.paste("Müller")
    screen.type(Key.TAB)
    screen.paste("email@webdb2.com")
    screen.type(Key.TAB)
    screen.paste("123456")
    screen.type(Key.TAB)
    screen.type(Key.SPACE)
    screen.click(dbRegistrationBtn,0)
    screen.wait(continueBtnFreeAccount)
    screen.click(continueBtnFreeAccount,0)
    App.open("firefox www.dropbox.com")
