# -*- coding: UTF-8 -*-
'''
Auto registration script for tonline.
Takes username and password from stdin. The parameters must each end in newlines ("\n").
Opens firefox, navigates to https://meinkonto.telekom-dienste.de/konto/registrierung/email/index.xhtml, enters registration details, and tries to solve the captcha.

Created on Aug 29, 2014

@author: joe
'''
from time import sleep
from sikuli.Sikuli import Screen, App, Key, addImagePath, Settings, popup, Env, Pattern, KeyModifier, getImagePath
import time
import sys

import os
import subprocess

import random
#Settings.MoveMouseDelay = 0
firstNameInput = "First_Name_Input.png"
salutationDropDown = 'Salutation_Dropdown.png'
salutationDropDownM = 'Salutation_M.png'
captchaInput = 'Captcha_Input.png'
retryCaptcha = 'Retry_Captcha.png'
copyImageLocation = 'Copy_Image_Location2.png'
finishRegistration = 'Finish_Registration.png'

def start_browser(url):
    try:
        subprocess.Popen(['xdg-open', url])
    except OSError:
        print 'Opening the browser failed'

if __name__ == '__main__':
    #read username and password from stdin
    time.sleep(1) #wait     

    user = sys.stdin.readline()
    pwd = sys.stdin.readline()
    #Add path to where images shall be searched at
    CLOUDFUSION_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    addImagePath(CLOUDFUSION_PATH+"/images/tonline")
    screen = Screen()
    # Start browser
    start_browser("https://meinkonto.telekom-dienste.de/konto/registrierung/email/index.xhtml")
    if screen.exists(salutationDropDown,5):
        screen.click(salutationDropDown)
    if screen.exists(salutationDropDownM,2):
        screen.click(salutationDropDownM)
    screen.type(Key.TAB)
    screen.paste("John")
    screen.type(Key.TAB)
    screen.paste("Smith")
    screen.type(Key.TAB)
    screen.paste(user)
    screen.type(Key.TAB)
    screen.paste(pwd)
    screen.type(Key.TAB)
    screen.paste(pwd)
    screen.type(Key.TAB)
    for i in range(random.randint(1,27)): #date
        screen.type(Key.DOWN)
    screen.type(Key.TAB)
    for i in range(random.randint(1,12)):
        screen.type(Key.DOWN)
    screen.type(Key.TAB)
    for i in range(random.randint(18,50)):
        screen.type(Key.DOWN)
    screen.type(Key.TAB)
    screen.type(Key.DOWN) #question
    screen.type(Key.TAB)
    screen.paste(str(random.randint(1111111111111111111,111111111111111111111))) #secret answer
    screen.type(Key.TAB)
    screen.mouseMove('Image_Next_To_Captcha.png')
    current_loc = Env.getMouseLocation()
    current_loc.x -= 150
    screen.hover(current_loc)
    #screen.wait(copyImageLocation,15)
    #screen.click(copyImageLocation)
    tries = 0
    while True: #try a few times to decode the captcha
        tries += 1
        if tries == 10: #solve manually
            captcha_solution = input("Please enter the solution to the captcha manually")
            break
        screen.rightClick(current_loc)
        try:
            screen.wait(copyImageLocation,15)
            screen.click(copyImageLocation)
            #screen.type("o", KeyModifier.CTRL)
            captcha_url = Env.getClipboard()
            p = subprocess.Popen(['%s %s "%s"' % ('/usr/bin/python', CLOUDFUSION_PATH+'/third_party/captcha_decoder/decode_number_captcha.py', captcha_url)], stdout=subprocess.PIPE, shell=True)
            captcha_solution, err = p.communicate() #wait for process to exit
            if captcha_solution != None and len(captcha_solution) > 2:
                captcha_solution = int(captcha_solution)
                break
        except:
            pass
        screen.click(retryCaptcha)
    
    screen.type(Key.DOWN)
    screen.type(Key.DOWN)
    screen.wait(captchaInput) 
    screen.click(captchaInput)    
    screen.paste(str(captcha_solution))
    screen.type(Key.TAB)
    screen.type(Key.SPACE)
    screen.type(Key.TAB)
    screen.type(Key.DOWN)
    screen.type(Key.DOWN)
    screen.type(Key.DOWN)
    screen.wait(finishRegistration) 
    screen.mouseMove(finishRegistration)
