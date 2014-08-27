#-*- coding:utf-8 -*-

import cv2.cv as cv


def smoothImage(im, nbiter=0, filter=cv.CV_GAUSSIAN):
    for i in range(nbiter):
        cv.Smooth(im, im, filter)

def openCloseImage(im, nbiter=0):
    for i in range(nbiter):
        cv.MorphologyEx(im, im, None, None, cv.CV_MOP_OPEN) #Open and close to make appear contours
        cv.MorphologyEx(im, im, None, None, cv.CV_MOP_CLOSE)

def dilateImage(im, nbiter=0):
    for i in range(nbiter):
        cv.Dilate(im, im)

def erodeImage(im, nbiter=0):
    for i in range(nbiter):
        cv.Erode(im, im)

def thresholdImage(im, value, filter=cv.CV_THRESH_BINARY_INV):
    cv.Threshold(im, im, value, 255, filter)

def resizeImage(im, (width, height)):
    #It appears to me that resize an image can be significant for the ocr engine to detect characters
    res = cv.CreateImage((width,height), im.depth, im.channels)
    cv.Resize(im, res)
    return res

def getContours(im, approx_value=1): #Return contours approximated
    storage = cv.CreateMemStorage(0)
    contours = cv.FindContours(cv.CloneImage(im), storage, cv.CV_RETR_CCOMP, cv.CV_CHAIN_APPROX_SIMPLE)
    contourLow=cv.ApproxPoly(contours, storage, cv.CV_POLY_APPROX_DP,approx_value,approx_value)
    return contourLow

def getIndividualContoursRectangles(contours): #Return the bounding rect for every contours
    contourscopy = contours
    rectangleList = []
    while contourscopy:
        x,y,w,h = cv.BoundingRect(contourscopy)
        rectangleList.append((x,y,w,h))
        contourscopy = contourscopy.h_next()
    return rectangleList


if __name__=="__main__":
    orig = cv.LoadImage("Out.jpg")

    #Convert in black and white
    res = cv.CreateImage(cv.GetSize(orig), 8, 1)
    cv.CvtColor(orig, res, cv.CV_BGR2GRAY)

    #Operations on the image
    openCloseImage(res)
    dilateImage(res, 2)
    erodeImage(res, 2)
    smoothImage(res, 5)
    
    thresholdImage(res, 150, cv.CV_THRESH_BINARY_INV)
    
    #Get contours approximated
    contourLow = getContours(res, 3)
    
    #Draw them on an empty image
    final = cv.CreateImage(cv.GetSize(res), 8, 1)
    cv.Zero(final)
    cv.DrawContours(final, contourLow, cv.Scalar(255), cv.Scalar(255), 2, cv.CV_FILLED)    
    
    cv.ShowImage("orig", orig)
    cv.ShowImage("image", res)
    cv.SaveImage("modified.png", res)
    cv.ShowImage("contour", final)
    cv.SaveImage("contour.png", final)
    
    cv.WaitKey(0)
