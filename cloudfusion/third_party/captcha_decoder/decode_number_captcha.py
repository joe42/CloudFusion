'''
Decodes an integer from an image. The image path is passed as the first argument, and may be a local path or an http address.
Outputs the recognized number on stdout. If no number is recognized an empty string is returned.
Originally from: https://github.com/RobinDavid/Captacha-basic-recognition
'''
from sh import tesseract
from sh import cat
import os
import cv2.cv as cv
import cv2
import Image
from generic_ocr_operations import *
import string
import re
import sys
import urllib
from multiprocessing import Lock
lock = Lock()


class ProbabilisticCracker():

    def __init__(self, image_path):
        self.image = cv.LoadImage(image_path, cv.CV_LOAD_IMAGE_GRAYSCALE) #Open the file
        self.values = [] #Will hold results
        self.ocrvalue = "" #Final value
      
    def getValue(self):
        return self.ocrvalue #Return the final value
      
    def crack(self,dilateiter=4, erodeiter=4, threshold=200, size=(155,55), whitelist_chars=string.ascii_lowercase): #Take all parameters
        ''':param whitelist_char: the characters to recognize'''
        resized = resizeImage(self.image, (self.image.width*6, self.image.height*6))
    
        dilateImage(resized, dilateiter)
        erodeImage(resized, erodeiter)
        thresholdImage(resized,threshold, cv.CV_THRESH_BINARY)
        
        resized = resizeImage(resized, size)
        
        #Call the tesseract engine
        from tempfile import NamedTemporaryFile
        temp_img_file = NamedTemporaryFile(suffix='.jpg') 
        temp_solution_file = NamedTemporaryFile() 
        cv.SaveImage(temp_img_file.name,resized)
        tesseract(temp_img_file.name, temp_solution_file.name, '-c', 'tessedit_char_whitelist='+whitelist_chars)
        ret = str(cat(temp_solution_file.name+'.txt'))
        import os
        os.unlink(temp_solution_file.name+'.txt')
        return ret

    def prefetch_generator2(self, gen, parameters):
        import uuid
        PRODUCER_FINISHED_TOKEN = uuid.uuid4() 
        from multiprocessing import Process, Queue
        def async_producer(q, rng):
            for i in gen(rng):
                q.put( i )
            q.put(PRODUCER_FINISHED_TOKEN)
        q = Queue(100)
        for p in parameters:
            Process(target=async_producer, args=(q,p)).start()
        end_count = len(parameters)
        try:
            while True:
                item = q.get()
                if item == PRODUCER_FINISHED_TOKEN:
                    end_count -= 1
                    if end_count == 0:
                        return
                else:
                    yield item               
        except Exception:
            import traceback
            print traceback.print_exc()

    def result_generator(self, parameters):
        '''Generates multiple possible texts that are recognized by tesseract 
           by slightly varying the image by diation, erosion, color treshold, size, and the set of characters to be recognized.
           If the text consists of digits, only digits between 100 and 100000 are recognized.
           If the text consists of alphabetic characters, only text with more than 10 characters are recognized.
           :param parameters: list of lists with the following values:
            1. list of strings of characters to be recognized - the strings must be either string.digits or string.ascii_lowercase
            2. list of integers for image erosion 
            3. list of integers for image dilation
            4. list of integers for color treshold from 0 to 255
        '''
        w = self.image.width
        h = self.image.height
        whitelist_chars_range = parameters[0]
        erosion_range = parameters[1]
        dilation_range = parameters[2]
        threshold_range = parameters[3]
        for erode in erosion_range:
            for dilate in dilation_range:
                for thresh in threshold_range:
                    for size in [(w*2,h*2),(w*3,h)]:
                        for whitelist_chars in whitelist_chars_range:
                            if whitelist_chars == string.digits:
                                val = self.crack(dilate, erode, thresh, size, whitelist_chars)  
                                val = re.sub(r"\s+", "", val) #remove whitespaces
				#lock.acquire()
                                #print "erode %s - dilate %s - thresh %s - size %s - val %s" % (erode, dilate, thresh, size[1]/57, repr(val))
				#lock.release()
                                if val.isdigit():
                                    val = int(val)
                                    if val > 100 and val < 100000:
                                        yield str(val)
                            else: 
                                val = self.crack(dilate, erode, thresh, size, whitelist_chars) #Call crack successively all parameters
                                val = re.sub(r"\s+", "", val) #remove whitespaces
                                #print repr(val)
                                if not len(val) > 10:
                                    continue
                                yield val

    def run(self): #Main method
        parameterset = []
        parameterset.append( [ [string.digits], [1,3,4,5], [1,3,4,5], [75] ] )
        parameterset.append( [ [string.digits], [1,3,4,5], [1,3,4,5], [100] ] )
        parameterset.append( [ [string.digits], [1,3,4,5], [1,3,4,5], [125] ] )
        parameterset.append( [ [string.digits], [1,3,4,5], [1,3,4,5], [175] ] )
        parameterset.append( [ [string.digits], [1,3,4,5], [1,3,4,5], [200] ] )
        for res in self.prefetch_generator2(self.result_generator, parameterset):
            self.values.append(res)
        self.postAnalysis()

    def most_common(self, lst):
        #print repr(lst)
        return max(set(lst), key=lst.count)
    
    def postAnalysis(self): #Analyse at the end
        self.ocrvalue = self.most_common(self.values)

if __name__=="__main__":
    #crop_im = im[0:im.height, 70:im.width-70]
    #cv.ShowImage('dst_rt',crop_im)
    #cv.WaitKey(0)
    captcha_path = sys.argv[1]
    if captcha_path.startswith('http'):
	urllib.urlretrieve(captcha_path, "/tmp/captcha.jpg")
	captcha_path = "/tmp/captcha.jpg"
    cracker = ProbabilisticCracker(captcha_path) #Instantiate the ProbabilistricCracker
    try:
        cracker.run() #Run it
    except:
        print ''
        exit()
    print cracker.getValue()

