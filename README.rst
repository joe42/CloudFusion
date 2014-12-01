CloudFusion
===========

CloudFusion lets you access a multitude of cloud storages from Linux like any file on your desktop.
Work with files from  Dropbox, Sugarsync, Amazon S3, Google Storage, Google Drive, and WebDAV storages like any other file on your desktop.


.. contents:: Table of Contents:



Install
-------

To install CloudFusion do the following::

    sudo apt-get install git
    git clone git://github.com/joe42/CloudFusion.git

The packages you need to install have slightly different names on Ubuntu and Debian.
Also, on Ubuntu, you just need to add yourself to the fuse group, while on Debian 
you might need to install fuseand set permissions to use it as a normal user.

On Ubuntu::

    # For using automatic registration with Sikuli: opencv-dev libhighgui2.4 libcvaux2.4 sikuli-ide
    # For using Google Storage: gcc libssl-dev libffi-dev python-dev
    # For automatic handling of captchas in the registration process: python-pycurl python-libxml2 python-imaging tesseract-ocr
    sudo apt-get install python-setuptools gcc libssl-dev libffi-dev python-dev fuse-utils opencv-dev libhighgui2.4 libcvaux2.4 sikuli-ide python-pycurl python-libxml2 python-imaging  tesseract-ocr

    # Finally install CloudFusion
    cd CloudFusion
    sudo python setup.py install
    

On Debian::

    # These are the same packages as for Ubuntu with slightly different names
    sudo apt-get install python-setuptools gcc libssl-dev libffi-dev python-dev fuse-utils libopencv-dev libhighgui-dev libcvaux-dev sikuli-ide python-pycurl python-libxml2 python-imaging  tesseract-ocr

    cd CloudFusion
    sudo python setup.py install

    # Add yourself to the group fuse
    sudo usermod -a -G fuse "$USER"
    # add the group for the current shell session (or restart your computer so it works in every shell)
    newgrp fuse
    # allow users in the fuse group access to fuse filesystems
    sudo chgrp fuse /dev/fuse 
    sudo chmod g+wr /dev/fuse


Create a Configuration File
-----------------------------

Sugarsync
+++++++++
Copy the Sugarsync configuration file located at **cloudfusion/cloudfusion/config/Sugarsync.ini** to your home directory.
Edit the configuration file by adding your e-mail address as your username and a password.
Your account will expire after a month, except if you change to a paid account.


Dropbox
+++++++
Simply copy the Dropbox configuration file located at **cloudfusion/cloudfusion/config/Dropbox.ini** to your home directory.
If you do not have a Dropbox account already, you can create a new one at https://www.dropbox.com.
Edit the configuration file by adding your username and a password.

Google Drive
++++++++++++++
Copy the Google Drive configuration file located at **cloudfusion/cloudfusion/config/GDrive.ini** to your home directory.
Add your client_id, and client_secret to the configuration file. Details on obtaining these are inside the configuration file.

Google Storage
++++++++++++++
Copy the Google Storage configuration file located at **cloudfusion/cloudfusion/config/Google.ini** to your home directory.
Add your access_key_id, and secret_access_key to the configuration file. Details on obtaining these are inside the configuration file.
Add a unique bucket_name to the configuration file. To create a unique name, you can simply call ::

    cloudfusion uuid

Amazon S3
+++++++++
Copy the Amazon S3 configuration file located at **cloudfusion/cloudfusion/config/AmazonS3.ini** to your home directory.
Add your access_key_id, and secret_access_key to the configuration file. Details on obtaining these are inside the configuration file.
Add a unique bucket_name to the configuration file. To create a unique name, you can simply call ::

    cloudfusion uuid

WebDAV
++++++
Copy the WebDAV configuration file located at **cloudfusion/cloudfusion/config/Webdav.ini** to your home directory.
Add the URL of the server, your username, and your  password to the configuration file. 
Here some information about WebDAV providers:

==============  ============================================ ============  ======================================================
Name            WebDAV URL                                   Free Storage  Further Details                  
==============  ============================================ ============  ======================================================
T-Online_       https://webdav.mediencenter.t-online.de      25 GB         German Provider                           
4shared_        https://webdav.4shared.com                   15 GB         3 GB daily traffic, 30 GB monthly, cannot delete directories                            
Box.com_        https://dav.box.com/dav                      10 GB  
yandex.com_     https://webdav.yandex.com                    10 GB       
GMX_            https://webdav.mc.gmx.net                    2  GB         German Provider     
OneDrive_       see: blog.lazut.in_                          7  GB         Does not seem to work anymore
==============  ============================================ ============  ======================================================

.. _T-Online : https://mediencenter.t-online.de 
.. _GMX : http://www.gmx.net/produkte/mediacenter/
.. _4shared : http://4shared.com/
.. _Box.com : https://www.box.com/
.. _OneDrive : https://www.box.com/
.. _blog.lazut.in : http://blog.lazut.in/2012/12/skydrive-webdav-access.html
.. _yandex.com: www.yandex.com


Get started
-----------

Start CloudFusion::

    cloudfusion --config ~/db.ini mnt

This assumes that you saved the configuration file as **db.ini** to your home directory. 
If you simply copied the configuration file as suggested, replace **db.ini** with the respective file; i.e. **Sugarsync.ini** or **Dropbox.ini**.
If the login process is not yet fully automatized, as with Google Drive, a browser will open, 
and you have to allow CloudFusion access to your account manually. 

Enjoy accessing your files in the directory **mnt/data**.


Shut Down
---------

To shut down CloudFusion, you can delete the file **mnt/config/config**, or use the following command::

    cloudfusion ~/mnt stop


Restrictions
------------

Cloudfusion does not set the correct permissions or time stamps. See the following projects if this is a requirement:

:s3fs_: Amazon S3
:s3fuse_: Google Storage
:davfs2_: WebDAV (included in Linux standard distributions)

.. _s3fs : https://github.com/s3fs-fuse/s3fs-fuse 
.. _s3fuse : https://code.google.com/p/s3fuse
.. _davfs2 : http://savannah.nongnu.org/projects/davfs2


There is no automatic sync from the online store to local disk. But 

 * you can manually refresh the directory to see changes
 * with Dropbox, files are moved to /overwritten directory (online) instead of being overwritten accidentially
There is no differential update, which means files are uploaded or downloaded as a whole.

Dropbox has a maximum file upload size of 150MB and operations can at most work on 10.000 files and folders.
It does not allow thumbs.db or .ds_store files.

Sugarsync has a maximum file upload size of 100MB. It does not allow Outlook .pst, Quicken, and Quickbooks.


Advanced Features
-----------------

Auto Registration
+++++++++++++++++++

Automatic account registration and storage allocation is a feature facilitating a semi-automatic or fully automatic registration process.
Try putting the line::

    autoregister = true

into the [store] section of your configuration file. Also, fill in the required username and password variables as described in the 
configuration file. The username/password combination will be used to register a new account.
For Sugarsync the proccess is fully automatic, and invisible to the user, but the account is only valid for 30 days; Then the user
needs to change to a payed plan.
For Tonline, the process is semi automatic; CloudFusion will fill the form automatically, and try to solve the captcha automatically,
but the user can change the automatic input if it is required before confirming the registration.
For Dropbox, the registration process will automatically provide mouse and keyboard input for the forms to register for a free account. 

Archiving Store
+++++++++++++++

Uploading a large amount of small files is quite slow. Instead, try putting the line::

    type = chunk

into the [store] section of your configuration file. With this, CloudFusion will transparently store multiple small files 
inside the same directory into single archives.
Using this parameter with Dropbox also solves the problem, that Dropbox does not distinguish file names by case.
I.e. Dropbox ignores the difference between "file", and "FILE", in contrast to Linux file systems, where these would be different files.
This feature is still experimental, but increases upload rate for small files a lot. 
A database is created in the temporary directory, which is necessary to access the files.
This means, that you will only be able to see the files from this one CloudFusion installation.

Statistics
++++++++++

Statistics can be read from the files in *mnt/stats*. The file *stats* contains general performance statistics, 
*errors* contains a summary of recently occured exceptions, and *notuploaded* contains files that are not yet completely uploaded to the remote storage provider.

Caching
+++++++

Advanced options can be set in the configuration file in order to set limits to how much or how long data is cached::

   #Approximate cache size limit in MB;
   cache_size = 5000
   
   # Hard cache size limit in MB. If this is exceeded, write operations are slowed down significantly,
   # until enough space is free again. 
   hard_cache_size_limit = 10000
   
   #How many seconds it may take until a file you just wrote is beginning to be uploaded, always counting from the time 
   #you last modified the file.
   #During this time you can delete the file again, without ever uploading the file.
   #If your files change a lot, and you are in no hurry to upload them, set this to about 10 minutes or more (600).
   cache = 60
   
   #How many seconds it may take for you to see changes made to your Dropbox account by another application.
   #During this time you do not need to communicate with the store to see a directory listing, for instance.
   #So listing directories is very fast. 
   #Set this to 15, if you quickly want to see files uploaded by your mobile computer or handheld, when you refresh the directory.
   #If you upload file through CloudFusion only, this can be set to ten minutes (600).
   metadata_cache = 120
   
   #Identifier for persistent database. Use one id per cloud account to keep the cache after application shutdown.
   #Default value is a random number.
   cache_id = dropboxacc1


Documentation
--------------

More documentation can be found here: 

http://joe42.github.com/CloudFusion/


Notes
-----

Thanks to Cloudbees and Travis, who help with CloudFusions quality assurance.

Cloudfusion is continually tested with travis-ci. So you can easily see if the current version works:

.. image:: https://travis-ci.org/joe42/CloudFusion.png?branch=development
   :target: https://travis-ci.org/joe42/CloudFusion

Cloudfusion will in the near future be continually tested with cloudbees' Jenkins.

.. image:: https://joe42.ci.cloudbees.com/buildStatus/icon?job=Cloudfusion

You will be able to see which tests pass or fail: https://joe42.ci.cloudbees.com/job/Cloudfusion/lastCompletedBuild/testReport
As well as a test coverage report.


I am not affiliated with Dropbox nor with Sugarsync.


