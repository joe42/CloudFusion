CloudFusion
===========

CloudFusion lets you access your Dropbox or Sugarsync files from Linux like any file on your desktop.


Install
-------

To install CloudFusion on Ubuntu do the following::

    sudo apt-get install git
    git clone git://github.com/joe42/CloudFusion.git
    sudo apt-get install python-setuptools
    cd CloudFusion
    sudo python setup.py install

Get started
-----------

Start CloudFusion::

    cloudfusion mnt

If the directory **mnt** does not yet exist, it will b created. (If you want to find an error, write the word **log**
at the end of the command, which will create the directory **.cloudfusion/logs** with log files.) 
After configuring CloudFusion, you can access the files from your cloud provider in **mnt/data**.

Create a Configuration File
...........................

Sugarsync
+++++++++
Copy the Sugarsync configuration file located at **cloudfusion/cloudfusion/config/Sugarsync.ini** to your home directory.
Edit the configuration file by adding your e-mail address as your username and a password. 


Dropbox
+++++++
Simply copy the Dropbox configuration file located at **cloudfusion/cloudfusion/config/Dropbox.ini** to your home directory.
If you do not have a Dropbox account already, you can create a new one at https://www.dropbox.com.
No further steps are required. You can however add your username (e-mail) and password to the configuration file. Then, 
you do not need to acknowledge that cloudfusion can access your data via your browser, when configuring CloudFusion.  

Configuring CloudFusion
.......................

Now copy the configuration file you edited to your mountpoint::

    cp ~/db.ini mnt/config/config

This assumes that you saved the configuration file as **db.ini** to your home directory. 
If you simply copied the configuration file as suggested, replace **db.ini** with **Sugarsync.ini** or **Dropbox.ini** respectively.
If you use Dropbox, your webbrowser will prompt you to login into Dropbox and grant access rights to CloudFusion. Except, 
if you entered your username and password in the configuration file, then this will be automatized.  
The current time limit for this is one minute. If you did not make it in time, copying the configuration file fails.
But you can simply retry by copying the configuration again

Enjoy accessing your files in the directory **mnt/data**.



Shut Down
---------

To shut down CloudFusion, you can delete the file **mnt/config/config**. 


Restrictions
------------

There is no automatic sync from the online store to local disk. But 

 * you can manually refresh the directory to see changes
 * with Dropbox, files are moved to /overwritten directory (online) instead of being overwritten accidentially
There is no differential update, which means files are uploaded or downloaded as a whole.

Dropbox has a maximum file upload size of 150MB and operations can at most work on 10.000 files and folders.
It does not allow thumbs.db or .ds_store files.

Sugarsync has a maximum file upload size of 100MB. It does not allow Outlook .pst, Quicken, and Quickbooks.


Notes
-----

Cloudfusion is continually tested with cloudbees' Jenkins. So you can easily see if the current version works:

.. image:: https://joe42.ci.cloudbees.com/buildStatus/icon?job=Cloudfusion

Or which tests failed: https://joe42.ci.cloudbees.com/job/Cloudfusion/lastCompletedBuild/testReport


I am not affiliated with Dropbox nor with Sugarsync.


