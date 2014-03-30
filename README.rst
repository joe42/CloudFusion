CloudFusion
===========

CloudFusion lets you access your Dropbox, Sugarsync, Amazon S3, and Google Storage files from Linux like any file on your desktop.
WebDAV support is experimental (Only from command line; nested directories are not supported).

.. contents:: Table of Contents:



Install
-------

To install CloudFusion do the following::

    sudo apt-get install git
    git clone git://github.com/joe42/CloudFusion.git
    sudo apt-get install python-setuptools gcc libssl-dev python-dev cadaver                 #On Ubuntu
    sudo apt-get install python-setuptools gcc openssl-devel python-devel fuse-utils cadaver #On Debian
    cd CloudFusion
    sudo python setup.py install
    
On Ubuntu, you just need to add yourself to the fuse group (replace joe with your own username)::

    sudo usermod -a -G fuse joe
    newgrp fuse

On Debian though, you also need to install fuse, and set permissions to use it as normal user (replace joe with your own username)::

    sudo usermod -a -G fuse joe
    newgrp fuse
    sudo chgrp fuse /dev/fuse 
    sudo chmod g+wr /dev/fuse


Create a Configuration File
...........................

Sugarsync
+++++++++
Copy the Sugarsync configuration file located at **cloudfusion/cloudfusion/config/Sugarsync.ini** to your home directory.
Edit the configuration file by adding your e-mail address as your username and a password.
Your account will expire after a month, except if you change to a payed account.


Dropbox
+++++++
Simply copy the Dropbox configuration file located at **cloudfusion/cloudfusion/config/Dropbox.ini** to your home directory.
If you do not have a Dropbox account already, you can create a new one at https://www.dropbox.com.
Edit the configuration file by adding your username and a password.

Google Storage
++++++++++++++
Copy the Google Storage configuration file located at **cloudfusion/cloudfusion/config/Google.ini** to your home directory.
Add your access_key_id, and secret_access_key to the configuration file. Details on obtaining these are inside the configuration file.

Amazon S3
++++++++++++++
Copy the Amazon S3 configuration file located at **cloudfusion/cloudfusion/config/AmazonS3.ini** to your home directory.
Add your access_key_id, and secret_access_key to the configuration file. Details on obtaining these are inside the configuration file.

WebDAV
++++++++++++++
Copy the WebDAV configuration file located at **cloudfusion/cloudfusion/config/Webdav.ini** to your home directory.
Add the URL of the server, your username, and your  password to the configuration file. 
There are some free WebDAV providers (i.e. https://webdav.mediencenter.t-online.de offers 25 GB of storage).
WebDAV support is experimental.


Get started
-----------

Start CloudFusion::

    cloudfusion --config ~/db.ini mnt

This assumes that you saved the configuration file as **db.ini** to your home directory. 
If you simply copied the configuration file as suggested, replace **db.ini** with the respective file; i.e. **Sugarsync.ini** or **Dropbox.ini**.

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
------------------

Uploading a large amount of small files is quite slow. Instead, try putting the line::

    type = chunk

into the [store] section of your configuration file. With this, CloudFusion will transparently store multiple small files 
inside the same directory into single archives.
Using this parameter with Dropbox solves the problem, that Dropbox does not distinguish file names by case.
I.e. the difference between "file", and "FILE" is ignored, in contrast to Linux file systems, where these would be different files.
This feature is still experimental, but increases upload rate for small files a lot. 
A database is created in the temporary directory, which is necessary to access the files.
This means, that you will only be able to see the files from this one CloudFusion installation.





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


