CloudFusion
===========

CloudFusion lets you access your Dropbox or Sugarsync files from Linux like any file on your desktop.

Install 
--------

To install CloudFusion on Ubuntu do the following::

    sudo apt-get install git
    git clone git://github.com/joe42/CloudFusion.git
    sudo apt-get install python-setuptools
    cd CloudFusion/cloudfusion/
    sudo python setup.py install

Get started
------------

Start CloudFusion::

    mkdir -p .cloudfusion/logs mnt
    python -m cloudfusion.main mnt

This will create a directory for your logs and a mountpoint **mnt**. 
After configuring CloudFusion, you can access the files from your cloud provider in **mnt/data**.

Create a Configuration File
.................................

Sugarsync
++++++++++
Copy the Sugarsync configuration file located at **cloudfusion/cloudfusion/config/Sugarsync.ini** to your home directory.
Edit the configuration file by adding your e-mail address as your username and a password. 


Dropbox
++++++++++
Simply copy the Dropbox configuration file located at **cloudfusion/cloudfusion/config/Dropbox.ini** to your home directory.
If you do not have a Dropbox account already, you can create a new one at https://www.dropbox.com.
No further steps are required. 

Configuring CloudFusion
...................................

Now copy the configuration file you edited to your mountpoint::

    cp ~/db.ini mnt/config/config

This assumes that you saved the configuration file with your login data as **db.ini** to your home directory.
If you use Dropbox, your webbrowser will prompt you to login into Dropbox and grant access rights to CloudFusion. 
The current time limit for this is one minute. If you did not make it in time, copying the configuration file fails.
But you can simply retry by copying the configuration again

Enjoy accessing your files in the directory **mnt/data**.


Notes
------

I am not affiliated with Dropbox nor with Sugarsync.

