CloudFusion
===========

CloudFusion lets you access your Dropbox or Sugarsync files like any file on your desktop.

Install 
--------

To install CloudFusion on Ubuntu do the following::

    sudo apt-get install git
    git clone git://github.com/joe42/CloudFusion.git
    sudo apt-get install python-setuptools
    cd CloudFusion/cloudfusion/
    sudo python setup.py install
    mkdir -p .cloudfusion/logs mnt
    python -m cloudfusion.main mnt

Get started
------------

Add the username and password in the Dropbox or Sugarsync configuration file located at **cloudfusion/cloudfusion/config/**. Also add the keys you get from when creating an application (for Sugarsync you need to get a developer account first).

Now copy the configuration file to your mountpoint::

    cp ~/db.ini mnt/config/config

This assumes that you saved the configuration file with your login data as **db.ini** to your home directory.

Enjoy accessing your files in the directory **mnt/data**.


Notes
------

I am not affiliated with Dropbox nor with Sugarsync.

Since the Dropbox API does not allow write synchronization an application like this file system cannot guarantee consistency when several clients try to write the same file. Therefore it cannot obtain production status. Hence, you need to create your own developer keys to use it.

